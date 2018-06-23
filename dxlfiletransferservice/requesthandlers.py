from __future__ import absolute_import
import hashlib
import logging
import os
import shutil
import threading
import uuid

from dxlclient.callbacks import RequestCallback
from dxlclient.message import Request, Response, \
    ErrorResponse  # pylint: disable=unused-import
from dxlbootstrap.util import MessageUtils
from .constants import FileStoreParam

# Configure local logger
logger = logging.getLogger(__name__)


def _get_value_as_int(dict_obj, key):
    return_value = None
    if key in dict_obj:
        try:
            return_value = int(dict_obj.get(key))
        except ValueError:
            raise ValueError(
                "'{}' of '{}' could not be converted to an int".format(
                    key, return_value))
    return return_value


class FileStoreRequestCallback(RequestCallback):
    """
    'file_transfer_service_file_store' request handler registered with topic
    '/opendxl-file-transfer/service/file/store'
    """

    _STORAGE_WORK_SUBDIR = ".workdir"
    _FILE_HASHER = "file_hasher"

    def __init__(self, app, storage_dir):
        """
        Constructor parameters:

        :param app: The application this handler is associated with
        """
        super(FileStoreRequestCallback, self).__init__()
        self._app = app
        self._files = {}
        self._files_lock = threading.RLock()

        self._storage_work_dir = os.path.join(storage_dir,
                                              self._STORAGE_WORK_SUBDIR)
        if not os.path.exists(self._storage_work_dir):
            os.makedirs(self._storage_work_dir)

        logger.info("Using file store at '%s'",
                    os.path.abspath(storage_dir))
        self._storage_dir = storage_dir

        for incomplete_file_id in os.listdir(self._storage_work_dir):
            logger.info("Purging content for incomplete file id: '{}'".format(
                incomplete_file_id
            ))
            file_path = os.path.join(self._storage_dir, incomplete_file_id)
            if os.path.exists(file_path):
                shutil.rmtree(file_path)
            os.remove(os.path.join(self._storage_work_dir, incomplete_file_id))

    def on_request(self, request):
        """
        Invoked when a request message is received.

        :param Request request: The request message
        """
        # Handle request
        logger.debug("Request received on topic: '%s'",
                     request.destination_topic)

        try:
            params = request.other_fields

            file_id = params.get(FileStoreParam.FILE_ID)

            file_name = params.get(FileStoreParam.FILE_NAME)
            if not file_name:
                raise ValueError("File name was not specified")

            file_size = _get_value_as_int(params, FileStoreParam.FILE_SIZE)
            file_hash = params.get(FileStoreParam.FILE_HASH)

            file_complete = params.get(FileStoreParam.FILE_COMPLETE)
            if file_complete:
                if file_complete == FileStoreParam.FILE_CANCEL:
                    if not file_id:
                        raise ValueError("File id to cancel must be specified")
                elif file_complete == FileStoreParam.FILE_STORE:
                    if file_size is None:
                        raise ValueError(
                            "File size must be specified for store request")
                    if file_size is not None and not file_hash:
                        raise ValueError(
                            "File hash must be specified for store request")
                else:
                    raise ValueError(
                        "Unexpected 'file_complete' value: '{}'".
                            format(file_complete))

            file_segment_number = _get_value_as_int(
                params, FileStoreParam.FILE_SEGMENT_NUMBER)

            if file_id:
                file_dir = os.path.join(self._storage_dir, file_id)
                with self._files_lock:
                    file_entry = self._files.get(file_id)
                    if not file_entry:
                        raise ValueError(
                            "Unable to find file id: {}".format(file_id))
                    if file_complete:
                        del self._files[file_id]
            else:
                file_id = str(uuid.uuid4()).lower()
                with open(os.path.join(self._storage_work_dir, file_id), "w"):
                    pass
                file_dir = os.path.join(self._storage_dir, file_id)
                os.makedirs(file_dir)
                file_entry = {
                    FileStoreParam.FILE_NAME: file_name,
                    FileStoreParam.FILE_SEGMENTS_RECEIVED: 0,
                    self._FILE_HASHER: hashlib.md5()
                }
                with self._files_lock:
                    self._files[file_id] = file_entry

            result = {FileStoreParam.FILE_ID: file_id}
            if file_complete == FileStoreParam.FILE_CANCEL:
                workdir_file = os.path.join(self._storage_work_dir, file_id)
                if os.path.exists(workdir_file):
                    os.remove(workdir_file)
                shutil.rmtree(file_dir)
                result[FileStoreParam.FILE_COMPLETE] = \
                    FileStoreParam.FILE_CANCEL
            else:
                file_name = os.path.join(file_dir,
                                         file_entry[FileStoreParam.FILE_NAME])
                segment = request.payload
                segments_written = file_entry[
                    FileStoreParam.FILE_SEGMENTS_RECEIVED]
                if (segments_written + 1) == file_segment_number:
                    with open(file_name, "ab+") as file_handle:
                        if segment:
                            file_handle.write(segment)
                            file_entry[self._FILE_HASHER].update(segment)
                    file_entry[FileStoreParam.FILE_SEGMENTS_RECEIVED] += 1
                else:
                    raise ValueError(
                        "Unexpected segment. Expected: '{}'. Received: '{}'".
                            format(segments_written + 1,
                                   file_segment_number)
                    )
                result[FileStoreParam.FILE_SEGMENTS_RECEIVED] = \
                    file_entry[FileStoreParam.FILE_SEGMENTS_RECEIVED]
                if file_complete == FileStoreParam.FILE_STORE:
                    store_error = None
                    stored_file_size = os.path.getsize(file_name)
                    if stored_file_size != file_size:
                        store_error = "Unexpected file size. Expected: '" + \
                            str(stored_file_size) + "'. Received: '" + \
                            str(file_size) + "'."
                    if stored_file_size:
                        stored_file_hash = file_entry[
                            self._FILE_HASHER].hexdigest()
                        if stored_file_hash != file_hash:
                            store_error = "Unexpected file hash. Expected: " + \
                                          "'" + str(stored_file_hash) + \
                                          "'. Received: '" + \
                                          str(file_hash) + "'."
                    workdir_file = os.path.join(self._storage_work_dir, file_id)
                    if os.path.exists(workdir_file):
                        os.remove(workdir_file)
                    if store_error:
                        shutil.rmtree(file_dir)
                        raise ValueError(
                            "File storage error: %s".format(store_error))
                    result[FileStoreParam.FILE_COMPLETE] = \
                        FileStoreParam.FILE_STORE

            # Create response
            res = Response(request)

            # Set payload
            MessageUtils.dict_to_json_payload(res, result)

            # Send response
            self._app.client.send_response(res)

        except Exception as ex:
            logger.exception("Error handling request")
            err_res = ErrorResponse(request, error_code=0,
                            error_message=MessageUtils.encode(str(ex)))
            self._app.client.send_response(err_res)
