from __future__ import absolute_import
import hashlib
import logging
import os

try:
    from queue import Empty, Queue
except ImportError:
    from Queue import Empty, Queue

import shutil
import threading
import uuid

from dxlclient.callbacks import RequestCallback
from dxlclient.message import Request, Response, ErrorResponse  # pylint: disable=unused-import
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

    _ACTIVE_FILE_TIMEOUT = 1 # seconds

    _STORAGE_WORK_SUBDIR = ".workdir"
    _FILE_ERROR_MESSAGE = "error_message"
    _FILE_SEGMENT_QUEUE = "segment_queue"
    _FILE_HASHER = "hasher"
    _FILE_LOCK = "lock"
    _FILE_SEGMENT_QUEUE_EMPTY_CONDITION = "segment_queue_empty_condition"

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

        logger.info("Using file store at '%s'", os.path.abspath(storage_dir))
        self._storage_dir = storage_dir
        self._purge_incomplete_files()

        self._active_files_queue = Queue()
        self._active_files = {}
        self._active_files_lock = threading.RLock()

        self._thread = threading.Thread(target=self._write_segment_worker)
        self._thread.start()

    def _purge_incomplete_files(self):
        for incomplete_file_id in os.listdir(self._storage_work_dir):
            logger.info("Purging content for incomplete file id: '{}'".format(
                incomplete_file_id
            ))
            file_path = os.path.join(self._storage_dir, incomplete_file_id)
            if os.path.exists(file_path):
                shutil.rmtree(file_path)
            os.remove(os.path.join(self._storage_work_dir, incomplete_file_id))

    def _write_file_segment(self, file_handle, segment, file_entry):
        file_handle.write(segment)
        with file_entry[self._FILE_LOCK]:
            file_entry[self._FILE_HASHER].update(segment)
            file_entry[FileStoreParam.FILE_SEGMENTS_WRITTEN] += 1

    def _write_segment_worker(self):
        file_entry = self._active_files_queue.get()
        while file_entry is not None:
            file_id = file_entry[FileStoreParam.FILE_ID]
            with self._active_files_lock:
                if file_id in self._active_files:
                    continue
                else:
                    self._active_files[file_id] = file_entry
            file_name = file_entry[FileStoreParam.FILE_NAME]

            file_dir = os.path.join(self._storage_dir, file_id)
            full_file_path = os.path.join(file_dir, file_name)

            file_handle = open(full_file_path, "ab+")
            try:
                segment = file_entry[self._FILE_SEGMENT_QUEUE].get(
                    timeout=self._ACTIVE_FILE_TIMEOUT)
                while segment:
                    self._write_file_segment(file_handle, segment, file_entry)
                    segment = file_entry[self._FILE_SEGMENT_QUEUE].get(
                        timeout=self._ACTIVE_FILE_TIMEOUT)
            except Empty:
                pass
            finally:
                try:
                    file_handle.close()
                finally:
                    with self._active_files_lock:
                        del self._active_files[file_id]
                        with file_entry[self._FILE_LOCK]:
                            file_entry[
                                self._FILE_SEGMENT_QUEUE_EMPTY_CONDITION].notify()
            file_entry = self._active_files_queue.get()

    @staticmethod
    def _validate_requested_file_result(requested_file_result, file_id,
                                        file_size, file_hash):
        if requested_file_result:
            if requested_file_result == FileStoreParam.FILE_RESULT_CANCEL:
                if not file_id:
                    raise ValueError("File id to cancel must be specified")
            elif requested_file_result == FileStoreParam.FILE_RESULT_STORE:
                if file_size is None:
                    raise ValueError(
                        "File size must be specified for store request")
                if file_size is not None and not file_hash:
                    raise ValueError(
                        "File hash must be specified for store request")
            else:
                raise ValueError(
                    "Unexpected 'file_complete' value: '{}'".
                        format(requested_file_result))

    def _get_file_entry(self, file_id, file_name):
        if file_id:
            with self._files_lock:
                file_entry = self._files.get(file_id)
                if not file_entry:
                    raise ValueError(
                        "Unable to find file id: {}".format(file_id))
        else:
            file_id = str(uuid.uuid4()).lower()
            with open(os.path.join(self._storage_work_dir, file_id), "w"):
                pass
            file_dir = os.path.join(self._storage_dir, file_id)
            os.makedirs(file_dir)
            file_lock = threading.RLock()
            file_entry = {
                FileStoreParam.FILE_ID: file_id,
                FileStoreParam.FILE_NAME: file_name,
                FileStoreParam.FILE_SEGMENTS_RECEIVED: 0,
                FileStoreParam.FILE_SEGMENTS_WRITTEN: 0,
                FileStoreParam.FILE_RESULT: FileStoreParam.FILE_RESULT_PENDING,
                self._FILE_ERROR_MESSAGE: None,
                self._FILE_HASHER: hashlib.md5(),
                self._FILE_SEGMENT_QUEUE: Queue(),
                self._FILE_LOCK: file_lock,
                self._FILE_SEGMENT_QUEUE_EMPTY_CONDITION: \
                    threading.Condition(file_lock)
            }
            with self._files_lock:
                self._files[file_id] = file_entry
        return file_entry

    def _wait_segments_written(self, file_entry):
        file_id = file_entry[FileStoreParam.FILE_ID]

        with self._active_files_lock:
            if file_id in self._active_files:
                file_entry[self._FILE_SEGMENT_QUEUE].put(None)

        while not file_entry[self._FILE_SEGMENT_QUEUE].empty():
            file_entry[
                self._FILE_SEGMENT_QUEUE_EMPTY_CONDITION].wait()

    def _complete_file(self, file_entry, requested_file_result,
                       last_segment, error_message, file_size, file_hash):
        file_id = file_entry[FileStoreParam.FILE_ID]
        file_dir = os.path.join(self._storage_dir, file_id)

        self._wait_segments_written(file_entry)

        with self._files_lock:
            del self._files[file_id]

        workdir_file = os.path.join(self._storage_work_dir, file_id)
        if os.path.exists(workdir_file):
            os.remove(workdir_file)

        if requested_file_result == FileStoreParam.FILE_RESULT_STORE:
            store_error = None
            if error_message:
                store_error = error_message
            else:
                full_file_path = os.path.join(
                    file_dir,
                    file_entry[FileStoreParam.FILE_NAME])
                if last_segment:
                    with open(full_file_path, "ab+") as file_handle:
                        self._write_file_segment(file_handle, last_segment,
                                                 file_entry)
                stored_file_size = os.path.getsize(full_file_path)
                if stored_file_size != file_size:
                    store_error = \
                        "Unexpected file size. Expected: '" + \
                        str(stored_file_size) + "'. Received: '" + \
                        str(file_size) + "'."
                if stored_file_size:
                    stored_file_hash = file_entry[
                        self._FILE_HASHER].hexdigest()
                    if stored_file_hash != file_hash:
                        store_error = \
                            "Unexpected file hash. Expected: " + \
                            "'" + str(stored_file_hash) + \
                            "'. Received: '" + \
                            str(file_hash) + "'."
            if store_error:
                shutil.rmtree(file_dir)
                raise ValueError(
                    "File storage error: {}".format(store_error))
            result = FileStoreParam.FILE_RESULT_STORE
        else:
            shutil.rmtree(file_dir)
            result = FileStoreParam.FILE_RESULT_CANCEL

        return result

    def _process_segment(self, file_entry, segment, segment_number,
                         requested_file_result, file_size, file_hash):
        file_id = file_entry[FileStoreParam.FILE_ID]

        result = {FileStoreParam.FILE_ID: file_id}

        with file_entry[self._FILE_LOCK]:
            segments_received = file_entry[
                FileStoreParam.FILE_SEGMENTS_RECEIVED]

            if requested_file_result != FileStoreParam.FILE_RESULT_CANCEL:
                if (segments_received + 1) == segment_number:
                    file_entry[FileStoreParam.FILE_SEGMENTS_RECEIVED] = \
                        segments_received + 1
                else:
                    raise ValueError(
                        "Unexpected segment. Expected: '{}'. Received: '{}'".
                        format(segments_received + 1, segment_number))

            error_message = file_entry[self._FILE_ERROR_MESSAGE]

            if requested_file_result:
                result[FileStoreParam.FILE_RESULT] = \
                    self._complete_file(file_entry, requested_file_result,
                                        segment, error_message, file_size,
                                        file_hash)
            elif error_message:
                raise ValueError(
                    "File storage error: {}".format(error_message))
            else:
                with self._active_files_lock:
                    if file_id not in self._active_files:
                        self._active_files_queue.put(file_entry)
                    file_entry[self._FILE_SEGMENT_QUEUE].put(segment)

            result[FileStoreParam.FILE_SEGMENTS_RECEIVED] = \
                file_entry[FileStoreParam.FILE_SEGMENTS_RECEIVED]
            result[FileStoreParam.FILE_SEGMENTS_WRITTEN] = \
                file_entry[FileStoreParam.FILE_SEGMENTS_WRITTEN]

        return result

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

            requested_file_result = params.get(FileStoreParam.FILE_RESULT)
            self._validate_requested_file_result(
                requested_file_result, file_id, file_size, file_hash)

            segment_number = _get_value_as_int(
                params, FileStoreParam.FILE_SEGMENT_NUMBER)

            file_entry = self._get_file_entry(file_id, file_name)

            # Create response
            res = Response(request)

            result = self._process_segment(
                file_entry, request.payload, segment_number,
                requested_file_result, file_size, file_hash)

            # Set payload
            MessageUtils.dict_to_json_payload(res, result)

            # Send response
            self._app.client.send_response(res)

        except Exception as ex:
            logger.exception("Error handling request")
            err_res = ErrorResponse(request, error_code=0,
                                    error_message=MessageUtils.encode(str(ex)))
            self._app.client.send_response(err_res)
