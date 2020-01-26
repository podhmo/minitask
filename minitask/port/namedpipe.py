import typing as t
import os
import time
import logging
from ._base import read, write  # noqa 410


logger = logging.getLogger(__name__)


def create_writer_port(endpoint: str, retries=[0.1, 0.2, 0.2, 0.4]) -> t.IO[bytes]:
    def _opener(path: str, flags: int) -> int:
        return os.open(path, os.O_WRONLY)  # NOT O_CREAT

    logger.info("create fifo: %s", endpoint)
    exc = None
    for i, waittime in enumerate(retries):
        try:
            os.mkfifo(str(endpoint))  # TODO: force option?
            return open(endpoint, "wb", opener=_opener)
        except FileNotFoundError as e:
            exc = e
            logger.debug("%r is not found, waiting, retry=%d", endpoint, i)
            time.sleep(waittime)
    raise exc


def create_reader_port(
    endpoint: str, retries=[0.1, 0.2, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8]
) -> t.IO[bytes]:
    exc = None
    for i, waittime in enumerate(retries, 1):
        try:
            io = open(endpoint, "rb")
            return io
        except FileNotFoundError as e:
            exc = e
            logger.debug("%r is not found, waiting, retry=%d", endpoint, i)
            time.sleep(waittime)
    raise exc
