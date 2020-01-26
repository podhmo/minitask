import typing as t
import os
import time
import logging

logger = logging.getLogger(__name__)


def create_writer_port(endpoint: str) -> t.IO[bytes]:
    def _opener(path: str, flags: int) -> int:
        return os.open(path, os.O_WRONLY)  # NOT O_CREAT

    logger.info("create fifo: %s", endpoint)
    os.mkfifo(str(endpoint))  # TODO: force option?
    return open(endpoint, "wb", opener=_opener)


@staticmethod
def create_reader_port(
    endpoint: str, retries=[0.1, 0.2, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8]
) -> t.IO[bytes]:
    for i, waittime in enumerate(retries, 1):
        try:
            io = open(endpoint, "rb")
            return io
        except FileNotFoundError:
            time.sleep(waittime)
            logger.debug("%r is not found, waiting, retry=%d", endpoint, i)