import typing as t
import os
import time
import logging
import pathlib
from ._base import read, write  # noqa 410
from ._buffer import InmemoryQueueBuffer
from ._gensym import IDGenerator

logger = logging.getLogger(__name__)


def create_endpoint(
    uid: t.Optional[t.Union[int, str]] = None, dirpath: str = ".", _gensym=IDGenerator()
) -> pathlib.Path:
    if uid is None:
        uid = _gensym()
    return pathlib.Path(dirpath) / f"worker.{uid}.fifo"


def create_writer_port(
    endpoint: str, *, retries=[0.1, 0.2, 0.2, 0.4], force=False
) -> t.IO[bytes]:
    path = pathlib.Path(endpoint)
    if force and path.exists():
        path.unlink(missing_ok=True)

    def _opener(path: str, flags: int) -> int:
        return os.open(path, os.O_WRONLY)  # NOT O_CREAT

    logger.info("open fifo[W]: %s", endpoint)
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
    endpoint: str, *, retries=[0.1, 0.2, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8]
) -> t.IO[bytes]:
    exc = None
    for i, waittime in enumerate(retries, 1):
        try:
            logger.info("open fifo[R]: %s", endpoint)
            io = open(endpoint, "rb")
            return io
        except FileNotFoundError as e:
            exc = e
            logger.debug("%r is not found, waiting, retry=%d", endpoint, i)
            time.sleep(waittime)
    raise exc


def create_reader_buffer(
    recv: t.Callable[[], t.Any]
) -> t.Tuple[t.Iterable[t.Any], t.Optional[t.Callable[[], None]]]:
    buf = InmemoryQueueBuffer(recv)
    buf.load()
    teardown = buf.save
    return iter(buf), teardown
