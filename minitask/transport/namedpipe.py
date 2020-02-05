import typing as t
import os
import time
import logging
import pathlib
from ._base import read, write  # noqa 410
from ._buffer import InmemoryQueueBuffer

__all__ = [
    "read",
    "write",
    "create_endpoint",
    "create_writer_port",
    "create_reader_port",
]
logger = logging.getLogger(__name__)


def create_endpoint(uid: t.Union[int, str], *, dirpath: str) -> pathlib.Path:
    return pathlib.Path(dirpath) / f"worker.{uid}.fifo"


def create_writer_port(
    endpoint: str, *, retries: t.List[float] = [0.1, 0.2, 0.2, 0.4], force: bool = False
) -> t.IO[bytes]:
    path = pathlib.Path(endpoint)
    if force and path.exists():
        path.unlink()

    def _opener(path: str, flags: int) -> int:
        return os.open(path, os.O_WRONLY)  # NOT O_CREAT

    exc: t.Optional[Exception] = None
    for i, waittime in enumerate(retries):
        try:
            os.mkfifo(str(endpoint))  # TODO: force option?
            io = open(endpoint, "wb", opener=_opener)
            logger.info("open fifo[W]: %s", endpoint)
            return io
        except FileNotFoundError as e:
            exc = e
            logger.debug("%r is not found, waiting, retry=%d", endpoint, i)
            time.sleep(waittime)
    if exc is not None:
        raise exc
    raise RuntimeError("too much waiting")


def create_reader_port(
    endpoint: str,
    *,
    retries: t.List[float] = [0.1, 0.2, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8],
) -> t.IO[bytes]:
    exc: t.Optional[Exception] = None
    for i, waittime in enumerate(retries, 1):
        try:
            io = open(endpoint, "rb")
            logger.info("open fifo[R]: %s", endpoint)
            return io
        except FileNotFoundError as e:
            exc = e
            logger.debug("%r is not found, waiting, retry=%d", endpoint, i)
            time.sleep(waittime)
    if exc is not None:
        raise exc
    raise RuntimeError("too much waiting")


def create_reader_buffer(
    recv: t.Callable[[], t.Any]
) -> t.Tuple[InmemoryQueueBuffer, t.Optional[t.Callable[[], None]]]:
    buf = InmemoryQueueBuffer(recv)
    buf.load()
    teardown = buf.save
    return buf, teardown
