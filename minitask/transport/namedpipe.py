import typing as t
import os
import time
import logging
import pathlib
import tempfile
import contextlib
from minitask.langhelpers import reify
from ._base import read, write  # noqa 410
from ._buffer import InmemoryQueueBuffer
from ._gensym import IDGenerator

logger = logging.getLogger(__name__)


class ContextStack(contextlib.ExitStack):
    @reify
    def tempdir(self):
        tempdir = tempfile.TemporaryDirectory()
        logger.info("create tempdir %s", tempdir.name)
        return tempdir

    @reify
    def _gensym(self):
        return IDGenerator()

    def __enter__(self):
        return self

    def __exit__(self, exc, value, tb):
        logger.info("remove tempdir %s", self.tempdir.name)
        return self.tempdir.__exit__(exc, value, tb)

    def create_endpoint(
        self, uid: t.Optional[t.Union[int, str]] = None,
    ) -> pathlib.Path:
        if uid is None:
            uid = self._gensym()
        return pathlib.Path(self.tempdir.name) / f"worker.{uid}.fifo"

    def serve(self, endpoint: str, *, force: bool = False):
        return create_writer_port(endpoint, force=force)

    def connect(self, endpoint: str):
        return create_reader_port(endpoint)


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
