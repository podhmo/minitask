from __future__ import annotations
import typing as t
import typing_extensions as tx
import logging
import pathlib
import tempfile
import contextlib
from minitask.langhelpers import reify
from minitask.transport import namedpipe
from minitask.q import Q, PickleFormat
from .types import T
from ._gensym import IDGenerator
from ._subprocess import SpawnProcessManagerBase


logger = logging.getLogger(__name__)


class Manager(SpawnProcessManagerBase):
    class OptionDict(tx.TypedDict):
        dirpath: t.Optional[str]
        sensitive: bool

    def __init__(self, dirpath: t.Optional[str] = None, *, sensitive: bool = False):
        self.dirpath: t.Optional[str] = dirpath
        self.sensitive = sensitive
        super().__init__()

    @reify
    def tempdir(self) -> tempfile.TemporaryDirectory[str]:
        tempdir = tempfile.TemporaryDirectory()
        self.dirpath = tempdir.name
        logger.info("create tempdir %s", tempdir.name)
        return tempdir

    @reify
    def _gensym(self) -> t.Callable[[], str]:
        return IDGenerator()

    def __enter__(self) -> Manager:
        return self

    def __exit__(
        self,
        exc: t.Optional[t.Type[BaseException]],
        value: t.Optional[BaseException],
        tb: t.Any,
    ) -> tx.Literal[False]:
        super().__exit__(exc, value, tb)
        logger.info("remove tempdir %s", self.tempdir.name)
        self.tempdir.__exit__(exc, value, tb)
        return False  # raise error

    def generate_uid(self, suffix: t.Optional[t.Union[int, str]] = None,) -> str:
        if suffix is None:
            suffix = self._gensym()
        return str(pathlib.Path(self.tempdir.name) / f"worker.{suffix}.fifo")

    @contextlib.contextmanager
    def open_writer_queue(self, uid: str, *, force: bool = False) -> t.Iterator[Q[T]]:
        try:
            with namedpipe.create_writer_port(uid, force=force) as wf:
                yield Q(wf, format_protocol=PickleFormat(), adapter=_QueueAdapter)
        except BrokenPipeError as e:
            logger.info("broken type: %s", e)
        except Exception as e:
            if self.sensitive:
                raise
            logger.warning("error occured: %s", e, exc_info=True)

    @contextlib.contextmanager
    def open_reader_queue(self, uid: str) -> t.Iterator[Q[T]]:
        try:
            with namedpipe.create_reader_port(uid) as rf:
                yield Q(rf, format_protocol=PickleFormat(), adapter=_QueueAdapter)
        except BrokenPipeError as e:
            logger.info("broken type: %s", e)
        except Exception as e:
            if self.sensitive:
                raise
            logger.warning("error occured: %s", e, exc_info=True)


class _QueueAdapter:
    def __init__(self, port: t.IO[bytes]) -> None:
        self.port = port

    def put(self, b: bytes) -> None:
        # import fcntl
        # fcntl.flock(self.port.fileno(), fcntl.LOCK_EX)
        namedpipe.write(b, file=self.port)
        # fcntl.flock(self.port.fileno(), fcntl.LOCK_UN)

    def get(self) -> t.Tuple[t.Optional[bytes], t.Callable[[], None]]:
        b = namedpipe.read(file=self.port)  # type:bytes
        if not b:
            return None, self._noop
        return b, self._noop

    def _noop(self) -> None:
        pass


def _use() -> None:
    from .types import WorkerManager

    _: WorkerManager = Manager()
