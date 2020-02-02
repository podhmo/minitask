from __future__ import annotations
import typing as t
import typing_extensions as tx
import logging
import pathlib
import tempfile
import contextlib
import dataclasses
import subprocess
from minitask.langhelpers import reify
from minitask.transport import namedpipe
from minitask.q import Q, PickleFormat, QueueLike
from .types import T, WorkerCallable
from ._gensym import IDGenerator
from ._subprocess import spawn_worker_process, wait_processes


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Config:
    dirpath: t.Optional[str] = None
    sensitive: bool = False


class Manager(contextlib.ExitStack):
    def __init__(self, config: t.Optional[Config] = None):
        self.config = config or Config()
        super().__init__()

    def spawn(self, target: WorkerCallable, *, uid: str) -> subprocess.Popen[bytes]:
        p = spawn_worker_process(self, target, uid=uid, config=self.config)
        self.processes.append(p)
        return p

    def __len__(self) -> int:
        return len(self.processes)

    def __enter__(self) -> Manager:
        return self

    def __exit__(
        self,
        exc: t.Optional[t.Type[BaseException]],
        value: t.Optional[BaseException],
        tb: t.Any,
    ) -> tx.Literal[False]:
        self.wait()
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
            if self.config.sensitive:
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
            if self.config.sensitive:
                raise
            logger.warning("error occured: %s", e, exc_info=True)

    def wait(self, *, check: bool = True) -> None:
        wait_processes(self.processes)

    @reify
    def processes(self) -> t.List[subprocess.Popen[bytes]]:
        return []

    @reify
    def tempdir(self) -> tempfile.TemporaryDirectory[str]:
        tempdir = tempfile.TemporaryDirectory()
        self.config.dirpath = tempdir.name  # side-effect!
        logger.info("create tempdir %s", tempdir.name)
        return tempdir

    @reify
    def _gensym(self) -> t.Callable[[], str]:
        return IDGenerator()


class _QueueAdapter(QueueLike[bytes]):
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
