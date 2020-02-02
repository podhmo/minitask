from __future__ import annotations
import typing as t
import typing_extensions as tx
import sys
import logging
import pathlib
import tempfile
import contextlib
import subprocess
from minitask.langhelpers import reify
from minitask.transport import namedpipe
from minitask.q import Q, PickleFormat
from .types import WorkerCallable, T
from ._gensym import IDGenerator
from ._name import fullfilename

logger = logging.getLogger(__name__)


class Manager(contextlib.ExitStack):
    def __init__(self, dirpath: t.Optional[str] = None, *, sensitive: bool = False):
        self.processes: t.List[subprocess.Popen[bytes]] = []
        self.dirpath: t.Optional[str] = dirpath
        self.sensitive = sensitive
        super().__init__()

    def spawn(self, target: WorkerCallable, *, uid: str) -> subprocess.Popen[bytes]:
        cmd = [
            sys.executable,
            "-m",
            "minitask.tool",
            "worker",
            "--uid",
            uid,
            "--manager",
            fullfilename(self),
            "--handler",
            fullfilename(target),
        ]
        if self.dirpath is not None:
            cmd.extend(["--dirpath", self.dirpath])

        logger.info("spawn cmd=%s", ", ".join(map(str, cmd)))
        p = subprocess.Popen(cmd)
        self.processes.append(p)
        return p

    def __len__(self) -> int:
        return len(self.processes)

    def wait(self, *, check: bool = True) -> None:
        for p in self.processes:
            try:
                p.wait()
                if check:
                    cp = subprocess.CompletedProcess(
                        p.args, p.returncode, stdout=p.stdout, stderr=p.stderr
                    )
                    cp.check_returncode()
            except KeyboardInterrupt:
                logger.info("keybord interrupted, pid=%d", p.pid)

    @reify
    def tempdir(self) -> tempfile.TemporaryDirectory[str]:
        tempdir = tempfile.TemporaryDirectory()
        self.dirpath = tempdir.name
        logger.info("create tempdir %s", tempdir.name)
        return tempdir

    @reify
    def _gensym(self) -> t.Callable[[], str]:
        g = IDGenerator()  # type:t.Callable[[], str]
        return g

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
                yield Q(_QueueAdapter(wf), format_protocol=PickleFormat())
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
                yield Q(_QueueAdapter(rf), format_protocol=PickleFormat())
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

    def get(self) -> t.Optional[bytes]:
        b = namedpipe.read(file=self.port)  # type:bytes
        if not b:
            return None
        return b

    def task_done(self) -> None:
        pass  # hmm


def _use() -> None:
    from .types import WorkerManager

    m: WorkerManager = Manager()
