import typing as t
import sys
import logging
import pathlib
import tempfile
import contextlib
from minitask.langhelpers import reify
from minitask.transport import namedpipe
from ._gensym import IDGenerator
from ..q import Q, PickleFormat

logger = logging.getLogger(__name__)


class ThreadingWorkerManager:
    def __init__(self):
        self.threads = []

    def spawn(self, target, *, endpoint: str, format_protocol=None, transport=None):
        import threading

        if format_protocol is None:
            format_protocol = PickleFormat
        if transport is None:
            from minitask.transport import namedpipe

            transport = namedpipe

        def worker():

            with namedpipe.create_reader_port(endpoint) as rf:
                q = Q(QueueLike(rf), format_protocol=format_protocol())
                target(q)

        th = threading.Thread(target=worker)
        self.threads.append(th)
        th.start()
        return th

    def __len__(self):
        return len(self.threads)

    def wait(self):
        for th in self.threads:
            th.join()


class SubprocessWorkerManager(contextlib.ExitStack):
    def __init__(self, dirpath: t.Optional[str] = None):
        self.processes = []
        self.dirpath = dirpath

        super().__init__()

    def spawn(self, target, *, endpoint):
        import sys
        import subprocess

        cmd = [
            sys.executable,
            "-m",
            "minitask.tool",
            "worker",
            "--endpoint",
            endpoint,
            "--manager",
            fullname(self),
            "--handler",
            fullname(target),
        ]
        if self.dirpath is not None:
            cmd.extend(["--dirpath", self.dirpath])

        p = subprocess.Popen(cmd)
        self.processes.append(p)
        return p

    def __len__(self):
        return len(self.processes)

    def wait(self):
        for p in self.processes:
            p.wait()

    @reify
    def tempdir(self):
        tempdir = tempfile.TemporaryDirectory()
        self.dirpath = tempdir.name
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

    @contextlib.contextmanager
    def open_writer_queue(self, endpoint: str, *, force: bool = False) -> t.Iterable[Q]:
        with namedpipe.create_writer_port(endpoint, force=force) as wf:
            yield Q(QueueLike(wf), format_protocol=PickleFormat())

    @contextlib.contextmanager
    def open_reader_queue(self, endpoint: str) -> t.Iterable[Q]:
        with namedpipe.create_reader_port(endpoint) as rf:
            yield Q(QueueLike(rf), format_protocol=PickleFormat())


class QueueLike:
    def __init__(self, port) -> None:
        self.port = port

    def put(self, b: bytes):
        namedpipe.write(b, file=self.port)

    def get(self) -> bytes:
        b = namedpipe.read(file=self.port)
        if not b:
            return None
        return b

    def task_done(self):
        pass  # hmm


def fullname(ob: t.Any) -> str:
    if not hasattr(ob, "__name__"):
        ob = ob.__class__
    return f"{sys.modules[ob.__module__].__file__}:{ob.__name__}"
