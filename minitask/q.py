import typing as t
import typing_extensions as tx
import sys
import dataclasses
import pickle
import logging

logger = logging.getLogger(__name__)
K = t.TypeVar("K")
T = t.TypeVar("T")


@dataclasses.dataclass
class Message(t.Generic[T]):
    body: T
    metadata: t.Dict[str, t.Any] = dataclasses.field(default_factory=dict)


class FormatProtocol(tx.Protocol[K]):
    def encode(self, v: t.Any) -> K:
        ...

    def decode(self, b: K) -> t.Any:
        ...


class PickleFormat(FormatProtocol[bytes]):
    def encode(self, v: t.Any) -> bytes:
        b = pickle.dumps(v)
        logger.debug("encode: %r -> %r", v, b)
        return b

    def decode(self, b: bytes) -> t.Any:
        v = pickle.loads(b)
        logger.debug("decode: %r <- %r", v, b)
        return v


class Q(t.Generic[T]):
    """queue"""

    # todo: typing
    def __init__(self, q: t.Any, format_protocol: t.Optional[FormatProtocol[K]] = None):
        self.q = q
        self.p = format_protocol

    def put(self, val: t.Any) -> None:
        if self.p is not None:
            val = self.p.encode(val)
        self.q.put(val)

    def get(self) -> t.Tuple[t.Optional[Message[T]], t.Callable[..., None]]:
        val = self.q.get()
        if self.p is not None:
            val = self.p.decode(val)
        if val is None:
            return None, self.q.task_done
        return Message(val), self.q.task_done

    def join(self) -> None:
        self.q.join()


def consume(q: Q[T]) -> t.Iterator[T]:
    while True:
        m, task_done = q.get()
        if m is None:
            task_done()
            break
        yield m.body
        task_done()


from minitask.communication import namedpipe


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
    return f"{sys.modules[ob.__module__].__file__}:{ob.__name__}"


class ThreadingExecutor:
    def __init__(self):
        self.threads = []

    def spawn(self, target):
        import threading

        th = threading.Thread(target=target)
        self.threads.append(th)
        th.start()

    def wait(self):
        for th in self.threads:
            th.join()


class SubprocessExecutor:
    def __init__(self):
        self.processes = []

    def spawn(self, target, *, endpoint, format_protocol=None, communication=None):
        import sys
        import subprocess

        if format_protocol is not None:
            format_protocol = fullname(format_protocol)
        else:
            format_protocol = "minitask.q:PickleFormat"

        if communication is not None:
            communication = fullname(communication)
        else:
            communication = "minitask.communication.namedpipe"

        cmd = [
            sys.executable,
            "-m",
            "minitask.tool",
            "worker",
            "--endpoint",
            endpoint,
            "--format-protocol",
            format_protocol,
            "--handler",
            fullname(target),
            "--communication",
            communication,
        ]

        p = subprocess.Popen(cmd)
        self.processes.append(p)
        return p

    def wait(self):
        for p in self.processes:
            p.wait()


def open_port(endpoint: str, mode):
    import pathlib

    if mode == "r":
        return namedpipe.create_reader_port(endpoint)
    path = pathlib.Path(endpoint)
    if path.exists():
        path.unlink(missing_ok=True)
    return namedpipe.create_writer_port(str(path))
