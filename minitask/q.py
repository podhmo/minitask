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
        # logger.debug("encode: %r -> %r", v, b)
        return b

    def decode(self, b: bytes) -> t.Any:
        v = pickle.loads(b)
        # logger.debug("decode: %r <- %r", v, b)
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
        if val is None:
            return None, self.q.task_done
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


from minitask.transport import namedpipe


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


class SubprocessExecutor:
    def __init__(self):
        self.processes = []

    def spawn(self, target, *, endpoint, format_protocol=None, transport=None):
        import sys
        import subprocess

        if format_protocol is not None:
            format_protocol = fullname(format_protocol)
        else:
            format_protocol = "minitask.q:PickleFormat"

        if transport is not None:
            transport = fullname(transport)
        else:
            transport = "minitask.transport.namedpipe"

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
            "--transport",
            transport,
        ]

        p = subprocess.Popen(cmd)
        self.processes.append(p)
        return p

    def __len__(self):
        return len(self.processes)

    def wait(self):
        for p in self.processes:
            p.wait()
