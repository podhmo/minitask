from __future__ import annotations
import typing as t
import typing_extensions as tx
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
        self.latest = None

    def put(self, val: T, **metadata: t.Any) -> None:
        m = Message(val, metadata=metadata)
        if self.p is not None:
            m = self.p.encode(m)  # e.g. pickle.dumps
        self.q.put(m)

    def get(self) -> t.Tuple[Message[t.Optional[T]], t.Callable[..., None]]:
        m = self.q.get()
        if m is None:
            m = Message(None)  # xxx
        elif self.p is not None:
            m = self.p.decode(m)  # e.g. pickle.loads
        self.latest = m
        return m, self.q.task_done

    def join(self) -> None:
        self.q.join()


def consume(q: Q[T]) -> t.Iterator[T]:
    while True:
        m, task_done = q.get()
        if m.body is None:
            task_done()
            break
        yield m.body
        task_done()
