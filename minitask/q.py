from __future__ import annotations
import typing as t
import typing_extensions as tx
import queue
import logging

from .formats import FormatProtocol, Message

K = t.TypeVar("K")
T = t.TypeVar("T")
logger = logging.getLogger(__name__)


class QueueLike(tx.Protocol[T]):
    def get(self) -> t.Tuple[t.Optional[T], t.Dict[str, t.Any], t.Callable[[], None]]:
        ...

    def put(self, v: T) -> None:
        ...

    def join(self) -> None:
        ...


class _QueueAdapter(QueueLike[t.Any]):
    def __init__(self, q: queue.Queue[t.Any]) -> None:
        self.q = q

    def get(self) -> t.Tuple[t.Any, t.Dict[str, t.Any], t.Callable[[], None]]:
        return self.q.get(), {}, self.q.task_done

    def put(self, v: t.Any) -> None:
        self.q.put(v)

    def join(self) -> None:
        return self.q.join()


class Q(t.Generic[T]):
    """queue"""

    adapter: QueueLike[t.Any]

    def __init__(
        self,
        q: t.Any,
        format_protocol: t.Optional[FormatProtocol[K]] = None,
        adapter: t.Optional[t.Callable[[t.Any], QueueLike[t.Any]]] = _QueueAdapter,
    ) -> None:
        if adapter is None:
            self.adapter = q
        else:
            self.adapter = adapter(q)
        self.p = format_protocol
        self.latest: t.Optional[Message[t.Any]] = None

    def put(self, val: T, **metadata: t.Any) -> None:
        m = Message(val, metadata=metadata)
        if self.p is not None:
            v = self.p.encode(m)  # e.g. pickle.dumps
            self.adapter.put(v)
        else:
            self.adapter.put(m)

    def get(self) -> t.Tuple[Message[t.Optional[T]], t.Callable[..., None]]:
        body, metadata, task_done = self.adapter.get()
        if body is None:
            m: Message[t.Optional[T]] = Message(None)
        elif self.p is not None:
            m = self.p.decode(body)  # e.g. pickle.loads
            if metadata:
                m.metadata.update(metadata)
        else:
            m = body
        self.latest = m
        return m, task_done

    def join(self) -> None:
        self.adapter.join()

    def __iter__(self) -> t.Iterable[T]:
        return consume(self)


def consume(q: Q[T]) -> t.Iterator[T]:
    while True:
        m, task_done = q.get()
        if m.body is None:
            task_done()
            break
        yield m.body
        task_done()
