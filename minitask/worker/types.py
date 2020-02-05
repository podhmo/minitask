from __future__ import annotations
import typing as t
import typing_extensions as tx
from minitask.q import Q

T = t.TypeVar("T")
WorkerT = t.TypeVar("WorkerT", covariant=True)


class WorkerCallable(tx.Protocol):
    def __call__(self, m: WorkerManager, q: Q[T]) -> None:
        ...


class WorkerManager(tx.Protocol):
    def spawn(self, target: WorkerCallable, **kwargs: t.Any) -> t.Any:
        ...

    def __len__(self) -> int:
        ...

    def wait(self, *, check: bool = True) -> None:
        ...

    def generate_uid(self, suffix: t.Optional[t.Union[int, str]] = None,) -> str:
        ...

    def open_writer_queue(
        self, uid: str, *, force: bool = False
    ) -> t.ContextManager[Q[T]]:
        ...

    def open_reader_queue(self, uid: str) -> t.ContextManager[Q[T]]:
        ...
