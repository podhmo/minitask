from __future__ import annotations
import typing as t
import typing_extensions as tx

QLike = t.Any
T = t.TypeVar("T")
WorkerT = t.TypeVar("WorkerT", covariant=True)


class WorkerCallable(tx.Protocol):
    def __call__(self, m: WorkerManager, q: QLike) -> None:
        ...


class WorkerManager(tx.Protocol):
    def spawn(self, target: WorkerCallable, *, uid: str) -> t.Any:
        ...

    def __len__(self) -> int:
        ...

    def wait(self, *, check: bool = True) -> None:
        ...

    def generate_uid(self, suffix: t.Optional[t.Union[int, str]] = None,) -> str:
        ...

    def open_writer_queue(
        self, uid: str, *, force: bool = False
    ) -> t.ContextManager[QLike]:
        ...

    def open_reader_queue(self, uid: str) -> t.ContextManager[QLike]:
        ...
