from __future__ import annotations
import typing as t
import typing_extensions as tx
import logging
import contextlib
from functools import partial
import threading
from minitask.langhelpers import reify
from minitask.q import Q
from ._gensym import IDGenerator
from ._name import fullmodulename
from .types import WorkerCallable, T

logger = logging.getLogger(__name__)


class Manager(contextlib.ExitStack):
    def __init__(self) -> None:
        self.threads: t.List[threading.Thread] = []
        super().__init__()

    @reify
    def _gensym(self) -> t.Callable[[], str]:
        g = IDGenerator()  # type: t.Callable[[], str]
        return g

    def spawn(self, fn: WorkerCallable, *, endpoint: str) -> threading.Thread:
        logger.info("spawn fn=%r endpoint=%r", fullmodulename(fn), endpoint)
        th = threading.Thread(target=partial(fn, self, endpoint))
        self.threads.append(th)
        th.start()
        return th

    def __len__(self) -> int:
        return len(self.threads)

    def wait(self, check: bool = False) -> None:
        for th in self.threads:
            th.join()

    def __enter__(self) -> Manager:
        return self

    def __exit__(
        self,
        exc: t.Optional[t.Type[BaseException]],
        value: t.Optional[BaseException],
        tb: t.Any,
    ) -> tx.Literal[False]:
        return False

    def create_endpoint(self, uid: t.Union[int, str, None] = None) -> str:
        if uid is None:
            return self._gensym()
        return str(uid)

    @contextlib.contextmanager
    def open_writer_queue(
        self, endpoint: str, *, force: bool = False
    ) -> t.Iterator[Q[T]]:
        from minitask.transport import fake

        q = fake.create_writer_port(endpoint).q
        yield Q(q)

    @contextlib.contextmanager
    def open_reader_queue(self, endpoint: str) -> t.Iterator[Q[T]]:
        from minitask.transport import fake

        q = fake.create_reader_port(endpoint).q
        yield Q(q)


def _use() -> None:
    from .types import WorkerManager

    m: WorkerManager = Manager()
