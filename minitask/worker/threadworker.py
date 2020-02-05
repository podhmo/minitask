from __future__ import annotations
import typing as t
import typing_extensions as tx
import logging
import contextlib
import dataclasses
from functools import partial
import threading
from minitask.langhelpers import reify
from minitask.q import Q
from ._gensym import IDGenerator
from ._name import fullmodulename
from .types import WorkerCallable, T

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Config:
    pass


class Manager(contextlib.ExitStack):
    def __init__(self, config: t.Optional[Config] = None):
        super().__init__()
        self.config = config or Config()
        self.reader_count = 0

    def spawn(self, target: WorkerCallable, **kwargs: t.Any) -> threading.Thread:
        logger.info("spawn target=%r kwargs=%r", fullmodulename(target), kwargs)
        th = threading.Thread(target=partial(target, self, **kwargs))
        self.threads.append(th)
        th.start()
        return th

    def __len__(self) -> int:
        return len(self.threads)

    def __enter__(self) -> Manager:
        return self

    def __exit__(
        self,
        exc: t.Optional[t.Type[BaseException]],
        value: t.Optional[BaseException],
        tb: t.Any,
    ) -> tx.Literal[False]:
        self.wait()
        return False

    def generate_uid(self, suffix: t.Union[int, str, None] = None) -> str:
        if suffix is None:
            return self._gensym()
        return str(suffix)

    @contextlib.contextmanager
    def open_writer_queue(self, uid: str, *, force: bool = False) -> t.Iterator[Q[T]]:
        from minitask.transport import fake

        q = fake.create_writer_port(uid).q
        yield Q(q)
        for _ in range(self.reader_count):
            q.put(None)

    @contextlib.contextmanager
    def open_reader_queue(self, uid: str) -> t.Iterator[Q[T]]:
        from minitask.transport import fake

        self.reader_count += 1
        q = fake.create_reader_port(uid).q
        yield Q(q)

    def wait(self, check: bool = False) -> None:
        for th in self.threads:
            th.join()

    @reify
    def threads(self) -> t.List[threading.Thread]:
        return []

    @reify
    def _gensym(self) -> t.Callable[[], str]:
        return IDGenerator()


def _use() -> None:
    """type assertion check"""
    from .types import WorkerManager

    _: WorkerManager = Manager()
