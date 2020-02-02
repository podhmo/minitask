from __future__ import annotations
import typing as t
import logging
import contextlib
import dataclasses
from minitask.transport import console
from minitask.q import Q, PickleFormat, QueueLike
from .types import T


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Config:
    sensitive: bool = False


class Manager(contextlib.ExitStack):
    def __init__(self, config: t.Optional[Config] = None):
        self.config = config or Config()

    @contextlib.contextmanager
    def open_writer_queue(
        self, uid: t.Optional[str], *, force: bool = False
    ) -> t.Iterator[Q[T]]:
        try:
            with console.create_writer_port(uid) as wf:
                yield Q(wf, format_protocol=PickleFormat(), adapter=_QueueAdapter)
        except Exception as e:
            if self.config.sensitive:
                raise
            logger.warning("error occured: %s", e, exc_info=True)

    @contextlib.contextmanager
    def open_reader_queue(self, uid: t.Optional[str]) -> t.Iterator[Q[T]]:
        try:
            with console.create_reader_port(uid) as rf:
                yield Q(rf, format_protocol=PickleFormat(), adapter=_QueueAdapter)
        except BrokenPipeError as e:
            logger.info("broken type: %s", e)
        except Exception as e:
            if self.config.sensitive:
                raise
            logger.warning("error occured: %s", e, exc_info=True)


class _QueueAdapter(QueueLike[bytes]):
    def __init__(self, port: t.IO[bytes]) -> None:
        self.port = port

    def put(self, b: bytes) -> None:
        console.write(b, file=self.port)

    def get(self) -> t.Tuple[t.Optional[bytes], t.Callable[[], None]]:
        b = console.read(file=self.port)  # type:bytes
        if not b:
            return None, self._noop
        return b, self._noop

    def _noop(self) -> None:
        pass
