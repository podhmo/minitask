from __future__ import annotations
import typing as t
import typing_extensions as tx
import logging
from .q import FormatProtocol
from .q import PickleFormat  # TODO: omit

logger = logging.getLogger(__name__)


T = t.TypeVar("T")


class Communication(tx.Protocol[T]):
    def create_writer_port(self, endpoint: str) -> t.IO[bytes]:
        ...

    def create_reader_port(self, endpoint: str) -> t.IO[bytes]:
        ...

    def create_reader_buffer(
        self, recv: t.Callable[[], T]
    ) -> t.Tuple[t.Iterable[T], t.Optional[t.Callable[[], None]]]:
        ...

    def write(self, body: bytes, *, file: t.IO[bytes]) -> None:
        ...

    def read(self, *, file: t.IO[bytes]) -> None:
        ...


class IPC:

    # TODO: type
    def __init__(
        self,
        *,
        format_protocol: t.Optional[FormatProtocol[bytes]] = None,
        communication: t.Optional[Communication] = None,
    ):
        if format_protocol is None:
            format_protocol = PickleFormat()
        self.format_protocol = format_protocol
        if communication is None:
            from minitask.communication import namedpipe

            communication = namedpipe
        self.communication = communication

    def connect(self, endpoint: str, *, sensitive: bool = False) -> InternalReceiver:
        io = self.communication.create_reader_port(endpoint)
        assert io is not None, io
        return InternalReceiver(
            io,
            format_protocol=self.format_protocol,
            communication=self.communication,
            sensitive=sensitive,
        )

    def serve(self, endpoint: str, *, sensitive: bool = False) -> InternalReceiver:
        io = self.communication.create_writer_port(endpoint)
        assert io is not None, io
        return InternalSender(
            io,
            format_protocol=self.format_protocol,
            communication=self.communication,
            sensitive=sensitive,
        )


class InternalReceiver(t.Generic[T]):
    def __init__(
        self,
        io: t.IO[bytes],
        *,
        format_protocol: FormatProtocol[bytes],
        communication: Communication,
        sensitive: bool,
    ) -> None:
        self.io = io
        self.format_protocol = format_protocol
        self.communication = communication
        self.sensitive = sensitive
        self._buffer: t.Optional[t.Iterable[T]] = None
        self._teardown: t.Optional[t.Callable[[], None]] = None

    def recv(self) -> T:
        msg = self.communication.read(file=self.io)
        if not msg:
            return None
        return self.format_protocol.decode(msg)

    def __iter__(self) -> t.Iterable[T]:
        self._buffer, teardown = self.communication.create_reader_buffer(self.recv)
        if teardown is not None:
            self._teardown = teardown
        return self._buffer

    def __enter__(self):
        return self

    def _need_save_exception(self, typ: t.Type[t.Any]) -> bool:
        return issubclass(typ, KeyboardInterrupt)

    def __exit__(self, typ, val, tb):
        if self.io is not None:
            self.io.close()
            self.io = None  # TODO: lock? (semaphore?)

        if typ is not None:
            # e.g. Ctrl+c
            if self._need_save_exception(typ):
                if self._teardown is not None:
                    self._teardown()
            else:
                logger.warn("error occured: %s", val, exc_info=True)
        return not self.sensitive


class InternalSender:
    def __init__(
        self,
        io: t.IO[bytes],
        *,
        format_protocol: FormatProtocol[bytes],
        communication: Communication,
        sensitive: bool,
    ) -> None:
        self.io = io
        self.format_protocol = format_protocol
        self.communication = communication
        self.sensitive = sensitive

    def send(self, message: T) -> bytes:
        b = self.format_protocol.encode(message)
        return self.communication.write(b, file=self.io)

    def __enter__(self):
        return self

    def __exit__(self, typ, val, tb):
        if self.io is None:
            logger.warn("already closed, but exception is cached: %r", val)
            return not self.sensitive

        if typ is not None:
            if issubclass(typ, BrokenPipeError):
                logger.info("broken type: %r", val)
                self.io = None
                return not self.sensitive

            logger.warn("error occured: %s", val, exc_info=True)

            if val is None:
                val = typ()
            b = self.format_protocol.encode(val)
            self.communication.write(b, file=self.io)

        if self.io is not None:
            self.io.close()
            self.io = None  # TODO: lock?
        return not self.sensitive
