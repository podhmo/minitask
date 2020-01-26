from __future__ import annotations
import typing as t
import logging

logger = logging.getLogger(__name__)
# TODO: type
Serialization = t.Any
Port = t.Any
Message = t.Any
T = t.TypeVar("T")


class IPC:
    # from tinyrpc.protocols import msgpackrpc

    # def __init__(
    #     self, *, serialization=msgpackrpc.MSGPACKRPCProtocol(), port=port
    # ):

    # TODO: type
    def __init__(
        self,
        *,
        serialization: t.Optional[Serialization] = None,
        port: t.Optional[Port] = None,
    ):
        self.serialization = serialization
        if serialization is None:
            from minitask.serialization import jsonrpc

            self.serialization = jsonrpc
        self.port = port

    def connect(self, endpoint: str, *, sensitive: bool = True) -> InternalReceiver:
        io = self.port.create_reader_port(endpoint)
        assert io is not None, io
        return InternalReceiver(
            io, serialization=self.serialization, port=self.port, sensitive=sensitive
        )

    def serve(self, endpoint: str, *, sensitive: bool = True) -> InternalReceiver:
        io = self.port.create_writer_port(endpoint)
        assert io is not None, io
        return InternalSender(
            io, serialization=self.serialization, port=self.port, sensitive=sensitive
        )


class InternalReceiver(t.Generic[T]):
    def __init__(
        self, io: t.IO[bytes], *, serialization, port: Port, sensitive: bool
    ) -> None:
        self.io = io
        self.serialization = serialization
        self.port = port
        self.sensitive = sensitive

    def recv(self) -> T:
        msg = self.port.read(file=self.io)
        if not msg:
            return None
        return self.serialization.deserialize(msg)

    def __iter__(self) -> t.Iterable[T]:
        while True:
            msg = self.recv()
            if msg is None:
                break
            yield msg

    def __enter__(self):
        return self

    def __exit__(self, typ, val, tb):
        # TODO: exception is raised.
        if self.io is not None:
            self.io.close()
            self.io = None  # TODO: lock? (semaphore?)

        if typ is not None:
            logger.warn("error occured: %s", val, exc_info=True)
        return not self.sensitive


class InternalSender:
    def __init__(
        self, io: t.IO[bytes], *, serialization, port: Port, sensitive: bool
    ) -> None:
        self.io = io
        self.serialization = serialization
        self.port = port
        self.sensitive = sensitive

    def send(self, message: T) -> bytes:
        b = self.serialization.serialize(message)
        return self.port.write(b, file=self.io)

    def __enter__(self):
        return self

    def __exit__(self, typ, val, tb):
        if self.io is None:
            logger.warn("already closed, but exception is cached: %r", val)
            return not self.sensitive

        # TODO: add error handling?
        if typ is not None:
            if issubclass(typ, BrokenPipeError):
                logger.info("broken type: %r", val)
                self.io = None
                return not self.sensitive

            if val is None:
                val = typ()
            message = self.serialization.create_error_message(val)
            b = self.serialization.serialize(message)
            self.port.write(b, file=self.io)

        if self.io is not None:
            self.io.close()
            self.io = None  # TODO: lock?
        return not self.sensitive
