from __future__ import annotations
import typing as t
import typing_extensions as tx
import logging

logger = logging.getLogger(__name__)


T = t.TypeVar("T")


class Communication(tx.Protocol[T]):
    def create_writer_port(self, endpoint: str) -> t.IO[bytes]:
        ...

    def create_reader_port(self, endpoint: str) -> t.IO[bytes]:
        ...

    def write(self, body: bytes, *, file: t.IO[bytes]) -> None:
        ...

    def read(self, *, file: t.IO[bytes]) -> None:
        ...


class Serialization(tx.Protocol[T]):
    def create_error_message(self, val: Exception) -> T:
        ...

    def serialize(self, val: T) -> bytes:
        ...

    def deserialize(self, b: bytes) -> T:
        ...


class IPC:

    # TODO: type
    def __init__(
        self,
        *,
        serialization: t.Optional[Serialization] = None,
        communication: t.Optional[Communication] = None,
    ):
        if serialization is None:
            from minitask.serialization import raw

            serialization = raw
        self.serialization = serialization
        if communication is None:
            from minitask.communication import namedpipe

            communication = namedpipe
        self.communication = communication

    def connect(self, endpoint: str, *, sensitive: bool = False) -> InternalReceiver:
        io = self.communication.create_reader_port(endpoint)
        assert io is not None, io
        return InternalReceiver(
            io,
            serialization=self.serialization,
            communication=self.communication,
            sensitive=sensitive,
        )

    def serve(self, endpoint: str, *, sensitive: bool = False) -> InternalReceiver:
        io = self.communication.create_writer_port(endpoint)
        assert io is not None, io
        return InternalSender(
            io,
            serialization=self.serialization,
            communication=self.communication,
            sensitive=sensitive,
        )


class InternalReceiver(t.Generic[T]):
    def __init__(
        self,
        io: t.IO[bytes],
        *,
        serialization: Serialization,
        communication: Communication,
        sensitive: bool,
    ) -> None:
        self.io = io
        self.serialization = serialization
        self.communication = communication
        self.sensitive = sensitive

    def recv(self) -> T:
        msg = self.communication.read(file=self.io)
        if not msg:
            return None
        return self.serialization.deserialize(msg)

    def __iter__(self) -> t.Iterable[T]:
        import queue
        import threading

        q = queue.Queue()

        def _peek():
            while True:
                item = self.recv()
                if item is None:
                    q.put(None)  # finish
                    break
                q.put(item)

        th = threading.Thread(target=_peek)
        th.start()

        while True:
            item = q.get()
            if item is None:
                q.task_done()
                break
            yield item
            q.task_done()
        q.join()

    def __enter__(self):
        return self

    def __exit__(self, typ, val, tb):
        if self.io is not None:
            self.io.close()
            self.io = None  # TODO: lock? (semaphore?)

        if typ is not None:
            logger.warn("error occured: %s", val, exc_info=True)
        return not self.sensitive


class InternalSender:
    def __init__(
        self,
        io: t.IO[bytes],
        *,
        serialization: Serialization,
        communication: Communication,
        sensitive: bool,
    ) -> None:
        self.io = io
        self.serialization = serialization
        self.communication = communication
        self.sensitive = sensitive

    def send(self, message: T) -> bytes:
        b = self.serialization.serialize(message)
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
            message = self.serialization.create_error_message(val)
            b = self.serialization.serialize(message)
            self.communication.write(b, file=self.io)

        if self.io is not None:
            self.io.close()
            self.io = None  # TODO: lock?
        return not self.sensitive
