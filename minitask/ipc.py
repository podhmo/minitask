from __future__ import annotations
import typing as t
from tinyrpc.protocols import jsonrpc
from tinyrpc.protocols import RPCRequest

# TODO: type
Serialization = t.Any
Port = t.Any


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
        self.serialization = serialization or jsonrpc.JSONRPCProtocol()
        self.port = port

    def connect(self, endpoint: str,) -> InternalReader:
        io = self.port.create_reader_port(endpoint)
        return InternalReader(io, serialization=self.serialization, port=self.port)

    def serve(self, endpoint: str,) -> InternalReader:
        io = self.port.create_writer_port(endpoint)
        return InternalWriter(io, serialization=self.serialization, port=self.port)


class InternalReader:
    def __init__(self, io: t.IO[bytes], *, serialization, port: Port) -> None:
        self.io = io
        self.serialization = serialization
        self.port = port

    def read(self) -> RPCRequest:
        msg = self.port.read(file=self.io)
        if not msg:
            return None
        return self.serialization.parse_request(msg)

    def __iter__(self) -> t.Iterable[RPCRequest]:
        while True:
            msg = self.read()
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


class InternalWriter:
    def __init__(self, io: t.IO[bytes], *, serialization, port: Port) -> None:
        self.io = io
        self.serialization = serialization
        self.port = port

    def write(
        self,
        method: str,
        args: t.List[t.Any] = None,
        kwargs: t.Dict[str, t.Any] = None,
        one_way: bool = False,
    ):
        req = self.serialization.create_request(
            method, args=args, kwargs=kwargs, one_way=one_way
        )
        return self.port.write(req.serialize(), file=self.io)

    def __enter__(self):
        return self

    def __exit__(self, typ, val, tb):
        if self.io is not None:
            self.io.close()
            self.io = None  # TODO: lock?
