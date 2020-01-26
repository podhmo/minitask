from __future__ import annotations
import typing as t
from tinyrpc.protocols import jsonrpc
from tinyrpc.protocols import RPCRequest

# TODO: type
Serialization = t.Any
Network = t.Any


class IPC:
    # from tinyrpc.protocols import msgpackrpc

    # def __init__(
    #     self, *, serialization=msgpackrpc.MSGPACKRPCProtocol(), network=network
    # ):

    # TODO: type
    def __init__(
        self,
        *,
        serialization: t.Optional[Serialization] = None,
        network: t.Optional[Network] = None,
    ):
        self.serialization = serialization or jsonrpc.JSONRPCProtocol()
        self.network = network

    def connect(self, endpoint: str,) -> InternalRead:
        io = self.network.create_reader_port(endpoint)
        return InternalRead(io, serialization=self.serialization, network=self.network)

    def serve(self, endpoint: str,) -> InternalRead:
        io = self.network.create_writer_port(endpoint)
        return InternalWrite(io, serialization=self.serialization, network=self.network)


class InternalRead:
    def __init__(self, io: t.IO[bytes], *, serialization, network) -> None:
        self.io = io
        self.serialization = serialization
        self.network = network

    def read(self) -> RPCRequest:
        msg = self.network.read(port=self.io)
        if not msg:
            return None
        return self.serialization.parse_request(msg)

    def __enter__(self):
        return self

    def __exit__(self, typ, val, tb):
        # TODO: exception is raised.

        if self.io is not None:
            self.io.close()
            self.io = None  # TODO: lock? (semaphore?)


class InternalWrite:
    def __init__(self, io: t.IO[bytes], *, serialization, network) -> None:
        self.io = io
        self.serialization = serialization
        self.network = network

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
        return self.network.write(req.serialize(), port=self.io)

    def __enter__(self):
        return self

    def __exit__(self, typ, val, tb):
        if self.io is not None:
            self.io.close()
            self.io = None  # TODO: lock?
