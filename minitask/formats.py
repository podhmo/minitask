from __future__ import annotations
import typing as t
import typing_extensions as tx
import dataclasses

K = t.TypeVar("K")
T = t.TypeVar("T")


@dataclasses.dataclass
class Message(t.Generic[T]):
    body: T
    metadata: t.Dict[str, t.Any] = dataclasses.field(default_factory=dict)


class FormatProtocol(tx.Protocol[K]):
    def encode(self, v: Message[t.Any]) -> K:
        ...

    def decode(self, b: K) -> t.Any:
        ...


class PickleMessageFormat(FormatProtocol[bytes]):
    def __init__(self) -> None:
        import pickle

        self.pickle = pickle

    def encode(self, m: Message[t.Any]) -> bytes:
        b: bytes = self.pickle.dumps(m.body)  # type:ignore
        # logger.debug("encode: %r -> %r", m, b)
        return b

    def decode(self, b: bytes) -> Message[t.Any]:
        v = self.pickle.loads(b)  # type:ignore
        # logger.debug("decode: %r <- %r", v, b)
        return Message(body=v)


class JSONMessageFormat(FormatProtocol[str]):
    def __init__(self) -> None:
        import json

        self.json = json

    def encode(self, m: Message[str]) -> str:
        b: str = self.json.dumps(m.body)  # type:ignore
        # logger.debug("encode: %r -> %r", v, b)
        return b

    def decode(self, b: str) -> Message[t.Dict[str, t.Any]]:
        v: t.Dict[str, t.Any] = self.json.loads(b)  # type:ignore
        # logger.debug("decode: %r <- %r", v, b)
        return Message(body=v)
