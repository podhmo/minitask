from __future__ import annotations
import typing as t
import typing_extensions as tx


K = t.TypeVar("K")


class FormatProtocol(tx.Protocol[K]):
    def encode(self, v: t.Any) -> K:
        ...

    def decode(self, b: K) -> t.Any:
        ...


class PickleFormat(FormatProtocol[bytes]):
    def __init__(self) -> None:
        import pickle

        self.pickle = pickle

    def encode(self, v: t.Any) -> bytes:
        b: bytes = self.pickle.dumps(v)  # type:ignore
        # logger.debug("encode: %r -> %r", v, b)
        return b

    def decode(self, b: bytes) -> t.Any:
        v = self.pickle.loads(b)  # type:ignore
        # logger.debug("decode: %r <- %r", v, b)
        return v
