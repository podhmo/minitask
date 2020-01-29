import pickle
import typing as t


def serialize(val: t.Dict[str, t.Any]) -> bytes:
    return pickle.dumps(val)


def deserialize(b: bytes) -> t.Dict[str, t.Any]:
    return pickle.loads(b)
