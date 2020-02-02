import typing as t
import json


def extract(
    ob: t.Any, typ: t.Type[t.Any], *, _missing: object = object()
) -> t.Dict[str, t.Any]:
    options = {}
    for k in t.get_type_hints(typ).keys():
        v = getattr(ob, k, _missing)
        if v is not _missing:
            options[k] = v
    return options


def dumps(options: t.Dict[str, t.Any]) -> str:
    return json.dumps(options)


def loads(options: t.Union[str, t.Dict[str, t.Any], None]) -> t.Dict[str, t.Any]:
    if options is None:
        return {}
    elif isinstance(options, str):
        return t.cast(t.Dict[str, t.Any], json.loads(options))
    else:
        return options


if __name__ == "__main__":
    import typing_extensions as tx
    import dataclasses

    class PersonDict(tx.TypedDict):
        name: str
        age: int

    @dataclasses.dataclass
    class Person:
        name: str = "Foo"
        age: int = 20
        nickname: str = "F"

    print("extract")
    print(extract(Person(), PersonDict))
    print(dumps(extract(Person(), PersonDict)))
    print(loads('{"name": "age", "age": 20}'))
