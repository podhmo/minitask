import typing as t
import sys


def fullmodulename(ob: t.Any) -> str:
    if not hasattr(ob, "__name__"):
        ob = ob.__class__
    return f"{ob.__module__}:{ob.__name__}"


def fullfilename(ob: t.Any) -> str:
    if not hasattr(ob, "__name__"):
        ob = ob.__class__
    return f"{sys.modules[ob.__module__].__file__}:{ob.__name__}"
