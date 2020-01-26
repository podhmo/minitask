from __future__ import annotations
import typing as t
import threading
import logging
from functools import partial

logger = logging.getLogger(__name__)


class Executor:
    def __init__(self):
        self.actions: t.Dict[t.Callable[..., t.Any], str] = {}
        self._threads: threading.Thread = []

    def register(self, fn):
        self.actions[fn] = fn.__name__
        return fn

    def __enter__(self) -> Executor:
        return self

    def __exit__(self, typ, val, tb) -> None:
        pass

    def spawn(self, fn, **kwargs) -> threading.Thread:
        assert fn in self.actions
        logger.info("spawn: fn=%r kwargs=%r", fn, kwargs)
        action = partial(fn, **kwargs)
        th = threading.Thread(target=action, daemon=True)  # daemon = True?
        th.start()
        self._threads.append(th)
        return th

    def create_endpoint(self, *, uid: int) -> str:
        return str(uid)

    def wait(self) -> None:
        for th in self._threads:
            th.join()
