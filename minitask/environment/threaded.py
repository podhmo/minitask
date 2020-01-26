from __future__ import annotations
import threading
from functools import partial


class Environment:
    def __init__(self):
        self.actions = {}

    def register(self, fn):
        self.actions[fn] = fn.__name__
        return fn

    def __enter__(self):
        return self

    def __exit__(self, typ, val, tb):
        pass

    def spawn(self, fn, **kwargs):
        assert fn in self.actions
        action = partial(fn, **kwargs)
        th = threading.Thread(target=action, daemon=True)  # daemon = True?
        th.start()
        return _ProcessAdapter(th)

    def create_endpoint(self, *, uid: int):
        return uid


class _ProcessAdapter:
    def __init__(self, th: threading.Thread) -> None:
        self.th = th

    def wait(self):
        self.th.join()

    def terminate(self):
        # send to None?
        pass  # daemon=True, so ignored
