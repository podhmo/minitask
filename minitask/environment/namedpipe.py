from __future__ import annotations
import typing as t
import itertools
import sys
import pathlib
import tempfile
import subprocess
import logging

from handofcats import as_subcommand

logger = logging.getLogger(__name__)


class Environment:
    def __init__(self, *, as_subcommand=as_subcommand):
        self.actions = {}
        self._tempdir = None
        self.dirpath = None

        # TODO: omit
        self._as_subcommand = as_subcommand
        self.run = self._as_subcommand.run

    def register(self, fn, name=None):
        self.actions[fn] = name or fn.__name__
        return self._as_subcommand(fn)

    def __enter__(self):
        self._tempdir = tempfile.TemporaryDirectory()
        self.dirpath = self._tempdir.__enter__()
        return self

    def __exit__(self, typ, val, tb):
        if self._tempdir is not None:
            return self._tempdir.__exit__(typ, val, tb)

    def spawn(
        self,
        fn: t.Callable[..., t.Any],
        filename: t.Optional[str] = None,
        _depth: int = 1,
        **kwargs,
    ):
        name = self.actions[fn]
        if filename is None:
            filename = sys._getframe(_depth).f_globals["__file__"]
        args = itertools.chain.from_iterable(
            [(f"--{k}", str(v)) for k, v in kwargs.items()]
        )
        cmd = [sys.executable, filename, name, *args]
        return subprocess.Popen(cmd)

    def create_endpoint(self, *, uid: int):
        return pathlib.Path(self.dirpath) / f"worker.{uid}.fifo"
