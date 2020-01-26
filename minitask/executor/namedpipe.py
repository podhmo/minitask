from __future__ import annotations
import typing as t
import sys
import pathlib
import tempfile
import subprocess
import logging
from collections import namedtuple
from handofcats import as_subcommand

logger = logging.getLogger(__name__)

_Action = namedtuple("_Action", "name, where")


class _TemporaryDirectory(tempfile.TemporaryDirectory):
    def __enter__(self):
        logger.info("create tempdir %s", self.name)
        return super().__enter__()

    def __exit__(self, exc, value, tb):
        logger.info("remove tempdir %s", self.name)
        return super().__exit__(exc, value, tb)


class Executor:
    def __init__(self, *, as_subcommand=as_subcommand) -> None:
        self.actions: t.Dict[str, _Action] = {}

        self._tempdir: t.Optional[_TemporaryDirectory] = None
        self.dirpath: t.Optional[str] = None

        self._processess: t.List[subprocess.Process] = []

        # TODO: omit
        self._as_subcommand = as_subcommand
        self.run = self._as_subcommand.run

    def register(
        self, fn: t.Callable[..., t.Any], name: t.Optional[str] = None
    ) -> t.Callable[..., t.Any]:
        name = name or fn.__name__
        where = sys.modules[fn.__module__].__file__
        self.actions[fn] = _Action(name=name, where=where)
        return self._as_subcommand(fn)

    def __enter__(self) -> Executor:
        self._tempdir = _TemporaryDirectory()
        self.dirpath = self._tempdir.__enter__()
        return self

    def __exit__(self, typ, val, tb) -> None:
        if self._tempdir is not None:
            return self._tempdir.__exit__(typ, val, tb)

    def spawn(
        self,
        fn: t.Callable[..., t.Any],
        filename: t.Optional[str] = None,
        _depth: int = 1,
        **kwargs: t.Any,
    ) -> subprocess.Process:
        action = self.actions[fn]
        action_name = action.name

        filename = filename or action.where
        if filename is None:
            filename = sys._getframe(_depth).f_globals["__file__"]

        # TODO: fix gentle flag name conversion
        args = []
        for k, v in kwargs.items():
            if v is True:
                args.append(f"--{k}")
            else:
                args.append(f"--{k}")
                args.append(str(v))  # support: str,int,float

        cmd = [sys.executable, filename, action_name, *args]
        p = subprocess.Popen(cmd)
        logger.info("spawn pid=%d, %s", p.pid, " ".join(cmd))
        self._processess.append(p)
        return p

    def create_endpoint(self, *, uid: int) -> str:
        return pathlib.Path(self.dirpath) / f"worker.{uid}.fifo"

    def wait(self, *, check: bool = True) -> None:
        for p in self._processess:
            p.wait()
            if check:
                cp = subprocess.CompletedProcess(
                    p.args, p.returncode, stdout=p.stdout, stderr=p.stderr
                )
                cp.check_returncode()
