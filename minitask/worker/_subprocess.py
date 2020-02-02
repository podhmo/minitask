from __future__ import annotations
import typing as t
import typing_extensions as tx
import sys
import subprocess
import logging
import contextlib
from minitask.langhelpers import reify
from minitask import _options
from .types import WorkerCallable, WorkerManager
from ._name import fullfilename

logger = logging.getLogger(__name__)


def spawn_worker_process(
    manager: WorkerManager,
    handler: WorkerCallable,
    *,
    uid: str,
    option_type: t.Type[t.Any]
) -> subprocess.Popen[bytes]:
    cmd = [
        sys.executable,
        "-m",
        "minitask.tool",
        "worker",
        "--uid",
        uid,
        "--manager",
        fullfilename(manager),
        "--handler",
        fullfilename(handler),
        "--options",
        _options.dumps(_options.extract(manager, option_type)),
    ]
    logger.info("spawn cmd=%s", ", ".join(map(str, cmd)))
    p = subprocess.Popen(cmd)
    return p


def wait_processes(
    processes: t.List[subprocess.Popen[bytes]], *, check: bool = True
) -> None:
    for p in processes:
        try:
            p.wait()
            if check:
                cp = subprocess.CompletedProcess(
                    p.args, p.returncode, stdout=p.stdout, stderr=p.stderr
                )
                cp.check_returncode()
        except KeyboardInterrupt:
            logger.info("keybord interrupted, pid=%d", p.pid)


class BaseManager(contextlib.ExitStack):
    class OptionDict(tx.TypedDict):
        pass

    @classmethod
    def from_dict(cls, kwargs: t.Dict[str, t.Any]) -> t.Any:  # todo: subtyping

        return cls(**kwargs)

    def __init__(self, *, _: t.Any) -> None:
        pass

    @reify
    def processes(self) -> t.List[subprocess.Popen[bytes]]:
        return []

    def spawn(self, target: WorkerCallable, *, uid: str) -> subprocess.Popen[bytes]:
        p = spawn_worker_process(
            self, target, uid=uid, option_type=self.__class__.OptionDict
        )
        self.processes.append(p)
        return p

    def __len__(self) -> int:
        return len(self.processes)

    def wait(self, *, check: bool = True) -> None:
        wait_processes(self.processes)

    def __enter__(self) -> WorkerManager:
        impl: WorkerManager = self
        return impl

    def __exit__(
        self,
        exc: t.Optional[t.Type[BaseException]],
        value: t.Optional[BaseException],
        tb: t.Any,
    ) -> tx.Literal[False]:
        self.wait()
        return False
