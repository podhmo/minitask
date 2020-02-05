from __future__ import annotations
import typing as t
import sys
import subprocess
import logging
from minitask import _options
from .types import WorkerCallable, WorkerManager
from ._name import fullfilename

logger = logging.getLogger(__name__)


def spawn_worker_process(
    manager: WorkerManager,
    handler: WorkerCallable,
    *,
    config: object,
    kwargs: t.Dict[str, t.Any]
) -> subprocess.Popen[bytes]:
    import inspect

    kwargs_ = inspect.getcallargs(handler, manager, **kwargs)
    for k, v in list(kwargs_.items()):
        if v == manager:
            kwargs_.pop(k)

    cmd = [
        sys.executable,
        "-m",
        "minitask.tool",
        "worker",
        "--kwargs",
        _options.dumps(kwargs_),
        "--manager",
        fullfilename(manager),
        "--handler",
        fullfilename(handler),
        "--config",
        fullfilename(config),
        "--config-options",
        _options.dumps(_options.extract(config, config.__class__)),
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
