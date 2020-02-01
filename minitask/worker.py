import typing as t
import sys
import logging
from .q import Q, QueueLike, PickleFormat

logger = logging.getLogger(__name__)


class ThreadingExecutor:
    def __init__(self):
        self.threads = []

    def spawn(self, target, *, endpoint: str, format_protocol=None, transport=None):
        import threading

        if format_protocol is None:
            format_protocol = PickleFormat
        if transport is None:
            from minitask.transport import namedpipe

            transport = namedpipe

        def worker():

            with namedpipe.create_reader_port(endpoint) as rf:
                q = Q(QueueLike(rf), format_protocol=format_protocol())
                target(q)

        th = threading.Thread(target=worker)
        self.threads.append(th)
        th.start()
        return th

    def __len__(self):
        return len(self.threads)

    def wait(self):
        for th in self.threads:
            th.join()


class SubprocessExecutor:
    def __init__(self):
        self.processes = []

    def spawn(self, target, *, endpoint, format_protocol=None, transport=None):
        import sys
        import subprocess

        if format_protocol is not None:
            format_protocol = fullname(format_protocol)
        else:
            format_protocol = "minitask.q:PickleFormat"

        if transport is not None:
            transport = fullname(transport)
        else:
            transport = "minitask.transport.namedpipe"

        cmd = [
            sys.executable,
            "-m",
            "minitask.tool",
            "worker",
            "--endpoint",
            endpoint,
            "--format-protocol",
            format_protocol,
            "--handler",
            fullname(target),
            "--transport",
            transport,
        ]

        p = subprocess.Popen(cmd)
        self.processes.append(p)
        return p

    def __len__(self):
        return len(self.processes)

    def wait(self):
        for p in self.processes:
            p.wait()


def fullname(ob: t.Any) -> str:
    return f"{sys.modules[ob.__module__].__file__}:{ob.__name__}"
