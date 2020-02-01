import time
from handofcats import as_subcommand
import minitask

from minitask.communication import namedpipe as communication
from minitask.executor.namedpipe import Executor


executor = Executor()


@executor.register
def producer(*, endpoint: str, ng: bool = False):
    import os

    ipc = minitask.IPC(communication=communication)
    pid = os.getpid()
    with ipc.serve(endpoint, sensitive=False) as x:
        for i in range(5):
            x.send({"method": "say", "message": "hello", "pid": pid})
            time.sleep(0.1)
            if ng and i == 2:
                1 / 0


@executor.register
def consumer(*, endpoint: str, ng: bool = False):
    import os
    from tinyrpc.protocols import RPCErrorResponse

    ipc = minitask.IPC(communication=communication)
    pid = os.getpid()
    with ipc.connect(endpoint, sensitive=False) as x:
        for i, msg in enumerate(x):
            if isinstance(msg, RPCErrorResponse):
                print("got error", msg, "with", pid)
            else:
                print("got", msg, "with", pid)
            if ng and i == 2:
                1 / 0


if __name__ == "__main__":
    as_subcommand.run()
