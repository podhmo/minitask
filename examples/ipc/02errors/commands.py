import time
from handofcats import as_subcommand
import minitask
from minitask.port import fake as port
from minitask.executor.threaded import Executor

# from minitask.port import namedpipe as port
# from minitask.executor.namedpipe import Executor


executor = Executor()


@executor.register
def producer(*, endpoint: str, ng: bool = False):
    import os

    ipc = minitask.IPC(port=port)
    pid = os.getpid()
    with ipc.serve(endpoint) as x:
        for i in range(5):
            # msgpackrpc not support kwargs
            x.write("say", args=["hello", [pid]])
            time.sleep(0.1)
            if ng:
                1 / 0


@executor.register
def consumer(*, endpoint: str, ng: bool = False):
    import os

    ipc = minitask.IPC(port=port)
    pid = os.getpid()
    with ipc.connect(endpoint) as x:
        for msg in x:
            print("got", msg.unique_id, msg.method, msg.args, "with", pid)
            if ng:
                1 / 0


if __name__ == "__main__":
    as_subcommand.run()
