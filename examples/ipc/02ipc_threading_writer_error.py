import time
from handofcats import as_subcommand
import minitask
from minitask.port import fake as port
from minitask.executor.threaded import Executor

executor = Executor()


@as_subcommand
def run():
    with executor:
        n = 2

        for uid in range(n):
            endpoint = executor.create_endpoint(uid=uid)
            executor.spawn(producer, endpoint=endpoint)
            executor.spawn(consumer, endpoint=endpoint)

        executor.wait()


@executor.register
def producer(*, endpoint: str):
    import os

    ipc = minitask.IPC(port=port)
    pid = os.getpid()
    with ipc.serve(endpoint) as x:
        for i in range(5):
            if i == 2:
                1 / 0
            # msgpackrpc not support kwargs
            x.write("say", args=["hello", [pid]])
            time.sleep(0.1)


@executor.register
def consumer(*, endpoint: str):
    import os

    ipc = minitask.IPC(port=port)
    pid = os.getpid()
    with ipc.connect(endpoint) as x:
        for msg in x:
            print("got", msg.unique_id, msg.method, msg.args, "with", pid)


as_subcommand.run()
