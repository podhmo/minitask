import time
from handofcats import as_subcommand
import minitask
from minitask.communication import namedpipe as communication
from minitask.executor.namedpipe import Executor

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

    ipc = minitask.IPC(communication=communication)
    pid = os.getpid()
    with ipc.serve(endpoint) as x:
        for i in range(5):
            x.send({"method": "say", "message": "hello", "pid": pid})
            time.sleep(0.1)


@executor.register
def consumer(*, endpoint: str):
    import os

    ipc = minitask.IPC(communication=communication)
    pid = os.getpid()
    with ipc.connect(endpoint) as x:
        for msg in x:
            print("got", msg, "with", pid)


as_subcommand.run()
