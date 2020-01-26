import time
from handofcats import as_subcommand
import minitask
from minitask.port import namedpipe as port
from minitask.executor.namedpipe import Executor

executor = Executor()


@as_subcommand
def run():
    with executor:
        ps = []
        n = 2

        for uid in range(n):
            endpoint = executor.create_endpoint(uid=uid)
            pp = executor.spawn(producer, endpoint=endpoint)
            ps.append(pp)
            cp = executor.spawn(consumer, endpoint=endpoint)
            ps.append(cp)

        for p in ps:
            p.wait()


@executor.register
def producer(*, endpoint: str):
    import os

    ipc = minitask.IPC(port=port)
    pid = os.getpid()
    with ipc.serve(endpoint) as x:
        for i in range(5):
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
