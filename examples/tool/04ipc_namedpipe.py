from __future__ import annotations
import time
from handofcats import as_subcommand
import minitask
from minitask.port import namedpipe as port
from minitask.environment.namedpipe import Environment

env = Environment()


@as_subcommand
def run():
    with env:
        pairs = []
        n = 2

        for uid in range(n):
            endpoint = env.create_endpoint(uid=uid)
            sp = env.spawn(producer, endpoint=endpoint)
            cp = env.spawn(consumer, endpoint=endpoint)
            pairs.append((sp, cp))

        # todo: fix
        for sp, cp in pairs:
            sp.wait()
            cp.wait()


@env.register
def producer(*, endpoint: str):
    import os

    ipc = minitask.IPC(port=port)
    pid = os.getpid()
    with ipc.serve(endpoint) as x:
        for i in range(5):
            # msgpackrpc not support kwargs
            x.write("say", args=["hello", [pid]])
            time.sleep(0.1)


@env.register
def consumer(*, endpoint: str):
    import os

    ipc = minitask.IPC(port=port)
    pid = os.getpid()
    with ipc.connect(endpoint) as x:
        while True:
            msg = x.read()
            if msg is None:
                break
            print("got", msg.unique_id, msg.method, msg.args, "with", pid)


as_subcommand.run()
