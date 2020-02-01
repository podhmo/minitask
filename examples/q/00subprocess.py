import time
from handofcats import as_command
from minitask.q import (
    SubprocessExecutor,
    consume,
    Q,
    QueueLike,
    PickleFormat,
)
from minitask.communication import namedpipe


def consumer(q: Q):
    import os

    print(os.getpid(), "!")
    for item in consume(q):
        print(os.getpid(), "<-", item)


@as_command
def run():
    endpoint = namedpipe.create_endpoint("x")
    ex = SubprocessExecutor()

    ex.spawn(consumer, endpoint=endpoint)
    with namedpipe.create_writer_port(endpoint, force=True) as wf:
        q = Q(QueueLike(wf), format_protocol=PickleFormat())
        for i in range(20):
            q.put(i)
            time.sleep(0.01)

        q.put(None)

    ex.wait()
    print("ok")
