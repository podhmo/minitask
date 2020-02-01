import time
from handofcats import as_command
from minitask.q import (
    SubprocessExecutor,
    consume,
    Q,
    QueueLike,
    PickleFormat,
)
from minitask.transport.namedpipe import ContextStack


def consumer(q: Q):
    import os

    print(os.getpid(), "!")
    for item in consume(q):
        print(os.getpid(), "<-", item)


@as_command
def run():
    with ContextStack() as s:
        endpoint = s.create_endpoint("x")

        ex = SubprocessExecutor()
        ex.spawn(consumer, endpoint=endpoint)
        ex.spawn(consumer, endpoint=endpoint)
        ex.spawn(consumer, endpoint=endpoint)
        ex.spawn(consumer, endpoint=endpoint)
        ex.spawn(consumer, endpoint=endpoint)

        with s.serve(endpoint, force=True) as wf:
            q = Q(QueueLike(wf), format_protocol=PickleFormat())
            for i in range(20):
                q.put(i)
                time.sleep(0.01)

            for _ in range(len(ex)):
                q.put(None)

        ex.wait()
        print("ok")
