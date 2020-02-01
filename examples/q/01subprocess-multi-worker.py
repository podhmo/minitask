import pathlib
import time
from handofcats import as_command
from minitask.q import (
    SubprocessExecutor,
    open_port,
    consume,
    Q,
    QueueLike,
    PickleFormat,
)


def consumer(q: Q):
    import os

    print(os.getpid(), "!")
    for item in consume(q):
        print(os.getpid(), "<-", item)


@as_command
def run():
    endpoint = str((pathlib.Path(__file__).parent / "x.fifo").absolute())
    ex = SubprocessExecutor()

    ex.spawn(consumer, endpoint=endpoint)
    ex.spawn(consumer, endpoint=endpoint)
    ex.spawn(consumer, endpoint=endpoint)
    ex.spawn(consumer, endpoint=endpoint)

    with open_port(endpoint, "w") as wf:
        q = Q(QueueLike(wf), format_protocol=PickleFormat())
        for i in range(20):
            q.put(i)
            time.sleep(0.01)

        for _ in range(len(ex)):
            q.put(None)
    ex.wait()
    print("ok")
