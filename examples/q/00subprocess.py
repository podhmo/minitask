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


def worker(q: Q):
    for item in consume(q):
        print("<-", item)


@as_command
def run():
    endpoint = str((pathlib.Path(__file__).parent / "x.fifo").absolute())
    ex = SubprocessExecutor()
    ex.spawn(worker, endpoint=endpoint)

    with open_port(endpoint, "w") as wf:
        q = Q(QueueLike(wf), format_protocol=PickleFormat())
        for i in range(5):
            q.put(i)
        time.sleep(0.05)
        q.put(None)
    ex.wait()
    print("ok")
