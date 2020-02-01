import time
from handofcats import as_command
from minitask.q import (
    consume,
    Q,
    QueueLike,
    PickleFormat,
)
from minitask.worker import SubprocessExecutor  # TODO: rename
from minitask.transport.namedpipe import ContextStack


def consumer(q: Q):
    import os

    print(os.getpid(), "!")
    for item in consume(q):
        print(os.getpid(), "<-", item)


@as_command
def run():
    with ContextStack() as s:
        ex = SubprocessExecutor()

        for i in range(2):
            endpoint = s.create_endpoint(str(i))
            ex.spawn(consumer, endpoint=endpoint)

            wf = s.enter_context(s.serve(endpoint, force=True))
            import threading

            def provide():
                q = Q(QueueLike(wf), format_protocol=PickleFormat())
                for i in range(20):
                    q.put(i)
                    time.sleep(0.01)
                q.put(None)

            threading.Thread(target=provide).start()
        ex.wait()
        print("ok")
