import time
from handofcats import as_command
from minitask.worker import SubprocessWorkerManager


def consumer(m: SubprocessWorkerManager, endpoint: str):
    import os
    from minitask.q import consume

    print(os.getpid(), "!")
    with m.open_reader_queue(endpoint) as q:
        for item in consume(q):
            print(os.getpid(), "<-", item)


@as_command
def run():
    with SubprocessWorkerManager() as m:
        endpoint = m.create_endpoint("x")
        m.spawn(consumer, endpoint=endpoint)

        with m.open_writer_queue(endpoint, force=True) as q:
            for i in range(20):
                q.put(i)
                time.sleep(0.01)
            q.put(None)
    print("ok")
