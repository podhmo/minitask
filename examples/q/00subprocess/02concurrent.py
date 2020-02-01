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


def producer(m: SubprocessWorkerManager, endpoint: str):
    with m.open_writer_queue(endpoint, force=True) as q:
        for i in range(20):
            q.put(i)
            time.sleep(0.01)
        q.put(None)


@as_command
def run():
    with SubprocessWorkerManager() as m:
        for i in range(3):
            endpoint = m.create_endpoint(str(i))
            m.spawn(consumer, endpoint=endpoint)
            m.spawn(producer, endpoint=endpoint)

        m.wait()
        print("ok")
