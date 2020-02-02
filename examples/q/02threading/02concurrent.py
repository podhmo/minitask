import time
from handofcats import as_command
from minitask.worker import ThreadingWorkerManager


def consumer(m: Manager, uid: str):
    import os
    from minitask.q import consume

    print(os.getpid(), "!")
    with m.open_reader_queue(uid) as q:
        for item in consume(q):
            print(os.getpid(), "<-", item)


def producer(m: Manager, uid: str):
    with m.open_writer_queue(uid, force=True) as q:
        for i in range(20):
            q.put(i)
            time.sleep(0.01)
        q.put(None)


@as_command
def run():
    with Manager() as m:
        for i in range(3):
            uid = m.generate_uid(str(i))
            m.spawn(consumer, uid=uid)
            m.spawn(producer, uid=uid)
    print("ok")
