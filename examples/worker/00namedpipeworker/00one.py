import time
from handofcats import as_command
from minitask.worker.namedpipeworker import Manager


def consumer(m: Manager, uid: str):
    import os

    print(os.getpid(), "!")
    with m.open_reader_queue(uid) as q:
        for item in q:
            print(os.getpid(), "<-", item)


@as_command
def run():
    with Manager() as m:
        uid = m.generate_uid("x")
        m.spawn(consumer, uid=uid)

        with m.open_writer_queue(uid, force=True) as q:
            for i in range(20):
                q.put(i)
                time.sleep(0.01)

    print("ok")
