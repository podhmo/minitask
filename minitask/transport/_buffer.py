from __future__ import annotations
import typing as t
import queue
import pathlib
import pickle
import threading
import logging

logger = logging.getLogger(__name__)

# TODO: dedup?


class InmemoryQueueBuffer:
    def __init__(
        self,
        recv: t.Callable[..., t.Any],  # todo: typing
        q: t.Optional[queue.Queue[t.Optional[bytes]]] = None,
        *,
        name: str = "queue.pickle",
    ):
        self.recv = recv
        self.q: queue.Queue[t.Optional[bytes]] = queue.Queue()
        self.path = pathlib.Path(name)
        self._th: t.Optional[threading.Thread] = None

    def load(
        self,
        q: t.Optional[queue.Queue[t.Optional[bytes]]] = None,
        *,
        path: t.Optional[pathlib.Path] = None,
    ) -> None:
        path = path or self.path
        q = q or self.q
        if path.exists():
            logger.info("load queue: %s", path)
            items = pickle.load(path.open("rb"))
            for item in items:
                if item is not None:
                    q.put_nowait(item)

    def save(
        self,
        q: t.Optional[queue.Queue[t.Optional[bytes]]] = None,
        *,
        path: t.Optional[pathlib.Path] = None,
    ) -> None:
        path = path or self.path
        q = q or self.q
        # todo: multiprocess safe
        if q.queue:
            if path.exists():
                path.unlink()
            logger.info("save queue: %s", path)
            pickle.dump(q.queue, path.open("wb"))

    def __iter__(self) -> t.Iterable[t.Optional[bytes]]:
        q = self.q
        path = self.path

        def _peek() ->None:
            while True:
                item = self.recv()
                if item is None:
                    q.put(None)  # finish
                    break
                q.put(item)

        th = threading.Thread(target=_peek)
        th.start()

        while True:
            item = q.get()
            if item is None:
                q.task_done()
                break
            yield item
            q.task_done()
        q.join()
        if path.exists():
            path.unlink()
