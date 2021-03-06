from __future__ import annotations
import typing as t
from collections import defaultdict
import queue
import threading
import logging


logger = logging.getLogger(__name__)


mapping: t.Dict[t.Optional[str], _IOAdapter] = {}
sems: t.Dict[t.Optional[str], threading.Event] = defaultdict(threading.Event)


def create_writer_port(endpoint: t.Optional[str] = None) -> _IOAdapter:
    port = _IOAdapter(queue.Queue())
    mapping[endpoint] = port
    sems[endpoint].set()
    return port


def create_reader_port(endpoint: t.Optional[str] = None) -> _IOAdapter:
    sems[endpoint].wait()
    return mapping[endpoint]


def write(body: bytes, *, file: _IOAdapter) -> None:
    size = len(body)
    logger.debug("write	size:%d	body:%r", size, body)
    file.q.put(body)


def read(*, file: _IOAdapter) -> bytes:
    body = file.q.get()
    if body is None:
        return b""
    size = len(body)
    logger.debug("read	size:%s	body:%r", size, body)
    return body


class _IOAdapter:
    def __init__(self, q: queue.Queue[t.Optional[t.Optional[bytes]]]) -> None:
        self.q = q

    def close(self) -> None:
        self.q.put_nowait(None)  # auto finalize?


# use inmemory buffer?
def create_reader_buffer(
    recv: t.Callable[[], t.Any]
) -> t.Tuple[t.Iterable[t.Any], t.Optional[t.Callable[[], None]]]:
    def iterate() -> t.Iterable[t.Any]:
        while True:
            item = recv()
            if item is None:
                break
            yield item

    return iterate(), None
