from __future__ import annotations
import typing as t
from collections import defaultdict
import queue
import threading
import logging


logger = logging.getLogger(__name__)


mapping = {}
sems = defaultdict(threading.Event)


def create_writer_port(endpoint: t.Optional[str] = None) -> _IOAdapter:
    port = _IOAdapter(queue.Queue())
    mapping[endpoint] = port
    sems[endpoint].set()
    return port


def create_reader_port(endpoint: t.Optional[str] = None) -> _IOAdapter:
    sems[endpoint].wait()
    return mapping[endpoint]


def write(body: bytes, *, port: _IOAdapter):
    size = len(body)
    logger.debug("send	size:%d	body:%r", size, body)
    port.q.put(body)


def read(*, port: _IOAdapter) -> bytes:
    body = port.q.get()
    if body is None:
        return ""
    size = len(body)
    logger.debug("recv	size:%s	body:%r", size, body)
    return body


class _IOAdapter:
    def __init__(self, q: queue.Queue):
        self.q = q

    def close(self):
        self.q.put_nowait(None)  # auto finalize?
