import typing as t
import logging

logger = logging.getLogger(__name__)


def write(body: bytes, *, port: t.IO[bytes], encoding="utf-8"):
    size = len(body)

    port.write(str(size).encode(encoding))
    port.write(b"\n")
    port.write(body)
    logger.debug("write	size:%d	body:%r", size, body)
    port.flush()


def read(*, port: t.IO[bytes]) -> bytes:
    size = port.readline()
    if not size:
        return ""
    body = port.read(int(size))
    logger.debug("read	size:%s	body:%r", size, body)
    return body
