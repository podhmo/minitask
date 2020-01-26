import typing as t
import logging

logger = logging.getLogger(__name__)


def write(body: bytes, *, file: t.IO[bytes], encoding="utf-8") -> None:
    size = len(body)

    file.write(str(size).encode(encoding))
    file.write(b"\n")
    file.write(body)
    logger.debug("write	size:%d	body:%r", size, body)
    file.flush()


def read(*, file: t.IO[bytes]) -> bytes:
    size = file.readline()
    if not size:
        return ""
    body = file.read(int(size))
    logger.debug("read	size:%s	body:%r", size, body)
    return body
