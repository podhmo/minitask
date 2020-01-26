import typing as t
import logging

logger = logging.getLogger(__name__)
# TODO: movepkg


def send(body: bytes, *, port: t.IO[bytes]):
    size = len(body)

    port.write(str(size).encode("utf-8"))  # todo: encoding
    port.write(b"\n")
    port.write(body)
    logger.debug("send	size:%d	body:%r", size, body)
    port.flush()


def recv(*, port: t.IO[bytes]) -> bytes:
    size = port.readline()
    if not size:
        return ""
    body = port.read(int(size))
    logger.debug("recv	size:%s	body:%r", size, body)
    return body
