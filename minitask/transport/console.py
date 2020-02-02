import typing as t
import sys
from ._base import read, write  # noqa 401


def create_reader_port(filename: t.Optional[str] = None) -> t.IO[bytes]:
    if filename is None:
        return sys.stdin.buffer
    return open(filename, "rb")


def create_writer_port(filename: t.Optional[str] = None) -> t.IO[bytes]:
    if filename is None:
        return sys.stdout.buffer
    return open(filename, "wb")


def create_reader_buffer(
    recv: t.Callable[[], t.Any]
) -> t.Tuple[t.Iterable[t.Any], t.Optional[t.Callable[[], None]]]:
    def iterate():
        while True:
            item = recv()
            if item is None:
                break
            yield item

    return iterate(), None
