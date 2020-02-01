import typing as t
import sys
from ._base import read, write  # noqa 401


def create_reader_port() -> t.IO[bytes]:
    return sys.stdin.buffer


def create_writer_port() -> t.IO[bytes]:
    return sys.stdout.buffer


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
