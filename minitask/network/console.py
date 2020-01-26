import typing as t
import sys
from .base import read, write  # noqa 401


def create_reader_port() -> t.IO[bytes]:
    return sys.stdin.buffer


def create_writer_port() -> t.IO[bytes]:
    return sys.stdout.buffer
