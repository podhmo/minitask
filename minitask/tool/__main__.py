import typing as t
from handofcats import as_subcommand


@as_subcommand
def writer(*, name: str = "hello", fieldname: str = "line"):
    import sys
    from minitask.transport.console import create_writer_port, write

    port = create_writer_port()
    for line in sys.stdin:
        b = line.rstrip("\n").encode("utf-8")
        write(b, file=port)


@as_subcommand
def reader():
    from minitask.transport.console import create_reader_port, read

    port = create_reader_port()
    while True:
        msg = read(file=port)
        if not msg:
            break

        print(msg.decode("utf-8"))


@as_subcommand  # type: ignore
def worker(
    *,
    uid: str,
    manager: str,
    handler: str,
    config: str,
    options: t.Union[str, t.Dict[str, t.Any], None] = None
) -> None:
    from magicalimport import import_symbol
    from minitask import _options

    options = _options.loads(options)
    handler = import_symbol(handler, cwd=True)
    config = import_symbol(config, cwd=True)(**options)
    manager = import_symbol(manager, cwd=True)(config)
    handler(manager, uid)


as_subcommand.run()
