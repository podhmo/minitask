import typing as t
from handofcats import as_subcommand


@as_subcommand  # type:ignore
def writer(*, name: str = "hello", fieldname: str = "line") -> None:
    import sys
    from minitask.transport.console import create_writer_port, write

    port = create_writer_port()
    for line in sys.stdin:
        b = line.rstrip("\n").encode("utf-8")
        write(b, file=port)


@as_subcommand  # type:ignore
def reader() -> None:
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
    manager: str,
    handler: str,
    kwargs: t.Dict[str, t.Any],
    config: str,
    config_options: t.Union[str, t.Dict[str, t.Any], None] = None
) -> None:
    from magicalimport import import_symbol
    from minitask import _options

    kwargs = _options.loads(kwargs)
    options = _options.loads(config_options)
    handler_callable = import_symbol(handler, cwd=True)
    config_object = import_symbol(config, cwd=True)(**options)
    manager_object = import_symbol(manager, cwd=True)(config_object)
    handler_callable(manager_object, **kwargs)


as_subcommand.run()
