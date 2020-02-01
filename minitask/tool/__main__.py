import typing as t
from handofcats import as_subcommand


@as_subcommand
def writer(*, name: str = "hello", fieldname: str = "line"):
    import sys
    from minitask.transport.console import create_writer_port, write
    from tinyrpc.protocols.jsonrpc import JSONRPCProtocol

    # todo: jsonrpc, raw
    protocol = JSONRPCProtocol()

    port = create_writer_port()
    for line in sys.stdin:
        req = protocol.create_request(name, kwargs={fieldname: line})
        write(req.serialize(), file=port)


@as_subcommand
def reader():
    from minitask.transport.console import create_reader_port, read

    port = create_reader_port()
    while True:
        msg = read(file=port)
        if not msg:
            break

        print(msg.decode("utf-8"))


@as_subcommand
def worker(
    *, endpoint: str, manager: str, handler: str, dirpath: t.Optional[str] = None
):
    from magicalimport import import_symbol

    handler = import_symbol(handler, cwd=True)
    manager = import_symbol(manager, cwd=True)(dirpath=dirpath)
    handler(manager, endpoint)


as_subcommand.run()
