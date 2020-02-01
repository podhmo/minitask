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
def worker(*, endpoint: str, format_protocol: str, handler: str, transport: str):
    from magicalimport import import_symbol, import_module
    from minitask.q import Q, QueueLike

    handler = import_symbol(handler, cwd=True)
    format_protocol = import_symbol(format_protocol, cwd=True)
    transport = import_module(transport, cwd=True)
    with transport.create_reader_port(endpoint) as rf:
        q = Q(QueueLike(rf), format_protocol=format_protocol())
        handler(q)


as_subcommand.run()
