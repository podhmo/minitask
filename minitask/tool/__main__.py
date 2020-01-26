from handofcats import as_subcommand


@as_subcommand
def writer(*, name: str = "hello", fieldname: str = "line"):
    import sys
    from minitask.network.console import create_writer_port, write
    from tinyrpc.protocols.jsonrpc import JSONRPCProtocol

    # todo: jsonrpc, raw
    protocol = JSONRPCProtocol()

    port = create_writer_port()
    for line in sys.stdin:
        req = protocol.create_request(name, kwargs={fieldname: line})
        write(req.serialize(), port=port)


@as_subcommand
def reader():
    from minitask.network.console import create_reader_port, read

    port = create_reader_port()
    while True:
        msg = read(port=port)
        if not msg:
            break

        print(msg.decode("utf-8"))


as_subcommand.run()
