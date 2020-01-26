from handofcats import as_subcommand


@as_subcommand
def writer(*, name: str = "hello", fieldname: str = "line"):
    import sys
    from minitask.io import send
    from tinyrpc.protocols.jsonrpc import JSONRPCProtocol

    # todo: jsonrpc, raw
    protocol = JSONRPCProtocol()

    for line in sys.stdin:
        req = protocol.create_request(name, kwargs={fieldname: line})
        send(req.serialize(), port=sys.stdout.buffer)


@as_subcommand
def reader():
    import sys
    from minitask.io import recv

    while True:
        msg = recv(port=sys.stdin.buffer)
        if not msg:
            break

        sys.stdout.buffer.write(msg)
        sys.stdout.write("\n")
        sys.stdout.flush()


as_subcommand.run()
