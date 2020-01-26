from handofcats import as_subcommand


@as_subcommand
def reader():
    import sys
    from minitask.io import send

    for line in sys.stdin:
        send(line.encode("utf-8"), port=sys.stdout.buffer)


as_subcommand.run()
