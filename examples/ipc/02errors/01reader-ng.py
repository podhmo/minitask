from handofcats import as_subcommand
from commands import executor
from commands import producer, consumer


@as_subcommand
def run():
    with executor:
        n = 2

        for uid in range(n):
            endpoint = executor.create_endpoint(uid=uid)
            executor.spawn(producer, endpoint=endpoint)
            executor.spawn(consumer, endpoint=endpoint, ng=True)

        executor.wait()


as_subcommand.run()
