import itertools


class IDGenerator:
    def __init__(self) -> None:
        self.c = itertools.count()

    def __call__(self) -> str:
        return f"G{next(self.c)}"
