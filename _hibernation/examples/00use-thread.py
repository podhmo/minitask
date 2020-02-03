from concurrent.futures import ThreadPoolExecutor
from _hibernation import sleep


def main():
    with ThreadPoolExecutor(max_workers=3) as ex:
        ex.submit(sleep, "x", 1)
        ex.submit(sleep, "y", 2)
        ex.submit(sleep, "z", 1)


if __name__ == "__main__":
    main()
