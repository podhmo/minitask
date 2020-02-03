from posix.unistd cimport sleep as _sleep

def sleep(x, unsigned int n, bint verbose=True):
    if verbose:
        print(f"	... {x} start sleep {n}")
    # with nogil:
    _sleep(n)
    if verbose:
        print(f"	... {x} end sleep")
    return x

def correct_sleep(x, unsigned int n, bint verbose=True):
    if verbose:
        print(f"	... {x} start sleep {n}")
    with nogil:
        _sleep(n)
    if verbose:
        print(f"	... {x} end sleep")
    return x