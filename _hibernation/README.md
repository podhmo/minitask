# _hibernation

:warning: don't use this package. this package is for testing.

## 00

_hibernation.sleep with ThreadPoolExecutor

```console
$ make -C examples 00
time python 00use-thread.py
	... x start sleep 1
	... x end sleep
	... y start sleep 2
	... z start sleep 1
	... y end sleep
	... z end sleep
        4.14 real         0.07 user         0.06 sys
```

## 01

_hibernation.sleep with ProcessPoolExecutor

```console
$ make -C examples 01
time python 01use-process.py
	... x start sleep 1
	... y start sleep 2
	... z start sleep 1
	... x end sleep
	... z end sleep
	... y end sleep
        2.41 real         0.52 user         0.13 sys
```

## 02

_hibernation.correct_sleep with ThreadPoolExecutor

(correct_sleep is sleep with `nogil()` decorator)

```console
$ make -C examples 02
time python 02correct-sleep.py
	... x start sleep 1
	... y start sleep 2
	... z start sleep 1
	... x end sleep
	... z end sleep
	... y end sleep
        2.13 real         0.06 user         0.02 sys
```
