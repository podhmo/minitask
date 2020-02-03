build: clean
	python setup.py build_ext --inplace

install:
	pip install -e .

check:
	PYTHONPATH=. python -c 'import _hibernation; print(_hibernation.sleep("hello", 1))'

# run:
# 	PYTHONPATH=. python 00use_thread_pool_executor.py
# 	PYTHONPATH=. python 01use_process_pool_executor.py

clean:
	pip uninstall _hibernation
	rm -f *.so *.c
