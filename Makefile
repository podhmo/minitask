test:
	pytest -vv --show-capture=all

ci:
	pytest --show-capture=all --cov=minitask --no-cov-on-fail --cov-report term-missing
	$(MAKE) lint typing

format:
#	pip install -e .[dev]
	black minitask setup.py

# https://www.flake8rules.com/rules/W503.html
# https://www.flake8rules.com/rules/E203.html
# https://www.flake8rules.com/rules/E501.html
lint:
#	pip install -e .[dev]
	flake8 minitask --ignore W503,E203,E501

typing:
#	pip install -e .[dev]
	mypy --strict --strict-equality --ignore-missing-imports minitask
mypy: typing

build:
#	pip install wheel
	python setup.py bdist_wheel

upload:
#	pip install twine
	twine check dist/minitask-$(shell cat VERSION)*
	twine upload dist/minitask-$(shell cat VERSION)*

.PHONY: test ci format lint typing mypy build upload
