default: 00 01 02

00:
	cat Makefile | python -m minitask.tool writer | tee $@writer.output

01:
	cat Makefile | python -m minitask.tool writer | python -m minitask.tool reader | tee $@reader.output

02:
	python $(shell echo $@*.py) | tee $(patsubst %.py,%.output,$(shell echo $@*.py))

