default: 00 01

00:
	cat Makefile | python -m minitask.tool writer | tee $@writer.output
01:
	cat Makefile | python -m minitask.tool writer | python -m minitask.tool reader | tee $@reader.output
