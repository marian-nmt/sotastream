.PHONY: all clean run setup
.SECONDARY:

run:
	bash ./run.sh

setup:
	bash ./setup.sh

all: setup run

clean:
	git clean -x -d -f tests
