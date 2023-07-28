.PHONY: check reformat test regression

check: checkformat test regression

checkformat:
	python -m black --check . || (echo "Please run 'make reformat' to fix formatting issues" && exit 1)

reformat:
	python -m black .

# unit tests; ignore tests/regression
test:
	python -m pytest test/ --ignore test/regression/

regression:
	python -m pytest test/regression
	cd test/regression && bash run.sh
