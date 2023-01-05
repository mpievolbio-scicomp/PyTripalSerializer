sources = tripser

.PHONY: test format lint unittest coverage pre-commit clean docs
test: format lint unittest

format:
	isort $(sources) tests
	black $(sources) tests

lint:
	flake8 $(sources) tests

unittest:
	pytest

coverage:
	pytest --cov $(sources) --cov-branch --cov-report term-missing tests

pre-commit:
	pre-commit run --all-files

docs:
	cd docs && make html

clean:
	rm -rf .pytest_cache
	rm -rf *.egg-info
	rm -rf .tox dist site
	rm -rf coverage.xml .coverage
