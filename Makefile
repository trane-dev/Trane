.PHONY: clean
clean:
	find . -name '*.pyo' -delete
	find . -name '*.pyc' -delete
	find . -name __pycache__ -delete
	find . -name '*~' -delete
	find . -name '.coverage.*' -delete

LINT_CONFIG = trane/ tests/ --config=./pyproject.toml
.PHONY: lint
lint:
	ruff check $(LINT_CONFIG)
	ruff format --check $(LINT_CONFIG)

.PHONY: lint-fix
lint-fix:
	ruff check --fix $(LINT_CONFIG)
	ruff format $(LINT_CONFIG)

.PHONY: installdeps-dev
installdeps-dev:
	python -m pip install ".[dev]"
	pre-commit install

.PHONY: installdeps-test
installdeps-test:
	python -m pip install ".[test]"

.PHONY: installdeps-docs
installdeps-docs:
	python -m pip install ".[docs]"

PYTEST = python -m pytest -n auto -s -vv -x
COVERAGE = --cov=trane/ --cov-report term-missing --cov-config=./pyproject.toml --cov-report=xml:./coverage.xml

.PHONY: tests
tests:
	$(PYTEST) tests/

.PHONY: unit-tests
unit-tests:
	$(PYTEST) tests/ $(COVERAGE)

.PHONY: upgradepip
upgradepip:
	python -m pip install --upgrade pip

.PHONY: upgradebuild
upgradebuild:
	python -m pip install --upgrade build

.PHONY: upgradesetuptools
upgradesetuptools:
	python -m pip install --upgrade setuptools

.PHONY: package
package: upgradepip upgradebuild upgradesetuptools
	python -m build
	$(eval PACKAGE=$(shell python -c 'import setuptools; setuptools.setup()' --version))
	tar -zxvf "dist/trane-${PACKAGE}.tar.gz"
	mv "trane-${PACKAGE}" unpacked_sdist

.PHONY: checkdeps
checkdeps:
	$(eval allow_list='numpy|pandas|scipy|tqdm')
	pip freeze | grep -v "Trane.git" | grep -E $(allow_list) > $(OUTPUT_FILEPATH)
