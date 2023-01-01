.PHONY: clean
clean:
	find . -name '*.pyo' -delete
	find . -name '*.pyc' -delete
	find . -name __pycache__ -delete
	find . -name '*~' -delete
	find . -name '.coverage.*' -delete

.PHONY: lint
lint:
	black trane/ tests/ -t py311 --check
	ruff trane/ tests/

.PHONY: lint-fix
lint-fix:
	black trane/ tests/ -t py311
	ruff trane/ tests/ --fix

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

.PHONY: tests
tests:
	python -m pytest -n auto -s -vv -x tests/ --sample 100 --cov=trane/ --cov-report term-missing

.PHONY: unit-tests
unit-tests:
	python -m pytest -n auto -s -vv -x tests/ --ignore=tests/integration_tests --cov=trane/ --cov-report term-missing

.PHONY: integration-tests
integration-tests:
	python -m pytest -n auto -s -vv -x tests/integration_tests --sample 100 --cov=trane/ --cov-report term-missing

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
	$(eval PACKAGE=$(shell python -c "from pep517.meta import load; metadata = load('.'); print(metadata.version)"))
	tar -zxvf "dist/trane-${PACKAGE}.tar.gz"
	mv "trane-${PACKAGE}" unpacked_sdist

.PHONY: checkdeps
checkdeps:
	$(eval allow_list='numpy|pandas|dill|scikit|dateutil|scipy|py|tornado|composeml|featuretools|matplotlib|tqdm')
	pip freeze | grep -v "Trane.git" | grep -E $(allow_list) > $(OUTPUT_FILEPATH)
