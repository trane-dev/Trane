.PHONY: clean
clean:
	find . -name '*.pyo' -delete
	find . -name '*.pyc' -delete
	find . -name __pycache__ -delete
	find . -name '*~' -delete
	find . -name '.coverage.*' -delete

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

.PHONY: test
test:
	python -m pytest -n auto -s -vv -x tests/ --sample 100

.PHONY: unit-tests
unit-tests:
	python -m pytest -n auto -s -vv -x tests/ --ignore=tests/integration_tests --cov=trane/ --cov-report term-missing

.PHONY: integration-tests
integration-tests:
	python -m pytest -n auto -s -vv -x tests/integration_tests --sample 100

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