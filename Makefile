.PHONY: install install-dev test test-spark lint format typecheck coverage ci-local dbt-compile

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements.txt -r requirements-dev.txt -r requirements-dbt.txt

test:
	pytest -m "not spark" -q

test-spark:
	pytest -m spark -q

lint:
	ruff check src tests scripts

format:
	ruff format src tests scripts

typecheck:
	mypy src/retail_lakehouse

coverage:
	pytest --cov=retail_lakehouse --cov-report=term-missing -q

ci-local: lint typecheck test test-spark coverage

dbt-compile:
	cd dbt && dbt deps && dbt compile
