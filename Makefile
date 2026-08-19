.PHONY: format lint test check

format:
	uv run ruff format pyspark_contracts/ tests/

lint:
	uv run ruff check pyspark_contracts/ tests/
	uv run ruff check --fix pyspark_contracts/ tests/

test:
	uv run pytest tests/ -v

check: format lint test
