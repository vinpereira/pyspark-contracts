.PHONY: format lint test check benchmark

format:
	uv run ruff format pyspark_contracts/ tests/ benchmarks/

lint:
	uv run ruff check pyspark_contracts/ tests/ benchmarks/
	uv run ruff check --fix pyspark_contracts/ tests/ benchmarks/

test:
	uv run pytest tests/ -v

check: format lint test

benchmark:
	uv run python benchmarks/benchmark_validate.py
