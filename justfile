set shell := ["bash", "-eu", "-c"]

default: help

help:
	@echo "Available recipes:"
	@echo "  init          Install project + dev deps via uv"
	@echo "  fmt           Format code with ruff"
	@echo "  lint          Run ruff checks"
	@echo "  typecheck     Run mypy"
	@echo "  test          Run pytest"
	@echo "  clean         Remove build + cache artifacts"
	@echo "  build         Clean and build sdist+wheel"
	@echo "  publish-test  Upload to TestPyPI (builds + twine check first)"
	@echo "  publish       Upload to PyPI (builds + twine check first)"

init:
	uv sync --group dev

fmt:
	uv run --group dev ruff format src examples tests

lint:
	uv run --group dev ruff check src examples

typecheck:
	uv run --group dev mypy src examples

test:
	uv run --group dev pytest

clean:
	rm -rf dist build .mypy_cache .ruff_cache .pytest_cache *.egg-info
	rm -rf data/
	rm -rf **/**/__pycache__
	rm -rf **/__pycache__

build: clean
	uv run python -m build

publish-test: build
	uv run twine check dist/*
	uv run twine upload --repository testpypi dist/*

publish: build
	uv run twine check dist/*
	uv run twine upload dist/*

profile:
	py-spy record -o profile.svg --subprocesses -- python examples/url_titles_spider.py --urls-file ./data/lobsters.jl --output data/url_titles.jl
