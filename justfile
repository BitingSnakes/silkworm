set shell := ["bash", "-eu", "-c"]

default: help

help:
	just -l

init: clean
	uv venv --python python3.13
	uv sync --group dev --upgrade

fmt:
	uv run --group dev ruff format src examples tests

lint:
	uv run --group dev ruff check src examples

typecheck:
	uv run --group dev mypy src

test:
	uv run --group dev pytest -o "anyio_mode=auto"

mypy:
	uv run --group dev mypy src tests examples

clean:
	rm -rf .venv/
	rm -rf .uv-cache/
	rm -rf dist build .mypy_cache .ruff_cache .pytest_cache *.egg-info
	rm -rf data/
	rm -rf **/**/__pycache__
	rm -rf **/__pycache__
	rm -f *.bin

build: clean init
	uv run python -m build

publish-test: build
	uv run twine check dist/*
	uv run twine upload --repository testpypi dist/*

publish: build
	uv run twine check dist/*
	uv run twine upload dist/*

py-spy:
	py-spy record -o profile.svg --subprocesses -- python examples/url_titles_spider.py --urls-file ./data/lobsters.jl --output data/url_titles.jl

memray:
	uv run --with memray memray run -o memray_report.bin examples/url_titles_spider.py --urls-file ./data/lobsters.jl --output data/url_titles.jl
	uv run --with memray memray flamegraph memray_report.bin -o memray_report.svg
