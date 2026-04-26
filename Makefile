.PHONY: help install lint format test build clean run auth

help:
	@echo "Available targets:"
	@echo "  install   - Install dependencies via uv"
	@echo "  lint      - Run ruff lint checks"
	@echo "  format    - Format code with black + ruff --fix"
	@echo "  test      - Run pytest"
	@echo "  build     - Build Docker image"
	@echo "  clean     - Remove caches and build artefacts"
	@echo "  run       - Run a one-shot sync locally"

install:
	uv sync

lint:
	uv run ruff check src tests

format:
	uv run ruff check --fix src tests
	uv run black src tests

test:
	uv run pytest -v

build:
	docker build -t homework-hub:latest .

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache build dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +

run:
	uv run python -m homework_hub sync
