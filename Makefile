REPO_OWNER=synadia-io
PROJECT_NAME=orbit.py


help:
	@cat $(MAKEFILE_LIST) | \
	grep -E '^[a-zA-Z_-]+:.*?##' | \
	sed "s/local-//" | \
	sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'


clean: ## Remove build/test artifacts
	find . -name "*.py[co]" -delete
	find . -name "__pycache__" -type d -delete


deps: ## Sync the workspace
	uv sync


format: ## Format all sources
	uv run ruff format .


test: ## Run lint, type, spell, and unit checks
	uv run ruff format --check .
	uv run ruff check .
	uv run codespell
	uv run ty check
	uv run pytest


ci: deps ## Run checks for CI
	uv run ruff check .
	uv run pytest -x -vv -s --continue-on-collection-errors
