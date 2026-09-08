.PHONY: help install-dev test lint format format-check typecheck check demo-showcase demo-standard cleanup purge-evidence

RUN_ID ?= run-local-showcase

help:
	@echo "Market Data Reliability Lab"
	@echo "  make install-dev                         Install the package and pinned dev tools"
	@echo "  make check                               Run lint, format, types, and tests"
	@echo "  make demo-showcase RUN_ID=<id>           Run the 1,236-record recovery scenario"
	@echo "  make demo-standard RUN_ID=<id>           Run the 12,763-record recovery scenario"
	@echo "  make cleanup RUN_ID=<id>                 Remove one run's runtime; retain evidence"
	@echo "  make purge-evidence RUN_ID=<id>          Delete evidence after runtime cleanup"

install-dev:
	python -m pip install -e ".[dev]"

test:
	python -m pytest

lint:
	python -m ruff check .

format:
	python -m ruff format .

format-check:
	python -m ruff format --check .

typecheck:
	python -m mypy

check: lint format-check typecheck test

demo-showcase:
	python scripts/demo.py --scenario recovery-showcase --run-id $(RUN_ID)

demo-standard:
	python scripts/demo.py --scenario recovery --run-id $(RUN_ID)

cleanup:
	python scripts/demo.py --cleanup-run $(RUN_ID)

purge-evidence:
	python scripts/demo.py --purge-evidence $(RUN_ID)
