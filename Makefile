# Pepper — Go + Python runtime
.PHONY: all test go-test python-test python-setup clean

PYTHON_DIR := internal/runtime/python
VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest

all: test

# ─── Go tests ─────────────────────────────────────────────────────────────────
go-test:
	go test -race -count=1 ./...

# ─── Python tests ─────────────────────────────────────────────────────────────
python-test: python-setup
	$(PYTEST) $(PYTHON_DIR) -v

python-setup: $(VENV)/bin/pytest

$(VENV)/bin/pytest: $(PYTHON_DIR)/pyproject.toml
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e $(PYTHON_DIR)[dev]
	@touch $(VENV)/bin/pytest

# ─── Both ─────────────────────────────────────────────────────────────────────
test: go-test python-test

# ─── Cleanup ──────────────────────────────────────────────────────────────────
clean:
	rm -rf $(VENV)
	go clean -testcache