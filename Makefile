# Pepper — Go + Python runtime
.PHONY: all test go-test go-test-unit go-real go-real-smoke go-real-whisper \
        go-real-pipeline go-real-recycle go-real-cli go-real-song \
        python-test python-test-unit python-setup \
        check-services python-setup-real clean

PYTHON_DIR := runtime/python
VENV        := .venv
PYTHON      := $(VENV)/bin/python
PIP         := $(VENV)/bin/pip
PYTEST      := $(VENV)/bin/pytest

# ── External service addresses ─────────────────────────────────────────────────
PEPPER_TEST_REDIS_HOST ?= 127.0.0.1
PEPPER_TEST_REDIS_PORT ?= 6379
PEPPER_TEST_NATS_HOST  ?= 127.0.0.1
PEPPER_TEST_NATS_PORT  ?= 4222

export PEPPER_TEST_REDIS_HOST
export PEPPER_TEST_REDIS_PORT
export PEPPER_TEST_NATS_HOST
export PEPPER_TEST_NATS_PORT

# ── Real-test tuning (override on the command line or in env) ──────────────────
# Model size for faster-whisper: tiny | base | small | medium | large-v3
PEPPER_WHISPER_MODEL ?= tiny
# Ollama model to use (auto-detected if blank)
PEPPER_OLLAMA_MODEL  ?=
# Path to a real WAV file for the full pipeline test (silence used if blank)
PEPPER_TEST_AUDIO    ?=
# Path to an MP3/audio file for the song analysis test (placeholder tone used if blank)
PEPPER_TEST_MP3      ?=
# Google Gemini API key — if set, song analysis uses Gemini instead of Ollama
PEPPER_GEMINI_KEY    ?=
# HuggingFace model cache dir (leave blank to use ~/.cache/huggingface)
HF_HOME              ?=

export PEPPER_WHISPER_MODEL
export PEPPER_OLLAMA_MODEL
export PEPPER_TEST_AUDIO
export PEPPER_TEST_MP3
export PEPPER_GEMINI_KEY
export HF_HOME

all: test

# ─── Standard Go tests (race detector, all packages) ─────────────────────────
# Redis and NATS bus tests skip automatically when the service is not running.
go-test:
	go test -race -count=1 ./...

# Unit tests only — no external services, no Python, no models:
go-test-unit:
	go test -race -count=1 -run "^Test[^(Redis|NATS|Real)]" ./...

# ─── Real pipeline integration tests ─────────────────────────────────────────
# Each target runs a focused subset so you can iterate quickly.
#
# IMPORTANT: real tests use the .venv Python so packages installed via
# "make python-setup" (faster-whisper, msgpack, etc.) are visible to both
# the Go package-check (requirePythonPackage) and the spawned worker processes.
# The PATH prepend below ensures system calls to python3 resolve to .venv/bin/python3.
#
# Dependency map:
#   go-real-smoke    → python3 + msgpack (no model download)
#   go-real-cli      → python3 + msgpack + ffprobe
#   go-real-whisper  → python3 + msgpack + faster-whisper (downloads ~75 MB on first run)
#   go-real-pipeline → python3 + msgpack + faster-whisper + Ollama running
#   go-real-recycle  → python3 + msgpack + faster-whisper
#   go-real-song     → python3 + msgpack + faster-whisper + ffmpeg + Ollama/Gemini
#   go-real          → all of the above
#
# Install real-test deps (one-time):
#   make python-setup
#   .venv/bin/pip install faster-whisper

# Absolute path to the venv python3 — used to prepend PATH for real tests.
VENV_PYTHON_DIR := $(abspath $(VENV)/bin)


# Smoke: Python subprocess + bus round-trip only. No model, no GPU.
go-real-nomic:
	PATH=$(VENV_PYTHON_DIR):$$PATH PEPPER_REAL_TESTS=1 go test -v -count=1 \
		-run "^TestNomic" \
		-timeout 300s \
		./tests/...

go-real-nomic-bench:
	PATH=$(VENV_PYTHON_DIR):$$PATH PEPPER_REAL_TESTS=1 go test -bench=BenchmarkNomic \
		-run=^$ \
		-benchmem \
		-benchtime=10s \
		-count=2 \
		-timeout 300s \
		./tests/...

# Smoke: Python subprocess + bus round-trip only. No model, no GPU.
go-real-smoke:
	PATH=$(VENV_PYTHON_DIR):$$PATH PEPPER_REAL_TESTS=1 go test -v -count=1 \
		-run TestRealDenoisePassthrough \
		-timeout 60s .

# CLI adapter: verifies ffprobe via Python subprocess.
go-real-cli:
	PATH=$(VENV_PYTHON_DIR):$$PATH PEPPER_REAL_TESTS=1 go test -v -count=1 \
		-run TestRealCLIAdapter \
		-timeout 60s .

# Whisper cold/warm: measures model caching. Downloads model on first run.
# Set PEPPER_WHISPER_MODEL=base for better accuracy, tiny for speed.
go-real-whisper:
	@echo "Whisper model : $(PEPPER_WHISPER_MODEL)"
	@echo "HF cache      : $${HF_HOME:-~/.cache/huggingface}"
	PATH=$(VENV_PYTHON_DIR):$$PATH PEPPER_REAL_TESTS=1 go test -v -count=1 \
		-run TestRealWhisperColdWarm \
		-timeout 240s .

# Worker recycle: MaxRequests=2, sends 4 requests, observes respawn + model reload.
go-real-recycle:
	@echo "Whisper model : $(PEPPER_WHISPER_MODEL)"
	PATH=$(VENV_PYTHON_DIR):$$PATH PEPPER_REAL_TESTS=1 go test -v -count=1 \
		-run TestRealWorkerRecycle \
		-timeout 240s .

# Full pipeline: audio → denoise → Whisper → Ollama LLM.
# Set PEPPER_TEST_AUDIO=/path/to/speech.wav for a real transcript.
# Requires Ollama running: ollama serve && ollama pull gemma:2b
go-real-pipeline:
	@echo "Whisper model : $(PEPPER_WHISPER_MODEL)"
	@echo "Ollama model  : $${PEPPER_OLLAMA_MODEL:-auto-detect}"
	@echo "Audio input   : $${PEPPER_TEST_AUDIO:-[generated silence]}"
	PATH=$(VENV_PYTHON_DIR):$$PATH PEPPER_REAL_TESTS=1 go test -v -count=1 \
		-run TestRealFullPipeline \
		-timeout 240s .

# Song analysis: MP3/audio → ffmpeg → Whisper lyrics → Ollama/Gemini analysis.
# Uses tests/testdata/audio/sample.mp3 by default (placeholder tone).
# Replace with a real song for a meaningful transcript.
#   Ollama:  make go-real-song                                 (needs Ollama running)
#   Gemini:  make go-real-song PEPPER_GEMINI_KEY=AIza...      (cloud, no local GPU)
#   Custom:  make go-real-song PEPPER_TEST_MP3=/path/to/song.mp3
go-real-song:
	@echo "Whisper model : $(PEPPER_WHISPER_MODEL)"
	@echo "Audio input   : $${PEPPER_TEST_MP3:-tests/testdata/audio/sample.mp3 (placeholder)}"
	@if [ -n "$(PEPPER_GEMINI_KEY)" ]; then echo "LLM backend   : Gemini"; 	  else echo "LLM backend   : Ollama ($${PEPPER_OLLAMA_MODEL:-auto-detect})"; fi
	PATH=$(VENV_PYTHON_DIR):$$PATH PEPPER_REAL_TESTS=1 go test -v -count=1 \
		-run TestRealSongAnalysis \
		-timeout 240s ./tests/...

# Run all real tests in dependency order.
# Each test skips individually if its requirements aren't met.
go-real:
	@echo "═══════════════════════════════════════════════"
	@echo "  Pepper real integration tests"
	@echo "  Whisper model : $(PEPPER_WHISPER_MODEL)"
	@echo "  HF cache      : $${HF_HOME:-~/.cache/huggingface}"
	@echo "  Audio (WAV)   : $${PEPPER_TEST_AUDIO:-[generated silence]}"
	@echo "  Audio (MP3)   : $${PEPPER_TEST_MP3:-tests/testdata/audio/sample.mp3}"
	@echo "  Ollama model  : $${PEPPER_OLLAMA_MODEL:-auto-detect}"
	@if [ -n "$${PEPPER_GEMINI_KEY}" ]; then echo "  LLM backend   : Gemini (song test)"; fi
	@echo "═══════════════════════════════════════════════"
	PATH=$(VENV_PYTHON_DIR):$$PATH PEPPER_REAL_TESTS=1 go test -v -count=1 \
		-run "^TestReal" \
		-timeout 600s .


# Requires Redis and/or NATS running locally.
#
#   Start services:
#     redis-server &
#     nats-server -js &
#
#   Run all cluster tests (both backends):
#     make go-real-cluster
#
#   Run a specific backend:
#     make go-real-cluster-redis
#     make go-real-cluster-nats
#
#   Override service addresses:
#     make go-real-cluster PEPPER_TEST_REDIS_HOST=10.0.0.1

go-real-cluster-redis:
	@echo "Cluster backend : Redis ($(PEPPER_TEST_REDIS_HOST):$(PEPPER_TEST_REDIS_PORT))"
	PATH=$(VENV_PYTHON_DIR):$$PATH PEPPER_REAL_TESTS=1 go test -v -count=1 \
		-run TestRealClusterRedis \
		-timeout 240s ./tests/...

go-real-cluster-nats:
	@echo "Cluster backend : NATS ($(PEPPER_TEST_NATS_HOST):$(PEPPER_TEST_NATS_PORT))"
	PATH=$(VENV_PYTHON_DIR):$$PATH PEPPER_REAL_TESTS=1 go test -v -count=1 \
		-run TestRealClusterNATS \
		-timeout 240s ./tests/...

go-real-cluster: go-real-cluster-redis go-real-cluster-nats


# ─── Python tests ─────────────────────────────────────────────────────────────
python-test: python-setup
	$(PYTEST) $(PYTHON_DIR) -v

python-test-unit: python-setup
	$(PYTEST) $(PYTHON_DIR) -v -k "not (Redis or NATS or redis or nats)"

# ─── Python venv ──────────────────────────────────────────────────────────────
python-setup: $(VENV)/bin/pytest

$(VENV)/bin/pytest: $(PYTHON_DIR)/pyproject.toml
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e $(PYTHON_DIR)[dev]
	@touch $(VENV)/bin/pytest

# Install real-test Python deps (faster-whisper etc) into the venv.
# Run this once before make go-real-whisper / go-real-song / go-real-pipeline.
python-setup-real: python-setup
	$(PIP) install faster-whisper
	$(PIP) install msgpack
	$(PIP) install nomic

# ─── Both ─────────────────────────────────────────────────────────────────────
test: go-test python-test

# ─── Service + environment check ─────────────────────────────────────────────
check-services:
	@echo "External services:"
	@nc -zw1 $(PEPPER_TEST_REDIS_HOST) $(PEPPER_TEST_REDIS_PORT) 2>/dev/null \
		&& echo "  Redis  $(PEPPER_TEST_REDIS_HOST):$(PEPPER_TEST_REDIS_PORT) ✓" \
		|| echo "  Redis  $(PEPPER_TEST_REDIS_HOST):$(PEPPER_TEST_REDIS_PORT) ✗ (bus tests skip)"
	@nc -zw1 $(PEPPER_TEST_NATS_HOST) $(PEPPER_TEST_NATS_PORT) 2>/dev/null \
		&& echo "  NATS   $(PEPPER_TEST_NATS_HOST):$(PEPPER_TEST_NATS_PORT) ✓" \
		|| echo "  NATS   $(PEPPER_TEST_NATS_HOST):$(PEPPER_TEST_NATS_PORT) ✗ (bus tests skip)"
	@nc -zw1 127.0.0.1 11434 2>/dev/null \
		&& echo "  Ollama 127.0.0.1:11434 ✓" \
		|| echo "  Ollama 127.0.0.1:11434 ✗ (go-real-pipeline skips)"
	@echo ""
	@echo "Real test environment:"
	@$(VENV)/bin/python3 -c "import faster_whisper; print('  faster-whisper ✓')" 2>/dev/null \
		|| echo "  faster-whisper ✗ (run: .venv/bin/pip install faster-whisper)"
	@python3 -c "import msgpack; print('  msgpack        ✓')" 2>/dev/null \
		|| echo "  msgpack        ✗ (all Python tests skip)"
	@command -v sox >/dev/null 2>&1 \
		&& echo "  sox            ✓ (real noise reduction)" \
		|| echo "  sox            ✗ (denoise falls back to passthrough)"
	@command -v ffprobe >/dev/null 2>&1 \
		&& echo "  ffprobe        ✓" \
		|| echo "  ffprobe        ✗ (go-real-cli skips)"
	@command -v ffmpeg >/dev/null 2>&1 \
		&& echo "  ffmpeg         ✓" \
		|| echo "  ffmpeg         ✗ (go-real-song skips)"
	@nvidia-smi >/dev/null 2>&1 \
		&& echo "  GPU            ✓" \
		|| echo "  GPU            ✗ (Whisper runs on CPU)"
	@echo ""
	@echo "Model cache: $${HF_HOME:-~/.cache/huggingface}"
	@ls "$${HF_HOME:-$$HOME/.cache/huggingface}/hub/" 2>/dev/null \
		| grep -i whisper \
		| sed 's/^/  cached: /' \
		|| echo "  (no Whisper models cached yet)"

# ─── Cleanup ──────────────────────────────────────────────────────────────────
clean:
	rm -rf $(VENV)
	go clean -testcache
