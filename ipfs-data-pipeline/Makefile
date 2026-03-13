.PHONY: all build test clean run stop logs bench help setup

# ─── Variables ──────────────────────────────────────────────
GO_DAEMON_DIR   := go-daemon
PYTHON_DIR      := python-client
DOCKER_COMPOSE  := docker compose

# ─── Default ────────────────────────────────────────────────
all: build

# ─── Help ───────────────────────────────────────────────────
help:
	@echo "IPFS Data Pipeline - Available targets:"
	@echo ""
	@echo "  setup         Install dependencies (Go + Python)"
	@echo "  build         Build Go daemon binary"
	@echo "  test          Run all tests (Go + Python)"
	@echo "  test-go       Run Go unit tests"
	@echo "  test-python   Run Python unit tests"
	@echo "  bench-go      Run Go benchmarks"
	@echo ""
	@echo "  run           Start all services via Docker Compose"
	@echo "  stop          Stop all services"
	@echo "  logs          View service logs"
	@echo "  status        Show service status"
	@echo ""
	@echo "  bench         Run pipeline benchmarks (requires running services)"
	@echo "  demo          Run demo workflow (requires running services)"
	@echo ""
	@echo "  clean         Remove build artifacts and volumes"

# ─── Setup ──────────────────────────────────────────────────
setup:
	cd $(GO_DAEMON_DIR) && go mod tidy && go mod download
	cd $(PYTHON_DIR) && pip install -e ".[dev,benchmark]"

# ─── Build ──────────────────────────────────────────────────
build:
	cd $(GO_DAEMON_DIR) && CGO_ENABLED=1 go build -o ../bin/pipeline-daemon ./cmd/daemon

# ─── Test ───────────────────────────────────────────────────
test: test-go test-python

test-go:
	cd $(GO_DAEMON_DIR) && go test -v -race ./...

test-python:
	cd $(PYTHON_DIR) && python -m pytest tests/ -v

bench-go:
	cd $(GO_DAEMON_DIR) && go test -bench=. -benchmem ./internal/shard/

# ─── Docker ─────────────────────────────────────────────────
run:
	$(DOCKER_COMPOSE) up -d --build
	@echo ""
	@echo "Services starting..."
	@echo "  Pipeline API:   http://localhost:8080"
	@echo "  IPFS Node 0:    http://localhost:5001"
	@echo "  IPFS Node 1:    http://localhost:5002"
	@echo "  IPFS Node 2:    http://localhost:5003"
	@echo ""
	@echo "Run 'make logs' to view logs"
	@echo "Run 'make status' to check service health"

stop:
	$(DOCKER_COMPOSE) down

logs:
	$(DOCKER_COMPOSE) logs -f

status:
	$(DOCKER_COMPOSE) ps
	@echo ""
	@curl -s http://localhost:8080/api/v1/health | python3 -m json.tool 2>/dev/null || echo "Daemon not ready yet..."

# ─── Benchmarks ─────────────────────────────────────────────
bench:
	@echo "Running pipeline benchmarks..."
	cd $(PYTHON_DIR) && python -m pipeline_client.benchmark http://localhost:8080
	@echo "Results saved to $(PYTHON_DIR)/benchmark_results.json"

# ─── Demo ───────────────────────────────────────────────────
demo:
	@./scripts/demo.sh

# ─── Clean ──────────────────────────────────────────────────
clean:
	rm -rf bin/
	$(DOCKER_COMPOSE) down -v --remove-orphans
	rm -f $(GO_DAEMON_DIR)/pipeline.db
	rm -f $(PYTHON_DIR)/benchmark_results.json
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
