#!/usr/bin/env bash
set -euo pipefail

echo "=== IPFS Data Pipeline - Setup ==="
echo ""

# Check prerequisites
command -v go >/dev/null 2>&1 || { echo "ERROR: Go is required but not installed."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: Python 3 is required but not installed."; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "WARNING: Docker is recommended for running IPFS nodes."; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "[1/3] Setting up Go daemon..."
cd "$PROJECT_DIR/go-daemon"
go mod tidy
go mod download
echo "  Go dependencies installed."

echo ""
echo "[2/3] Setting up Python client..."
cd "$PROJECT_DIR/python-client"
python3 -m pip install -e ".[dev,benchmark]" --quiet
echo "  Python client installed."

echo ""
echo "[3/3] Building Go daemon..."
cd "$PROJECT_DIR"
mkdir -p bin
cd go-daemon
CGO_ENABLED=1 go build -o ../bin/pipeline-daemon ./cmd/daemon
echo "  Binary built: bin/pipeline-daemon"

echo ""
echo "=== Setup complete! ==="
echo ""
echo "Quick start:"
echo "  make run     # Start with Docker Compose (IPFS nodes + daemon)"
echo "  make demo    # Run a demo workflow"
echo "  make bench   # Run benchmarks"
