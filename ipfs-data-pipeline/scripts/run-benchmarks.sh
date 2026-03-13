#!/usr/bin/env bash
set -euo pipefail

API_URL="${PIPELINE_API_URL:-http://localhost:8080}"
OUTPUT_DIR="${1:-benchmarks/results}"

echo "=== IPFS Data Pipeline Benchmarks ==="
echo "API: $API_URL"
echo "Output: $OUTPUT_DIR"
echo ""

# Wait for service
echo "Waiting for pipeline daemon..."
for i in $(seq 1 30); do
    if curl -sf "$API_URL/api/v1/health" > /dev/null 2>&1; then
        echo "Daemon is ready!"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: Daemon not available. Run 'make run' first."
        exit 1
    fi
    sleep 1
done

echo ""
echo "Running benchmarks..."
echo ""

mkdir -p "$OUTPUT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="$OUTPUT_DIR/benchmark_${TIMESTAMP}.json"

cd "$(dirname "${BASH_SOURCE[0]}")/../python-client"
python3 -m pipeline_client.benchmark "$API_URL"

if [ -f "benchmark_results.json" ]; then
    mv benchmark_results.json "../$OUTPUT_FILE"
    echo ""
    echo "Results saved to: $OUTPUT_FILE"
fi

echo ""
echo "=== Benchmarks complete! ==="
