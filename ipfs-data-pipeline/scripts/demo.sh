#!/usr/bin/env bash
set -euo pipefail

API_URL="${PIPELINE_API_URL:-http://localhost:8080}"

echo "=== IPFS Data Pipeline Demo ==="
echo "API: $API_URL"
echo ""

# Wait for service
echo "[0] Waiting for pipeline daemon..."
for i in $(seq 1 30); do
    if curl -sf "$API_URL/api/v1/health" > /dev/null 2>&1; then
        echo "  Daemon is ready!"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  ERROR: Daemon not available after 30s. Run 'make run' first."
        exit 1
    fi
    sleep 1
done

echo ""

# 1. Check health
echo "[1] Checking system health..."
curl -s "$API_URL/api/v1/health" | python3 -m json.tool
echo ""

# 2. Generate test data
echo "[2] Generating test data (5MB)..."
TEST_FILE=$(mktemp /tmp/pipeline-demo-XXXXXX.bin)
dd if=/dev/urandom of="$TEST_FILE" bs=1M count=5 2>/dev/null
echo "  Created: $TEST_FILE ($(du -h "$TEST_FILE" | cut -f1))"
echo ""

# 3. Ingest
echo "[3] Ingesting test data..."
INGEST_RESULT=$(curl -s -X POST "$API_URL/api/v1/ingest" \
    -F "file=@$TEST_FILE" \
    -F "name=demo-dataset" \
    -F "description=Demo pipeline test data" \
    -F "shard_size=1048576")
echo "$INGEST_RESULT" | python3 -m json.tool

DATASET_ID=$(echo "$INGEST_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin)['dataset_id'])")
echo "  Dataset ID: $DATASET_ID"
echo ""

# 4. List datasets
echo "[4] Listing all datasets..."
curl -s "$API_URL/api/v1/datasets" | python3 -m json.tool
echo ""

# 5. Query dataset
echo "[5] Querying dataset $DATASET_ID..."
curl -s "$API_URL/api/v1/datasets/$DATASET_ID" | python3 -m json.tool
echo ""

# 6. Check replication
echo "[6] Checking replication status..."
curl -s "$API_URL/api/v1/datasets/$DATASET_ID/replication" | python3 -m json.tool
echo ""

# 7. Download and verify
echo "[7] Downloading and verifying data integrity..."
DOWNLOAD_FILE=$(mktemp /tmp/pipeline-demo-download-XXXXXX.bin)
curl -s "$API_URL/api/v1/datasets/$DATASET_ID/download" -o "$DOWNLOAD_FILE"

ORIG_HASH=$(sha256sum "$TEST_FILE" | cut -d' ' -f1)
DL_HASH=$(sha256sum "$DOWNLOAD_FILE" | cut -d' ' -f1)

if [ "$ORIG_HASH" = "$DL_HASH" ]; then
    echo "  PASS: Data integrity verified (SHA-256 match)"
else
    echo "  FAIL: Data integrity check failed!"
    echo "    Original: $ORIG_HASH"
    echo "    Download: $DL_HASH"
fi
echo ""

# 8. Trigger replication check
echo "[8] Triggering manual replication check..."
curl -s -X POST "$API_URL/api/v1/replication/check" | python3 -m json.tool
echo ""

# 9. Delete dataset
echo "[9] Deleting test dataset..."
curl -s -X DELETE "$API_URL/api/v1/datasets/$DATASET_ID" | python3 -m json.tool
echo ""

# Cleanup
rm -f "$TEST_FILE" "$DOWNLOAD_FILE"

echo "=== Demo complete! ==="
