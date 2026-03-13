# Architecture Deep Dive

## System Overview

The IPFS-Backed Distributed Data Pipeline is a two-tier system:

1. **Go Daemon** — A stateful backend service that manages data ingestion, sharding, IPFS pinning, metadata tracking, and replication monitoring.
2. **Python Client** — A stateless CLI and library that communicates with the daemon over HTTP.

## Data Lifecycle

### Ingestion Flow

1. Client uploads a file via `POST /api/v1/ingest` (multipart form).
2. The Shard Engine splits the data into fixed-size, content-addressed chunks.
   - Each chunk is SHA-256 checksummed.
   - Chunk size is configurable (default: 1MB, range: 64KB–64MB).
3. Each shard is added to **all** configured IPFS nodes in parallel via the Kubo HTTP RPC API (`/api/v0/add`).
4. The CID (Content Identifier) returned by IPFS is stored in the metadata database along with shard index, size, and checksum.
5. Pin statuses are recorded for each shard-node pair.

### Query / Reconstruction Flow

1. Client requests a dataset by ID via `GET /api/v1/datasets/{id}/download`.
2. The daemon retrieves the ordered shard list from the metadata store.
3. For each shard (in order), data is fetched from the first available IPFS node.
4. Each shard's SHA-256 checksum is validated before streaming to the client.
5. Data is streamed directly to the HTTP response (no full buffering).

### Replication Flow

1. A background goroutine runs on a configurable interval (default: 30s).
2. For each shard in the metadata store:
   - Pin status is checked on every IPFS node via `/api/v0/pin/ls`.
   - Results are stored in the `pin_statuses` table.
3. Under-replicated shards (below `MIN_REPLICAS`) are re-pinned on available nodes.
4. Statistics (fully replicated, under-replicated, re-pinned, failed) are logged and exposed via API.

## Database Schema

```sql
-- Datasets
CREATE TABLE datasets (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT DEFAULT '',
    total_size INTEGER NOT NULL DEFAULT 0,
    shard_count INTEGER NOT NULL DEFAULT 0,
    shard_size INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL
);

-- Shards (ordered by index within each dataset)
CREATE TABLE shards (
    id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES datasets(id),
    shard_index INTEGER NOT NULL,
    cid TEXT NOT NULL,
    size INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    created_at DATETIME NOT NULL
);

-- Pin status per shard per IPFS node
CREATE TABLE pin_statuses (
    shard_id TEXT NOT NULL REFERENCES shards(id),
    cid TEXT NOT NULL,
    node_id TEXT NOT NULL,
    pinned BOOLEAN NOT NULL DEFAULT 0,
    checked_at DATETIME NOT NULL,
    PRIMARY KEY (shard_id, node_id)
);
```

## Concurrency Model

- **Ingest:** Shards are uploaded to IPFS nodes concurrently using goroutines with a sync.WaitGroup.
- **Replication Monitor:** Runs as a single background goroutine with periodic ticks.
- **Metadata Store:** SQLite with WAL journal mode and busy timeout for safe concurrent reads.
- **HTTP Server:** Standard Go net/http with gorilla/mux, each request in its own goroutine.

## Security Considerations

- IPFS nodes should be on a private network or behind a firewall (Kubo API has no auth by default).
- The daemon API has CORS enabled for development; restrict in production.
- Data is content-addressed but not encrypted; add encryption at the application layer if needed.
- SQLite database should be on a persistent volume in production.
