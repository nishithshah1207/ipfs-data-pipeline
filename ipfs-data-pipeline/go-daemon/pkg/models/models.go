// Package models defines shared data types for the IPFS data pipeline.
package models

import "time"

// ShardInfo represents a single shard of a dataset stored on IPFS.
type ShardInfo struct {
	ID        string    `json:"id"`
	DatasetID string    `json:"dataset_id"`
	Index     int       `json:"index"`
	CID       string    `json:"cid"`
	Size      int64     `json:"size"`
	Checksum  string    `json:"checksum"`
	CreatedAt time.Time `json:"created_at"`
}

// PinStatus represents the pin state of a shard on an IPFS node.
type PinStatus struct {
	ShardID   string    `json:"shard_id"`
	CID       string    `json:"cid"`
	NodeID    string    `json:"node_id"`
	Pinned    bool      `json:"pinned"`
	CheckedAt time.Time `json:"checked_at"`
}

// Dataset represents a collection of shards that form a complete dataset.
type Dataset struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	TotalSize   int64        `json:"total_size"`
	ShardCount  int          `json:"shard_count"`
	ShardSize   int64        `json:"shard_size"`
	Shards      []*ShardInfo `json:"shards,omitempty"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
}

// IngestRequest is the request body for ingesting data.
type IngestRequest struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	ShardSize   int64  `json:"shard_size,omitempty"` // bytes, default 1MB
}

// IngestResponse is returned after successful ingestion.
type IngestResponse struct {
	DatasetID  string       `json:"dataset_id"`
	Name       string       `json:"name"`
	TotalSize  int64        `json:"total_size"`
	ShardCount int          `json:"shard_count"`
	Shards     []*ShardInfo `json:"shards"`
}

// QueryResponse is returned when querying a dataset.
type QueryResponse struct {
	Dataset *Dataset `json:"dataset"`
}

// HealthResponse represents the system health status.
type HealthResponse struct {
	Status      string            `json:"status"`
	IPFSNodes   []NodeHealth      `json:"ipfs_nodes"`
	TotalShards int               `json:"total_shards"`
	PinnedRatio float64           `json:"pinned_ratio"`
	Uptime      string            `json:"uptime"`
}

// NodeHealth represents the health of a single IPFS node.
type NodeHealth struct {
	ID        string `json:"id"`
	Address   string `json:"address"`
	Reachable bool   `json:"reachable"`
	PinCount  int    `json:"pin_count"`
}

// ReplicationStatus represents the replication health of a dataset.
type ReplicationStatus struct {
	DatasetID       string       `json:"dataset_id"`
	TotalShards     int          `json:"total_shards"`
	FullyReplicated int          `json:"fully_replicated"`
	UnderReplicated int          `json:"under_replicated"`
	Missing         int          `json:"missing"`
	PinStatuses     []*PinStatus `json:"pin_statuses,omitempty"`
}

// MetricsSnapshot captures pipeline performance metrics at a point in time.
type MetricsSnapshot struct {
	Timestamp         time.Time `json:"timestamp"`
	IngestThroughput  float64   `json:"ingest_throughput_mbps"`
	QueryLatencyP50   float64   `json:"query_latency_p50_ms"`
	QueryLatencyP99   float64   `json:"query_latency_p99_ms"`
	ActiveDatasets    int       `json:"active_datasets"`
	TotalShardsStored int       `json:"total_shards_stored"`
	TotalDataStored   int64     `json:"total_data_stored_bytes"`
	IPFSNodesOnline   int       `json:"ipfs_nodes_online"`
}

// ErrorResponse is a standard error response.
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
	Details string `json:"details,omitempty"`
}

// ListDatasetsResponse is returned when listing datasets.
type ListDatasetsResponse struct {
	Datasets []*Dataset `json:"datasets"`
	Total    int        `json:"total"`
}
