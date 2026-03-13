// Package shard implements content-addressed data chunking for the pipeline.
package shard

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/ipfs-data-pipeline/go-daemon/pkg/models"
)

// Engine handles splitting data into content-addressed shards.
type Engine struct {
	defaultShardSize int64
	maxShardSize     int64
	minShardSize     int64
}

// NewEngine creates a new sharding engine with the given configuration.
func NewEngine(defaultSize, minSize, maxSize int64) *Engine {
	return &Engine{
		defaultShardSize: defaultSize,
		minShardSize:     minSize,
		maxShardSize:     maxSize,
	}
}

// ShardResult holds a single shard's data and metadata.
type ShardResult struct {
	Index    int
	Data     []byte
	Size     int64
	Checksum string
}

// Shard splits the data from the reader into fixed-size content-addressed chunks.
// If shardSize is 0, the default shard size is used.
func (e *Engine) Shard(reader io.Reader, shardSize int64) ([]*ShardResult, int64, error) {
	if shardSize <= 0 {
		shardSize = e.defaultShardSize
	}
	if shardSize < e.minShardSize {
		shardSize = e.minShardSize
	}
	if shardSize > e.maxShardSize {
		shardSize = e.maxShardSize
	}

	var shards []*ShardResult
	var totalSize int64
	index := 0

	for {
		buf := make([]byte, shardSize)
		n, err := io.ReadFull(reader, buf)

		if n > 0 {
			data := buf[:n]
			checksum := computeChecksum(data)

			shards = append(shards, &ShardResult{
				Index:    index,
				Data:     data,
				Size:     int64(n),
				Checksum: checksum,
			})
			totalSize += int64(n)
			index++
		}

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("reading data for shard %d: %w", index, err)
		}
	}

	return shards, totalSize, nil
}

// ValidateShard verifies a shard's data matches its checksum.
func ValidateShard(shard *models.ShardInfo, data []byte) bool {
	return computeChecksum(data) == shard.Checksum
}

// computeChecksum generates a SHA-256 hex digest of the data.
func computeChecksum(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
