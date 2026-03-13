// Package replication provides a background monitor that ensures data availability
// across multiple IPFS nodes by checking pin health and re-pinning as needed.
package replication

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ipfs-data-pipeline/go-daemon/internal/ipfs"
	"github.com/ipfs-data-pipeline/go-daemon/internal/metadata"
	"github.com/ipfs-data-pipeline/go-daemon/pkg/models"
)

// Monitor periodically checks pin health and re-pins shards as necessary.
type Monitor struct {
	ipfsClient    *ipfs.Client
	store         *metadata.Store
	minReplicas   int
	checkInterval time.Duration
	cancel        context.CancelFunc
	wg            sync.WaitGroup

	mu             sync.RWMutex
	lastCheckTime  time.Time
	lastCheckStats CheckStats
}

// CheckStats holds statistics from the last replication check.
type CheckStats struct {
	TotalShards     int       `json:"total_shards"`
	FullyReplicated int       `json:"fully_replicated"`
	UnderReplicated int       `json:"under_replicated"`
	RePinned        int       `json:"re_pinned"`
	Failed          int       `json:"failed"`
	Duration        string    `json:"duration"`
	CheckedAt       time.Time `json:"checked_at"`
}

// NewMonitor creates a new replication monitor.
func NewMonitor(ipfsClient *ipfs.Client, store *metadata.Store, minReplicas int, checkInterval time.Duration) *Monitor {
	return &Monitor{
		ipfsClient:    ipfsClient,
		store:         store,
		minReplicas:   minReplicas,
		checkInterval: checkInterval,
	}
}

// Start begins the background replication monitoring loop.
func (m *Monitor) Start(ctx context.Context) {
	ctx, m.cancel = context.WithCancel(ctx)
	m.wg.Add(1)

	go func() {
		defer m.wg.Done()
		log.Info().
			Int("min_replicas", m.minReplicas).
			Dur("check_interval", m.checkInterval).
			Msg("replication monitor started")

		// Run initial check
		m.runCheck()

		ticker := time.NewTicker(m.checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("replication monitor stopped")
				return
			case <-ticker.C:
				m.runCheck()
			}
		}
	}()
}

// Stop stops the replication monitor.
func (m *Monitor) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

// LastStats returns the statistics from the most recent check.
func (m *Monitor) LastStats() CheckStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastCheckStats
}

// CheckNow triggers an immediate replication check.
func (m *Monitor) CheckNow() CheckStats {
	return m.runCheck()
}

func (m *Monitor) runCheck() CheckStats {
	start := time.Now()
	nodes := m.ipfsClient.Nodes()

	shards, err := m.store.GetAllShards()
	if err != nil {
		log.Error().Err(err).Msg("failed to get shards for replication check")
		return CheckStats{}
	}

	stats := CheckStats{
		TotalShards: len(shards),
		CheckedAt:   start,
	}

	for _, shard := range shards {
		replicaCount := 0
		for _, node := range nodes {
			pinned, err := m.ipfsClient.IsPinned(node, shard.CID)
			if err != nil {
				log.Debug().Str("cid", shard.CID).Str("node", node).Err(err).Msg("pin check error")
				continue
			}

			// Update pin status in metadata store
			ps := &models.PinStatus{
				ShardID:   shard.ID,
				CID:       shard.CID,
				NodeID:    node,
				Pinned:    pinned,
				CheckedAt: time.Now(),
			}
			if storeErr := m.store.UpsertPinStatus(ps); storeErr != nil {
				log.Error().Err(storeErr).Msg("failed to update pin status")
			}

			if pinned {
				replicaCount++
			}
		}

		if replicaCount >= m.minReplicas {
			stats.FullyReplicated++
		} else {
			stats.UnderReplicated++
			// Attempt to re-pin on nodes where not pinned
			rePinned := m.rePinShard(shard, nodes, replicaCount)
			stats.RePinned += rePinned
			if rePinned == 0 && replicaCount < m.minReplicas {
				stats.Failed++
			}
		}
	}

	stats.Duration = time.Since(start).String()

	m.mu.Lock()
	m.lastCheckTime = time.Now()
	m.lastCheckStats = stats
	m.mu.Unlock()

	log.Info().
		Int("total", stats.TotalShards).
		Int("fully_replicated", stats.FullyReplicated).
		Int("under_replicated", stats.UnderReplicated).
		Int("re_pinned", stats.RePinned).
		Str("duration", stats.Duration).
		Msg("replication check completed")

	return stats
}

func (m *Monitor) rePinShard(shard *models.ShardInfo, nodes []string, currentReplicas int) int {
	needed := m.minReplicas - currentReplicas
	if needed <= 0 {
		return 0
	}

	rePinned := 0
	for _, node := range nodes {
		if rePinned >= needed {
			break
		}

		pinned, _ := m.ipfsClient.IsPinned(node, shard.CID)
		if pinned {
			continue
		}

		if err := m.ipfsClient.Pin(node, shard.CID); err != nil {
			log.Warn().
				Str("cid", shard.CID).
				Str("node", node).
				Err(err).
				Msg("failed to re-pin shard")
			continue
		}

		log.Info().
			Str("cid", shard.CID).
			Str("node", node).
			Msg("successfully re-pinned shard")

		// Update pin status
		ps := &models.PinStatus{
			ShardID:   shard.ID,
			CID:       shard.CID,
			NodeID:    node,
			Pinned:    true,
			CheckedAt: time.Now(),
		}
		m.store.UpsertPinStatus(ps)
		rePinned++
	}

	return rePinned
}

// GetReplicationStatus returns the replication status for a specific dataset.
func (m *Monitor) GetReplicationStatus(datasetID string) (*models.ReplicationStatus, error) {
	shards, err := m.store.GetShardsByDataset(datasetID)
	if err != nil {
		return nil, err
	}

	nodes := m.ipfsClient.Nodes()
	status := &models.ReplicationStatus{
		DatasetID:   datasetID,
		TotalShards: len(shards),
	}

	for _, shard := range shards {
		replicaCount := 0
		for _, node := range nodes {
			pinned, _ := m.ipfsClient.IsPinned(node, shard.CID)
			ps := &models.PinStatus{
				ShardID:   shard.ID,
				CID:       shard.CID,
				NodeID:    node,
				Pinned:    pinned,
				CheckedAt: time.Now(),
			}
			status.PinStatuses = append(status.PinStatuses, ps)
			if pinned {
				replicaCount++
			}
		}

		if replicaCount >= m.minReplicas {
			status.FullyReplicated++
		} else if replicaCount > 0 {
			status.UnderReplicated++
		} else {
			status.Missing++
		}
	}

	return status, nil
}
