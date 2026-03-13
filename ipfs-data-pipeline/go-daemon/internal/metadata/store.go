// Package metadata provides a SQLite-backed metadata store for tracking datasets, shards, and pin status.
package metadata

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"

	"github.com/ipfs-data-pipeline/go-daemon/pkg/models"
)

// Store manages dataset and shard metadata in SQLite.
type Store struct {
	db *sql.DB
}

// NewStore creates a new metadata store at the given path.
func NewStore(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	store := &Store{db: db}
	if err := store.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("running migrations: %w", err)
	}

	return store, nil
}

// Close closes the database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate() error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS datasets (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			description TEXT DEFAULT '',
			total_size INTEGER NOT NULL DEFAULT 0,
			shard_count INTEGER NOT NULL DEFAULT 0,
			shard_size INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS shards (
			id TEXT PRIMARY KEY,
			dataset_id TEXT NOT NULL,
			shard_index INTEGER NOT NULL,
			cid TEXT NOT NULL,
			size INTEGER NOT NULL,
			checksum TEXT NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (dataset_id) REFERENCES datasets(id)
		)`,
		`CREATE TABLE IF NOT EXISTS pin_statuses (
			shard_id TEXT NOT NULL,
			cid TEXT NOT NULL,
			node_id TEXT NOT NULL,
			pinned BOOLEAN NOT NULL DEFAULT 0,
			checked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (shard_id, node_id),
			FOREIGN KEY (shard_id) REFERENCES shards(id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_shards_dataset ON shards(dataset_id)`,
		`CREATE INDEX IF NOT EXISTS idx_shards_cid ON shards(cid)`,
		`CREATE INDEX IF NOT EXISTS idx_pin_statuses_cid ON pin_statuses(cid)`,
	}

	for _, m := range migrations {
		if _, err := s.db.Exec(m); err != nil {
			return fmt.Errorf("executing migration: %w", err)
		}
	}

	log.Info().Msg("database migrations completed")
	return nil
}

// CreateDataset stores a new dataset record.
func (s *Store) CreateDataset(ds *models.Dataset) error {
	_, err := s.db.Exec(
		`INSERT INTO datasets (id, name, description, total_size, shard_count, shard_size, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		ds.ID, ds.Name, ds.Description, ds.TotalSize, ds.ShardCount, ds.ShardSize,
		ds.CreatedAt, ds.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("inserting dataset: %w", err)
	}
	return nil
}

// GetDataset retrieves a dataset by ID, including its shards.
func (s *Store) GetDataset(id string) (*models.Dataset, error) {
	ds := &models.Dataset{}
	err := s.db.QueryRow(
		`SELECT id, name, description, total_size, shard_count, shard_size, created_at, updated_at
		 FROM datasets WHERE id = ?`, id,
	).Scan(&ds.ID, &ds.Name, &ds.Description, &ds.TotalSize, &ds.ShardCount,
		&ds.ShardSize, &ds.CreatedAt, &ds.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying dataset: %w", err)
	}

	shards, err := s.GetShardsByDataset(id)
	if err != nil {
		return nil, err
	}
	ds.Shards = shards

	return ds, nil
}

// ListDatasets returns all datasets.
func (s *Store) ListDatasets() ([]*models.Dataset, error) {
	rows, err := s.db.Query(
		`SELECT id, name, description, total_size, shard_count, shard_size, created_at, updated_at
		 FROM datasets ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("querying datasets: %w", err)
	}
	defer rows.Close()

	var datasets []*models.Dataset
	for rows.Next() {
		ds := &models.Dataset{}
		if err := rows.Scan(&ds.ID, &ds.Name, &ds.Description, &ds.TotalSize,
			&ds.ShardCount, &ds.ShardSize, &ds.CreatedAt, &ds.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scanning dataset row: %w", err)
		}
		datasets = append(datasets, ds)
	}

	return datasets, rows.Err()
}

// DeleteDataset removes a dataset and all its shards and pin statuses.
func (s *Store) DeleteDataset(id string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM pin_statuses WHERE shard_id IN (SELECT id FROM shards WHERE dataset_id = ?)`, id); err != nil {
		return fmt.Errorf("deleting pin statuses: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM shards WHERE dataset_id = ?`, id); err != nil {
		return fmt.Errorf("deleting shards: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM datasets WHERE id = ?`, id); err != nil {
		return fmt.Errorf("deleting dataset: %w", err)
	}

	return tx.Commit()
}

// CreateShard stores a new shard record.
func (s *Store) CreateShard(shard *models.ShardInfo) error {
	_, err := s.db.Exec(
		`INSERT INTO shards (id, dataset_id, shard_index, cid, size, checksum, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		shard.ID, shard.DatasetID, shard.Index, shard.CID, shard.Size, shard.Checksum, shard.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("inserting shard: %w", err)
	}
	return nil
}

// GetShardsByDataset retrieves all shards for a dataset, ordered by index.
func (s *Store) GetShardsByDataset(datasetID string) ([]*models.ShardInfo, error) {
	rows, err := s.db.Query(
		`SELECT id, dataset_id, shard_index, cid, size, checksum, created_at
		 FROM shards WHERE dataset_id = ? ORDER BY shard_index`, datasetID,
	)
	if err != nil {
		return nil, fmt.Errorf("querying shards: %w", err)
	}
	defer rows.Close()

	var shards []*models.ShardInfo
	for rows.Next() {
		si := &models.ShardInfo{}
		if err := rows.Scan(&si.ID, &si.DatasetID, &si.Index, &si.CID,
			&si.Size, &si.Checksum, &si.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning shard row: %w", err)
		}
		shards = append(shards, si)
	}

	return shards, rows.Err()
}

// GetShardByCID retrieves a shard by its IPFS CID.
func (s *Store) GetShardByCID(cid string) (*models.ShardInfo, error) {
	si := &models.ShardInfo{}
	err := s.db.QueryRow(
		`SELECT id, dataset_id, shard_index, cid, size, checksum, created_at
		 FROM shards WHERE cid = ?`, cid,
	).Scan(&si.ID, &si.DatasetID, &si.Index, &si.CID, &si.Size, &si.Checksum, &si.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying shard by CID: %w", err)
	}
	return si, nil
}

// UpsertPinStatus creates or updates the pin status for a shard on a node.
func (s *Store) UpsertPinStatus(ps *models.PinStatus) error {
	_, err := s.db.Exec(
		`INSERT INTO pin_statuses (shard_id, cid, node_id, pinned, checked_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(shard_id, node_id) DO UPDATE SET pinned=?, checked_at=?`,
		ps.ShardID, ps.CID, ps.NodeID, ps.Pinned, ps.CheckedAt,
		ps.Pinned, ps.CheckedAt,
	)
	if err != nil {
		return fmt.Errorf("upserting pin status: %w", err)
	}
	return nil
}

// GetPinStatuses returns all pin statuses for a dataset's shards.
func (s *Store) GetPinStatuses(datasetID string) ([]*models.PinStatus, error) {
	rows, err := s.db.Query(
		`SELECT ps.shard_id, ps.cid, ps.node_id, ps.pinned, ps.checked_at
		 FROM pin_statuses ps
		 JOIN shards s ON ps.shard_id = s.id
		 WHERE s.dataset_id = ?`, datasetID,
	)
	if err != nil {
		return nil, fmt.Errorf("querying pin statuses: %w", err)
	}
	defer rows.Close()

	var statuses []*models.PinStatus
	for rows.Next() {
		ps := &models.PinStatus{}
		if err := rows.Scan(&ps.ShardID, &ps.CID, &ps.NodeID, &ps.Pinned, &ps.CheckedAt); err != nil {
			return nil, fmt.Errorf("scanning pin status row: %w", err)
		}
		statuses = append(statuses, ps)
	}

	return statuses, rows.Err()
}

// GetAllShards returns all shards in the store.
func (s *Store) GetAllShards() ([]*models.ShardInfo, error) {
	rows, err := s.db.Query(
		`SELECT id, dataset_id, shard_index, cid, size, checksum, created_at FROM shards ORDER BY dataset_id, shard_index`,
	)
	if err != nil {
		return nil, fmt.Errorf("querying all shards: %w", err)
	}
	defer rows.Close()

	var shards []*models.ShardInfo
	for rows.Next() {
		si := &models.ShardInfo{}
		if err := rows.Scan(&si.ID, &si.DatasetID, &si.Index, &si.CID, &si.Size, &si.Checksum, &si.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning shard row: %w", err)
		}
		shards = append(shards, si)
	}
	return shards, rows.Err()
}

// CountShards returns the total number of shards.
func (s *Store) CountShards() (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM shards`).Scan(&count)
	return count, err
}

// TotalDataSize returns the total data stored in bytes.
func (s *Store) TotalDataSize() (int64, error) {
	var total sql.NullInt64
	err := s.db.QueryRow(`SELECT SUM(size) FROM shards`).Scan(&total)
	if !total.Valid {
		return 0, err
	}
	return total.Int64, err
}

// GetPinHealthSummary returns summary of pin health per node.
func (s *Store) GetPinHealthSummary() (map[string]int, error) {
	rows, err := s.db.Query(
		`SELECT node_id, COUNT(*) FROM pin_statuses WHERE pinned = 1 GROUP BY node_id`,
	)
	if err != nil {
		return nil, fmt.Errorf("querying pin health: %w", err)
	}
	defer rows.Close()

	result := make(map[string]int)
	for rows.Next() {
		var nodeID string
		var count int
		if err := rows.Scan(&nodeID, &count); err != nil {
			return nil, fmt.Errorf("scanning pin health row: %w", err)
		}
		result[nodeID] = count
	}
	return result, rows.Err()
}

// UpdateDatasetSize updates the total size and shard count for a dataset.
func (s *Store) UpdateDatasetSize(datasetID string, totalSize int64, shardCount int) error {
	_, err := s.db.Exec(
		`UPDATE datasets SET total_size = ?, shard_count = ?, updated_at = ? WHERE id = ?`,
		totalSize, shardCount, time.Now(), datasetID,
	)
	return err
}
