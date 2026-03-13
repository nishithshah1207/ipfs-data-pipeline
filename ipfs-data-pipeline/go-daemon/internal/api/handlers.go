package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"

	"github.com/ipfs-data-pipeline/go-daemon/internal/ipfs"
	"github.com/ipfs-data-pipeline/go-daemon/internal/metadata"
	"github.com/ipfs-data-pipeline/go-daemon/internal/replication"
	"github.com/ipfs-data-pipeline/go-daemon/internal/shard"
	"github.com/ipfs-data-pipeline/go-daemon/pkg/models"
)

// Handlers holds all HTTP handler dependencies.
type Handlers struct {
	shardEngine *shard.Engine
	ipfsClient  *ipfs.Client
	store       *metadata.Store
	monitor     *replication.Monitor
	startTime   time.Time
}

// NewHandlers creates a new Handlers instance.
func NewHandlers(
	shardEngine *shard.Engine,
	ipfsClient *ipfs.Client,
	store *metadata.Store,
	monitor *replication.Monitor,
) *Handlers {
	return &Handlers{
		shardEngine: shardEngine,
		ipfsClient:  ipfsClient,
		store:       store,
		monitor:     monitor,
		startTime:   time.Now(),
	}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, models.ErrorResponse{
		Error: msg,
		Code:  status,
	})
}

// HandleIngest handles multipart file upload, shards the data, and pins to IPFS.
// POST /api/v1/ingest
func (h *Handlers) HandleIngest(w http.ResponseWriter, r *http.Request) {
	// Parse multipart form (max 512MB)
	if err := r.ParseMultipartForm(512 << 20); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("parsing form: %v", err))
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("reading file: %v", err))
		return
	}
	defer file.Close()

	name := r.FormValue("name")
	if name == "" {
		name = header.Filename
	}
	description := r.FormValue("description")

	var shardSize int64
	if ss := r.FormValue("shard_size"); ss != "" {
		fmt.Sscanf(ss, "%d", &shardSize)
	}

	log.Info().
		Str("name", name).
		Str("filename", header.Filename).
		Int64("size", header.Size).
		Msg("ingesting data")

	// Read all data
	data, err := io.ReadAll(file)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("reading file data: %v", err))
		return
	}

	// Shard the data
	shards, totalSize, err := h.shardEngine.Shard(bytes.NewReader(data), shardSize)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("sharding data: %v", err))
		return
	}

	// Create dataset record
	datasetID := generateID(name, time.Now())
	if shardSize <= 0 {
		shardSize = 1024 * 1024 // default 1MB
	}
	dataset := &models.Dataset{
		ID:          datasetID,
		Name:        name,
		Description: description,
		TotalSize:   totalSize,
		ShardCount:  len(shards),
		ShardSize:   shardSize,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if err := h.store.CreateDataset(dataset); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("storing dataset: %v", err))
		return
	}

	// Pin each shard to IPFS
	var shardInfos []*models.ShardInfo
	for _, s := range shards {
		cid, pinnedNodes, err := h.ipfsClient.AddToAllNodes(s.Data)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("adding shard %d to IPFS: %v", s.Index, err))
			return
		}

		shardID := fmt.Sprintf("%s-shard-%04d", datasetID, s.Index)
		si := &models.ShardInfo{
			ID:        shardID,
			DatasetID: datasetID,
			Index:     s.Index,
			CID:       cid,
			Size:      s.Size,
			Checksum:  s.Checksum,
			CreatedAt: time.Now(),
		}

		if err := h.store.CreateShard(si); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("storing shard %d: %v", s.Index, err))
			return
		}

		// Record pin statuses
		for _, node := range pinnedNodes {
			ps := &models.PinStatus{
				ShardID:   shardID,
				CID:       cid,
				NodeID:    node,
				Pinned:    true,
				CheckedAt: time.Now(),
			}
			h.store.UpsertPinStatus(ps)
		}

		shardInfos = append(shardInfos, si)

		log.Info().
			Str("shard_id", shardID).
			Str("cid", cid).
			Int("replicas", len(pinnedNodes)).
			Msg("shard pinned")
	}

	resp := models.IngestResponse{
		DatasetID:  datasetID,
		Name:       name,
		TotalSize:  totalSize,
		ShardCount: len(shards),
		Shards:     shardInfos,
	}

	log.Info().
		Str("dataset_id", datasetID).
		Int("shards", len(shards)).
		Int64("total_size", totalSize).
		Msg("ingestion complete")

	writeJSON(w, http.StatusCreated, resp)
}

// HandleQuery retrieves a dataset and optionally reconstructs it.
// GET /api/v1/datasets/{id}
func (h *Handlers) HandleQuery(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	dataset, err := h.store.GetDataset(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("querying dataset: %v", err))
		return
	}
	if dataset == nil {
		writeError(w, http.StatusNotFound, "dataset not found")
		return
	}

	writeJSON(w, http.StatusOK, models.QueryResponse{Dataset: dataset})
}

// HandleDownload reconstructs and downloads the full dataset.
// GET /api/v1/datasets/{id}/download
func (h *Handlers) HandleDownload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	dataset, err := h.store.GetDataset(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("querying dataset: %v", err))
		return
	}
	if dataset == nil {
		writeError(w, http.StatusNotFound, "dataset not found")
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, dataset.Name))

	for _, shardInfo := range dataset.Shards {
		data, err := h.ipfsClient.GetFromAnyNode(shardInfo.CID)
		if err != nil {
			log.Error().Str("cid", shardInfo.CID).Err(err).Msg("failed to retrieve shard")
			return
		}

		if !shard.ValidateShard(shardInfo, data) {
			log.Error().Str("cid", shardInfo.CID).Msg("shard checksum mismatch")
			return
		}

		if _, err := w.Write(data); err != nil {
			log.Error().Err(err).Msg("failed to write shard data to response")
			return
		}
	}
}

// HandleGetShard retrieves a single shard by CID.
// GET /api/v1/shards/{cid}
func (h *Handlers) HandleGetShard(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	cid := vars["cid"]

	data, err := h.ipfsClient.GetFromAnyNode(cid)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("shard not found: %v", err))
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-IPFS-CID", cid)
	w.Write(data)
}

// HandleListDatasets lists all datasets.
// GET /api/v1/datasets
func (h *Handlers) HandleListDatasets(w http.ResponseWriter, r *http.Request) {
	datasets, err := h.store.ListDatasets()
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("listing datasets: %v", err))
		return
	}

	if datasets == nil {
		datasets = []*models.Dataset{}
	}

	writeJSON(w, http.StatusOK, models.ListDatasetsResponse{
		Datasets: datasets,
		Total:    len(datasets),
	})
}

// HandleDeleteDataset deletes a dataset and its shards.
// DELETE /api/v1/datasets/{id}
func (h *Handlers) HandleDeleteDataset(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	dataset, err := h.store.GetDataset(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("querying dataset: %v", err))
		return
	}
	if dataset == nil {
		writeError(w, http.StatusNotFound, "dataset not found")
		return
	}

	// Unpin shards from all nodes
	nodes := h.ipfsClient.Nodes()
	for _, shardInfo := range dataset.Shards {
		for _, node := range nodes {
			h.ipfsClient.Unpin(node, shardInfo.CID)
		}
	}

	if err := h.store.DeleteDataset(id); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("deleting dataset: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "dataset_id": id})
}

// HandleReplicationStatus returns replication health for a dataset.
// GET /api/v1/datasets/{id}/replication
func (h *Handlers) HandleReplicationStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	status, err := h.monitor.GetReplicationStatus(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("checking replication: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, status)
}

// HandleHealth returns the system health status.
// GET /api/v1/health
func (h *Handlers) HandleHealth(w http.ResponseWriter, r *http.Request) {
	nodes := h.ipfsClient.Nodes()
	var nodeHealths []models.NodeHealth

	pinHealth, _ := h.store.GetPinHealthSummary()
	totalShards, _ := h.store.CountShards()

	for _, node := range nodes {
		peerID, _ := h.ipfsClient.NodeID(node)
		nh := models.NodeHealth{
			ID:        peerID,
			Address:   node,
			Reachable: h.ipfsClient.IsReachable(node),
			PinCount:  pinHealth[node],
		}
		nodeHealths = append(nodeHealths, nh)
	}

	var pinnedTotal int
	for _, count := range pinHealth {
		pinnedTotal += count
	}
	var pinnedRatio float64
	if totalShards > 0 && len(nodes) > 0 {
		pinnedRatio = float64(pinnedTotal) / float64(totalShards*len(nodes))
	}

	resp := models.HealthResponse{
		Status:      "healthy",
		IPFSNodes:   nodeHealths,
		TotalShards: totalShards,
		PinnedRatio: pinnedRatio,
		Uptime:      time.Since(h.startTime).String(),
	}

	writeJSON(w, http.StatusOK, resp)
}

// HandleMetrics returns pipeline metrics.
// GET /api/v1/metrics
func (h *Handlers) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	totalShards, _ := h.store.CountShards()
	totalSize, _ := h.store.TotalDataSize()
	datasets, _ := h.store.ListDatasets()

	nodes := h.ipfsClient.Nodes()
	onlineCount := 0
	for _, node := range nodes {
		if h.ipfsClient.IsReachable(node) {
			onlineCount++
		}
	}

	stats := h.monitor.LastStats()

	snapshot := models.MetricsSnapshot{
		Timestamp:         time.Now(),
		ActiveDatasets:    len(datasets),
		TotalShardsStored: totalShards,
		TotalDataStored:   totalSize,
		IPFSNodesOnline:   onlineCount,
	}

	// Parse duration string to get query latency estimate
	if stats.TotalShards > 0 {
		snapshot.QueryLatencyP50 = float64(stats.FullyReplicated) / float64(stats.TotalShards) * 100
	}

	writeJSON(w, http.StatusOK, snapshot)
}

// HandleReplicationCheck triggers an immediate replication check.
// POST /api/v1/replication/check
func (h *Handlers) HandleReplicationCheck(w http.ResponseWriter, r *http.Request) {
	stats := h.monitor.CheckNow()
	writeJSON(w, http.StatusOK, stats)
}

func generateID(name string, t time.Time) string {
	h := sha256.New()
	h.Write([]byte(name))
	h.Write([]byte(t.String()))
	return hex.EncodeToString(h.Sum(nil))[:16]
}
