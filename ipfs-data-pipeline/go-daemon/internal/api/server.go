package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"

	"github.com/ipfs-data-pipeline/go-daemon/internal/ipfs"
	"github.com/ipfs-data-pipeline/go-daemon/internal/metadata"
	"github.com/ipfs-data-pipeline/go-daemon/internal/replication"
	"github.com/ipfs-data-pipeline/go-daemon/internal/shard"
)

// Server is the HTTP API server for the pipeline daemon.
type Server struct {
	httpServer *http.Server
	handlers   *Handlers
}

// NewServer creates and configures a new API server.
func NewServer(
	host string,
	port int,
	readTimeout, writeTimeout time.Duration,
	shardEngine *shard.Engine,
	ipfsClient *ipfs.Client,
	store *metadata.Store,
	monitor *replication.Monitor,
) *Server {
	handlers := NewHandlers(shardEngine, ipfsClient, store, monitor)

	router := mux.NewRouter()
	router.Use(corsMiddleware)
	router.Use(loggingMiddleware)

	// API v1 routes
	api := router.PathPrefix("/api/v1").Subrouter()

	// Ingest
	api.HandleFunc("/ingest", handlers.HandleIngest).Methods("POST")

	// Datasets
	api.HandleFunc("/datasets", handlers.HandleListDatasets).Methods("GET")
	api.HandleFunc("/datasets/{id}", handlers.HandleQuery).Methods("GET")
	api.HandleFunc("/datasets/{id}", handlers.HandleDeleteDataset).Methods("DELETE")
	api.HandleFunc("/datasets/{id}/download", handlers.HandleDownload).Methods("GET")
	api.HandleFunc("/datasets/{id}/replication", handlers.HandleReplicationStatus).Methods("GET")

	// Shards
	api.HandleFunc("/shards/{cid}", handlers.HandleGetShard).Methods("GET")

	// Health & Metrics
	api.HandleFunc("/health", handlers.HandleHealth).Methods("GET")
	api.HandleFunc("/metrics", handlers.HandleMetrics).Methods("GET")

	// Replication
	api.HandleFunc("/replication/check", handlers.HandleReplicationCheck).Methods("POST")

	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", host, port),
		Handler:      router,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}

	return &Server{
		httpServer: srv,
		handlers:   handlers,
	}
}

// Start begins listening for HTTP requests.
func (s *Server) Start() error {
	log.Info().Str("addr", s.httpServer.Addr).Msg("API server starting")
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server error: %w", err)
	}
	return nil
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	log.Info().Msg("API server shutting down")
	return s.httpServer.Shutdown(ctx)
}
