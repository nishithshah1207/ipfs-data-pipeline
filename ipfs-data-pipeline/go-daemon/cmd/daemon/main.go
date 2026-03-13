// Pipeline daemon - the core service for the IPFS-backed distributed data pipeline.
// It exposes an HTTP API for ingesting data, querying datasets, and monitoring
// replication health across multiple IPFS nodes.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/ipfs-data-pipeline/go-daemon/internal/api"
	"github.com/ipfs-data-pipeline/go-daemon/internal/config"
	"github.com/ipfs-data-pipeline/go-daemon/internal/ipfs"
	"github.com/ipfs-data-pipeline/go-daemon/internal/metadata"
	"github.com/ipfs-data-pipeline/go-daemon/internal/replication"
	"github.com/ipfs-data-pipeline/go-daemon/internal/shard"
)

func main() {
	// Configure logging
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().Msg("IPFS Data Pipeline Daemon starting...")

	// Load configuration
	cfg := config.LoadFromEnv()

	log.Info().
		Int("port", cfg.Server.Port).
		Strs("ipfs_nodes", cfg.IPFS.Nodes).
		Int64("default_shard_size", cfg.Sharding.DefaultShardSize).
		Int("min_replicas", cfg.Replication.MinReplicas).
		Msg("configuration loaded")

	// Initialize components
	shardEngine := shard.NewEngine(
		cfg.Sharding.DefaultShardSize,
		cfg.Sharding.MinShardSize,
		cfg.Sharding.MaxShardSize,
	)

	ipfsClient := ipfs.NewClient(cfg.IPFS.Nodes)

	store, err := metadata.NewStore(cfg.Database.Path)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize metadata store")
	}
	defer store.Close()

	monitor := replication.NewMonitor(
		ipfsClient,
		store,
		cfg.Replication.MinReplicas,
		cfg.Replication.CheckInterval,
	)

	// Start replication monitor
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	monitor.Start(ctx)
	defer monitor.Stop()

	// Start API server
	server := api.NewServer(
		cfg.Server.Host,
		cfg.Server.Port,
		cfg.Server.ReadTimeout,
		cfg.Server.WriteTimeout,
		shardEngine,
		ipfsClient,
		store,
		monitor,
	)

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigChan
		log.Info().Str("signal", sig.String()).Msg("shutting down...")
		cancel()
		server.Shutdown(context.Background())
	}()

	if err := server.Start(); err != nil {
		log.Fatal().Err(err).Msg("server failed")
	}
}
