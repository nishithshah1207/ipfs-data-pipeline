// Package config provides configuration management for the pipeline daemon.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for the daemon.
type Config struct {
	Server      ServerConfig
	IPFS        IPFSConfig
	Sharding    ShardingConfig
	Replication ReplicationConfig
	Database    DatabaseConfig
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Host         string
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// IPFSConfig holds IPFS node connection settings.
type IPFSConfig struct {
	Nodes []string // list of IPFS API endpoints
}

// ShardingConfig holds data sharding settings.
type ShardingConfig struct {
	DefaultShardSize int64 // default shard size in bytes
	MaxShardSize     int64
	MinShardSize     int64
}

// ReplicationConfig holds replication monitor settings.
type ReplicationConfig struct {
	MinReplicas   int
	CheckInterval time.Duration
	RepinTimeout  time.Duration
}

// DatabaseConfig holds metadata store settings.
type DatabaseConfig struct {
	Path string
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Host:         "0.0.0.0",
			Port:         8080,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 60 * time.Second,
		},
		IPFS: IPFSConfig{
			Nodes: []string{"http://localhost:5001"},
		},
		Sharding: ShardingConfig{
			DefaultShardSize: 1 * 1024 * 1024, // 1MB
			MaxShardSize:     64 * 1024 * 1024, // 64MB
			MinShardSize:     64 * 1024,         // 64KB
		},
		Replication: ReplicationConfig{
			MinReplicas:   2,
			CheckInterval: 30 * time.Second,
			RepinTimeout:  5 * time.Minute,
		},
		Database: DatabaseConfig{
			Path: "./pipeline.db",
		},
	}
}

// LoadFromEnv loads configuration from environment variables, falling back to defaults.
func LoadFromEnv() *Config {
	cfg := DefaultConfig()

	if v := os.Getenv("PIPELINE_HOST"); v != "" {
		cfg.Server.Host = v
	}
	if v := os.Getenv("PIPELINE_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Server.Port = port
		}
	}
	if v := os.Getenv("IPFS_NODES"); v != "" {
		cfg.IPFS.Nodes = strings.Split(v, ",")
	}
	if v := os.Getenv("SHARD_SIZE"); v != "" {
		if size, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.Sharding.DefaultShardSize = size
		}
	}
	if v := os.Getenv("MIN_REPLICAS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Replication.MinReplicas = n
		}
	}
	if v := os.Getenv("REPLICATION_CHECK_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Replication.CheckInterval = d
		}
	}
	if v := os.Getenv("DB_PATH"); v != "" {
		cfg.Database.Path = v
	}

	return cfg
}
