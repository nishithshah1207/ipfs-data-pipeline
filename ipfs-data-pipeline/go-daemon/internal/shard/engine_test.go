package shard

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestShardBasic(t *testing.T) {
	engine := NewEngine(1024, 64, 1024*1024)

	data := make([]byte, 4096)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	shards, totalSize, err := engine.Shard(bytes.NewReader(data), 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(shards) != 4 {
		t.Errorf("expected 4 shards, got %d", len(shards))
	}
	if totalSize != 4096 {
		t.Errorf("expected total size 4096, got %d", totalSize)
	}

	// Verify each shard
	for i, s := range shards {
		if s.Index != i {
			t.Errorf("shard %d has index %d", i, s.Index)
		}
		if s.Size != 1024 {
			t.Errorf("shard %d has size %d, expected 1024", i, s.Size)
		}
		if s.Checksum == "" {
			t.Errorf("shard %d has empty checksum", i)
		}
	}
}

func TestShardUnevenSize(t *testing.T) {
	engine := NewEngine(1024, 64, 1024*1024)

	data := make([]byte, 3000)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	shards, totalSize, err := engine.Shard(bytes.NewReader(data), 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(shards) != 3 {
		t.Errorf("expected 3 shards, got %d", len(shards))
	}
	if totalSize != 3000 {
		t.Errorf("expected total size 3000, got %d", totalSize)
	}

	// Last shard should be smaller
	if shards[2].Size != 952 {
		t.Errorf("last shard size should be 952, got %d", shards[2].Size)
	}
}

func TestShardEmptyReader(t *testing.T) {
	engine := NewEngine(1024, 64, 1024*1024)

	shards, totalSize, err := engine.Shard(bytes.NewReader([]byte{}), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(shards) != 0 {
		t.Errorf("expected 0 shards, got %d", len(shards))
	}
	if totalSize != 0 {
		t.Errorf("expected total size 0, got %d", totalSize)
	}
}

func TestShardDefaultSize(t *testing.T) {
	engine := NewEngine(512, 64, 1024*1024)

	data := make([]byte, 1536)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	shards, _, err := engine.Shard(bytes.NewReader(data), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(shards) != 3 {
		t.Errorf("expected 3 shards with default size 512, got %d", len(shards))
	}
}

func TestShardChecksumConsistency(t *testing.T) {
	engine := NewEngine(1024, 64, 1024*1024)

	data := []byte("hello world, this is test data for checksum verification")

	shards1, _, _ := engine.Shard(bytes.NewReader(data), 0)
	shards2, _, _ := engine.Shard(bytes.NewReader(data), 0)

	if shards1[0].Checksum != shards2[0].Checksum {
		t.Error("identical data should produce identical checksums")
	}
}

func BenchmarkShard1MB(b *testing.B) {
	engine := NewEngine(64*1024, 1024, 1024*1024)
	data := make([]byte, 1024*1024)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		engine.Shard(bytes.NewReader(data), 64*1024)
	}
}

func BenchmarkShard100MB(b *testing.B) {
	engine := NewEngine(1024*1024, 1024, 64*1024*1024)
	data := make([]byte, 100*1024*1024)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		engine.Shard(bytes.NewReader(data), 1024*1024)
	}
}
