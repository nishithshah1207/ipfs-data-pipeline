"""Benchmarking suite for measuring pipeline throughput, latency, and shard performance."""

import json
import os
import statistics
import tempfile
import time
from dataclasses import asdict, dataclass, field

from .client import PipelineClient


@dataclass
class BenchmarkResult:
    """Results from a single benchmark run."""

    test_name: str
    file_size_bytes: int
    shard_size_bytes: int
    shard_count: int
    ingest_duration_s: float
    ingest_throughput_mbps: float
    download_duration_s: float
    download_throughput_mbps: float
    query_latency_ms: float
    delete_duration_s: float


@dataclass
class BenchmarkSuite:
    """Complete benchmark suite results."""

    timestamp: str
    results: list[BenchmarkResult] = field(default_factory=list)
    summary: dict = field(default_factory=dict)


def run_benchmarks(
    api_url: str = "http://localhost:8080",
    sizes: list[int] | None = None,
    shard_sizes: list[int] | None = None,
    iterations: int = 3,
) -> BenchmarkSuite:
    """Run the full benchmark suite.

    Args:
        api_url: Pipeline daemon URL.
        sizes: List of file sizes to test (bytes). Default: [100KB, 1MB, 10MB, 50MB].
        shard_sizes: List of shard sizes to test (bytes). Default: [64KB, 256KB, 1MB].
        iterations: Number of iterations per test case.

    Returns:
        BenchmarkSuite with all results and summary statistics.
    """
    if sizes is None:
        sizes = [
            100 * 1024,       # 100KB
            1024 * 1024,      # 1MB
            10 * 1024 * 1024, # 10MB
            50 * 1024 * 1024, # 50MB
        ]
    if shard_sizes is None:
        shard_sizes = [
            64 * 1024,        # 64KB
            256 * 1024,       # 256KB
            1024 * 1024,      # 1MB
        ]

    client = PipelineClient(base_url=api_url, timeout=300)
    suite = BenchmarkSuite(timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))

    for file_size in sizes:
        for shard_size in shard_sizes:
            if shard_size > file_size:
                continue

            ingest_times = []
            download_times = []
            query_times = []
            delete_times = []
            shard_count = 0

            for i in range(iterations):
                # Generate test data
                test_data = os.urandom(file_size)

                # --- Ingest benchmark ---
                start = time.time()
                result = client.ingest_bytes(
                    data=test_data,
                    name=f"bench-{file_size}-{shard_size}-{i}",
                    description="benchmark test data",
                    shard_size=shard_size,
                )
                ingest_times.append(time.time() - start)
                shard_count = result.shard_count

                # --- Query benchmark ---
                start = time.time()
                client.get_dataset(result.dataset_id)
                query_times.append(time.time() - start)

                # --- Download benchmark ---
                with tempfile.NamedTemporaryFile(delete=True) as tmp:
                    start = time.time()
                    client.download_dataset(result.dataset_id, tmp.name)
                    download_times.append(time.time() - start)

                # --- Delete benchmark ---
                start = time.time()
                client.delete_dataset(result.dataset_id)
                delete_times.append(time.time() - start)

            avg_ingest = statistics.mean(ingest_times)
            avg_download = statistics.mean(download_times)
            avg_query = statistics.mean(query_times)
            avg_delete = statistics.mean(delete_times)

            bench_result = BenchmarkResult(
                test_name=f"{_fmt_size(file_size)}_shard_{_fmt_size(shard_size)}",
                file_size_bytes=file_size,
                shard_size_bytes=shard_size,
                shard_count=shard_count,
                ingest_duration_s=round(avg_ingest, 4),
                ingest_throughput_mbps=round(file_size / avg_ingest / (1024 * 1024), 2),
                download_duration_s=round(avg_download, 4),
                download_throughput_mbps=round(file_size / avg_download / (1024 * 1024), 2),
                query_latency_ms=round(avg_query * 1000, 2),
                delete_duration_s=round(avg_delete, 4),
            )
            suite.results.append(bench_result)

            print(
                f"  {bench_result.test_name}: "
                f"ingest={avg_ingest:.3f}s ({bench_result.ingest_throughput_mbps:.1f} MB/s), "
                f"download={avg_download:.3f}s ({bench_result.download_throughput_mbps:.1f} MB/s), "
                f"query={bench_result.query_latency_ms:.1f}ms"
            )

    # Compute summary
    if suite.results:
        suite.summary = {
            "avg_ingest_throughput_mbps": round(
                statistics.mean(r.ingest_throughput_mbps for r in suite.results), 2
            ),
            "max_ingest_throughput_mbps": round(
                max(r.ingest_throughput_mbps for r in suite.results), 2
            ),
            "avg_download_throughput_mbps": round(
                statistics.mean(r.download_throughput_mbps for r in suite.results), 2
            ),
            "max_download_throughput_mbps": round(
                max(r.download_throughput_mbps for r in suite.results), 2
            ),
            "avg_query_latency_ms": round(
                statistics.mean(r.query_latency_ms for r in suite.results), 2
            ),
            "p99_query_latency_ms": round(
                sorted(r.query_latency_ms for r in suite.results)[
                    int(len(suite.results) * 0.99)
                ],
                2,
            ),
            "total_tests": len(suite.results),
            "iterations_per_test": iterations,
        }

    return suite


def save_results(suite: BenchmarkSuite, output_path: str):
    """Save benchmark results to a JSON file."""
    data = {
        "timestamp": suite.timestamp,
        "results": [asdict(r) for r in suite.results],
        "summary": suite.summary,
    }
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Results saved to {output_path}")


def print_results_table(suite: BenchmarkSuite):
    """Print benchmark results as a formatted table."""
    try:
        from rich.console import Console
        from rich.table import Table

        console = Console()

        table = Table(title="Benchmark Results")
        table.add_column("Test", style="cyan")
        table.add_column("File Size", justify="right")
        table.add_column("Shard Size", justify="right")
        table.add_column("Shards", justify="right")
        table.add_column("Ingest (MB/s)", justify="right", style="green")
        table.add_column("Download (MB/s)", justify="right", style="blue")
        table.add_column("Query (ms)", justify="right", style="yellow")

        for r in suite.results:
            table.add_row(
                r.test_name,
                _fmt_size(r.file_size_bytes),
                _fmt_size(r.shard_size_bytes),
                str(r.shard_count),
                f"{r.ingest_throughput_mbps:.1f}",
                f"{r.download_throughput_mbps:.1f}",
                f"{r.query_latency_ms:.1f}",
            )

        console.print(table)

        if suite.summary:
            console.print()
            console.print("[bold]Summary:[/bold]")
            console.print(
                f"  Avg ingest throughput:  [green]{suite.summary['avg_ingest_throughput_mbps']:.1f} MB/s[/green]"
            )
            console.print(
                f"  Max ingest throughput:  [green]{suite.summary['max_ingest_throughput_mbps']:.1f} MB/s[/green]"
            )
            console.print(
                f"  Avg download throughput: [blue]{suite.summary['avg_download_throughput_mbps']:.1f} MB/s[/blue]"
            )
            console.print(
                f"  Avg query latency:     [yellow]{suite.summary['avg_query_latency_ms']:.1f} ms[/yellow]"
            )

    except ImportError:
        # Fallback without rich
        print("\n=== Benchmark Results ===")
        for r in suite.results:
            print(
                f"{r.test_name}: ingest={r.ingest_throughput_mbps:.1f} MB/s, "
                f"download={r.download_throughput_mbps:.1f} MB/s, "
                f"query={r.query_latency_ms:.1f}ms"
            )


def _fmt_size(size_bytes: int) -> str:
    if size_bytes == 0:
        return "0B"
    units = ["B", "KB", "MB", "GB"]
    i = 0
    size = float(size_bytes)
    while size >= 1024 and i < len(units) - 1:
        size /= 1024
        i += 1
    return f"{size:.0f}{units[i]}"


if __name__ == "__main__":
    import sys

    url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
    print(f"Running benchmarks against {url}...")
    suite = run_benchmarks(api_url=url)
    print_results_table(suite)
    save_results(suite, "benchmark_results.json")
