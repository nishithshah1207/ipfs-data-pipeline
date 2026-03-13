"""Command-line interface for the IPFS data pipeline client."""

import os
import sys

import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from .client import PipelineClient, PipelineError

console = Console()


def get_client(ctx: click.Context) -> PipelineClient:
    return PipelineClient(base_url=ctx.obj["base_url"], timeout=ctx.obj["timeout"])


@click.group()
@click.option(
    "--api-url",
    default="http://localhost:8080",
    envvar="PIPELINE_API_URL",
    help="Pipeline daemon API URL.",
)
@click.option("--timeout", default=120, help="Request timeout in seconds.")
@click.pass_context
def cli(ctx: click.Context, api_url: str, timeout: int):
    """IPFS Data Pipeline CLI - Ingest, query, and manage distributed datasets."""
    ctx.ensure_object(dict)
    ctx.obj["base_url"] = api_url
    ctx.obj["timeout"] = timeout


# ──────────────────────────── Ingest ────────────────────────────


@cli.command()
@click.argument("file_path", type=click.Path(exists=True))
@click.option("--name", "-n", default=None, help="Dataset name (defaults to filename).")
@click.option("--description", "-d", default="", help="Dataset description.")
@click.option(
    "--shard-size",
    "-s",
    default=0,
    type=int,
    help="Shard size in bytes (0 = server default, e.g. 1048576 for 1MB).",
)
@click.pass_context
def ingest(ctx: click.Context, file_path: str, name: str, description: str, shard_size: int):
    """Ingest a file into the distributed data pipeline."""
    client = get_client(ctx)
    file_size = os.path.getsize(file_path)

    console.print(f"[bold blue]Ingesting[/bold blue] {file_path} ({_fmt_size(file_size)})")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        progress.add_task("Uploading and sharding...", total=None)
        try:
            result = client.ingest_file(
                file_path=file_path,
                name=name,
                description=description,
                shard_size=shard_size,
            )
        except PipelineError as e:
            console.print(f"[bold red]Error:[/bold red] {e}")
            sys.exit(1)

    throughput = result.total_size / result.duration_seconds / (1024 * 1024)

    console.print()
    console.print("[bold green]Ingestion complete![/bold green]")
    console.print(f"  Dataset ID:  [cyan]{result.dataset_id}[/cyan]")
    console.print(f"  Name:        {result.name}")
    console.print(f"  Total size:  {_fmt_size(result.total_size)}")
    console.print(f"  Shards:      {result.shard_count}")
    console.print(f"  Duration:    {result.duration_seconds:.2f}s")
    console.print(f"  Throughput:  {throughput:.2f} MB/s")

    if result.shards:
        console.print()
        table = Table(title="Shards")
        table.add_column("Index", style="cyan")
        table.add_column("CID", style="green")
        table.add_column("Size", justify="right")
        for s in result.shards:
            table.add_row(str(s.index), s.cid[:20] + "...", _fmt_size(s.size))
        console.print(table)


# ──────────────────────────── Query ────────────────────────────


@cli.command()
@click.argument("dataset_id")
@click.pass_context
def query(ctx: click.Context, dataset_id: str):
    """Query a dataset by ID and display its metadata."""
    client = get_client(ctx)

    try:
        ds = client.get_dataset(dataset_id)
    except PipelineError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)

    console.print(f"[bold]Dataset:[/bold] {ds.name}")
    console.print(f"  ID:          [cyan]{ds.id}[/cyan]")
    console.print(f"  Description: {ds.description}")
    console.print(f"  Total size:  {_fmt_size(ds.total_size)}")
    console.print(f"  Shards:      {ds.shard_count}")
    console.print(f"  Shard size:  {_fmt_size(ds.shard_size)}")
    console.print(f"  Created:     {ds.created_at}")

    if ds.shards:
        console.print()
        table = Table(title="Shards")
        table.add_column("Index", style="cyan")
        table.add_column("CID", style="green")
        table.add_column("Size", justify="right")
        table.add_column("Checksum", style="dim")
        for s in ds.shards:
            table.add_row(
                str(s.index),
                s.cid,
                _fmt_size(s.size),
                s.checksum[:16] + "...",
            )
        console.print(table)


# ──────────────────────────── Download ────────────────────────────


@cli.command()
@click.argument("dataset_id")
@click.option("--output", "-o", required=True, help="Output file path.")
@click.pass_context
def download(ctx: click.Context, dataset_id: str, output: str):
    """Download and reconstruct a dataset from IPFS."""
    client = get_client(ctx)

    console.print(f"[bold blue]Downloading[/bold blue] dataset {dataset_id}...")

    try:
        import time

        start = time.time()
        size = client.download_dataset(dataset_id, output)
        duration = time.time() - start
    except PipelineError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)

    throughput = size / duration / (1024 * 1024) if duration > 0 else 0

    console.print(f"[bold green]Download complete![/bold green]")
    console.print(f"  Output:     {output}")
    console.print(f"  Size:       {_fmt_size(size)}")
    console.print(f"  Duration:   {duration:.2f}s")
    console.print(f"  Throughput: {throughput:.2f} MB/s")


# ──────────────────────────── List ────────────────────────────


@cli.command("list")
@click.pass_context
def list_datasets(ctx: click.Context):
    """List all datasets in the pipeline."""
    client = get_client(ctx)

    try:
        datasets = client.list_datasets()
    except PipelineError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)

    if not datasets:
        console.print("[dim]No datasets found.[/dim]")
        return

    table = Table(title=f"Datasets ({len(datasets)} total)")
    table.add_column("ID", style="cyan")
    table.add_column("Name", style="bold")
    table.add_column("Size", justify="right")
    table.add_column("Shards", justify="right")
    table.add_column("Created")

    for ds in datasets:
        table.add_row(
            ds.id[:16],
            ds.name,
            _fmt_size(ds.total_size),
            str(ds.shard_count),
            ds.created_at[:19] if ds.created_at else "",
        )

    console.print(table)


# ──────────────────────────── Delete ────────────────────────────


@cli.command()
@click.argument("dataset_id")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation.")
@click.pass_context
def delete(ctx: click.Context, dataset_id: str, yes: bool):
    """Delete a dataset and unpin its shards."""
    client = get_client(ctx)

    if not yes:
        click.confirm(f"Delete dataset {dataset_id}?", abort=True)

    try:
        result = client.delete_dataset(dataset_id)
    except PipelineError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)

    console.print(f"[bold green]Deleted[/bold green] dataset {result.get('dataset_id', dataset_id)}")


# ──────────────────────────── Replication ────────────────────────────


@cli.command()
@click.argument("dataset_id")
@click.pass_context
def replication(ctx: click.Context, dataset_id: str):
    """Check replication status of a dataset."""
    client = get_client(ctx)

    try:
        status = client.get_replication_status(dataset_id)
    except PipelineError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)

    console.print(f"[bold]Replication Status:[/bold] {dataset_id}")
    console.print(f"  Total shards:      {status.total_shards}")
    console.print(f"  Fully replicated:  [green]{status.fully_replicated}[/green]")
    console.print(f"  Under replicated:  [yellow]{status.under_replicated}[/yellow]")
    console.print(f"  Missing:           [red]{status.missing}[/red]")


# ──────────────────────────── Health ────────────────────────────


@cli.command()
@click.pass_context
def health(ctx: click.Context):
    """Check pipeline system health."""
    client = get_client(ctx)

    try:
        h = client.health()
    except PipelineError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)

    status_color = "green" if h["status"] == "healthy" else "red"
    console.print(f"[bold]Status:[/bold] [{status_color}]{h['status']}[/{status_color}]")
    console.print(f"  Uptime:       {h.get('uptime', 'N/A')}")
    console.print(f"  Total shards: {h.get('total_shards', 0)}")
    console.print(f"  Pin ratio:    {h.get('pinned_ratio', 0):.1%}")

    if h.get("ipfs_nodes"):
        console.print()
        table = Table(title="IPFS Nodes")
        table.add_column("Peer ID", style="cyan")
        table.add_column("Address")
        table.add_column("Status")
        table.add_column("Pins", justify="right")

        for node in h["ipfs_nodes"]:
            status = "[green]online[/green]" if node["reachable"] else "[red]offline[/red]"
            table.add_row(
                node.get("id", "")[:16] + "..." if node.get("id") else "N/A",
                node["address"],
                status,
                str(node.get("pin_count", 0)),
            )
        console.print(table)


# ──────────────────────────── Helpers ────────────────────────────


def _fmt_size(size_bytes: int) -> str:
    """Format bytes into human-readable size."""
    if size_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    size = float(size_bytes)
    while size >= 1024 and i < len(units) - 1:
        size /= 1024
        i += 1
    return f"{size:.1f} {units[i]}"


if __name__ == "__main__":
    cli()
