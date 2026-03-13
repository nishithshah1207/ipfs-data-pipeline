"""Core HTTP client for communicating with the pipeline daemon."""

import time
from dataclasses import dataclass, field
from typing import Optional

import requests


@dataclass
class ShardInfo:
    """Represents a single shard stored on IPFS."""

    id: str
    dataset_id: str
    index: int
    cid: str
    size: int
    checksum: str
    created_at: str


@dataclass
class Dataset:
    """Represents a dataset composed of IPFS-pinned shards."""

    id: str
    name: str
    description: str
    total_size: int
    shard_count: int
    shard_size: int
    created_at: str
    updated_at: str
    shards: list[ShardInfo] = field(default_factory=list)


@dataclass
class IngestResult:
    """Result of a data ingestion operation."""

    dataset_id: str
    name: str
    total_size: int
    shard_count: int
    shards: list[ShardInfo]
    duration_seconds: float


@dataclass
class ReplicationStatus:
    """Replication health status for a dataset."""

    dataset_id: str
    total_shards: int
    fully_replicated: int
    under_replicated: int
    missing: int


class PipelineClient:
    """HTTP client for the IPFS data pipeline daemon API."""

    def __init__(self, base_url: str = "http://localhost:8080", timeout: int = 120):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _url(self, path: str) -> str:
        return f"{self.base_url}/api/v1{path}"

    def _check_response(self, resp: requests.Response) -> dict:
        if resp.status_code >= 400:
            try:
                error_data = resp.json()
                msg = error_data.get("error", resp.text)
            except Exception:
                msg = resp.text
            raise PipelineError(f"API error ({resp.status_code}): {msg}")
        return resp.json()

    # --- Ingest ---

    def ingest_file(
        self,
        file_path: str,
        name: Optional[str] = None,
        description: str = "",
        shard_size: int = 0,
    ) -> IngestResult:
        """Ingest a file into the pipeline.

        Args:
            file_path: Path to the file to ingest.
            name: Optional dataset name (defaults to filename).
            description: Optional dataset description.
            shard_size: Shard size in bytes (0 = server default).

        Returns:
            IngestResult with dataset ID and shard information.
        """
        import os

        if name is None:
            name = os.path.basename(file_path)

        start = time.time()

        with open(file_path, "rb") as f:
            files = {"file": (name, f)}
            data = {"name": name, "description": description}
            if shard_size > 0:
                data["shard_size"] = str(shard_size)

            resp = self.session.post(
                self._url("/ingest"),
                files=files,
                data=data,
                timeout=self.timeout,
            )

        result = self._check_response(resp)
        duration = time.time() - start

        shards = [
            ShardInfo(
                id=s["id"],
                dataset_id=result["dataset_id"],
                index=s["index"],
                cid=s["cid"],
                size=s["size"],
                checksum=s["checksum"],
                created_at=s.get("created_at", ""),
            )
            for s in result.get("shards", [])
        ]

        return IngestResult(
            dataset_id=result["dataset_id"],
            name=result["name"],
            total_size=result["total_size"],
            shard_count=result["shard_count"],
            shards=shards,
            duration_seconds=duration,
        )

    def ingest_bytes(
        self,
        data: bytes,
        name: str,
        description: str = "",
        shard_size: int = 0,
    ) -> IngestResult:
        """Ingest raw bytes into the pipeline."""
        start = time.time()

        files = {"file": (name, data)}
        form_data = {"name": name, "description": description}
        if shard_size > 0:
            form_data["shard_size"] = str(shard_size)

        resp = self.session.post(
            self._url("/ingest"),
            files=files,
            data=form_data,
            timeout=self.timeout,
        )

        result = self._check_response(resp)
        duration = time.time() - start

        shards = [
            ShardInfo(
                id=s["id"],
                dataset_id=result["dataset_id"],
                index=s["index"],
                cid=s["cid"],
                size=s["size"],
                checksum=s["checksum"],
                created_at=s.get("created_at", ""),
            )
            for s in result.get("shards", [])
        ]

        return IngestResult(
            dataset_id=result["dataset_id"],
            name=result["name"],
            total_size=result["total_size"],
            shard_count=result["shard_count"],
            shards=shards,
            duration_seconds=duration,
        )

    # --- Query ---

    def get_dataset(self, dataset_id: str) -> Dataset:
        """Retrieve dataset metadata and shard information."""
        resp = self.session.get(
            self._url(f"/datasets/{dataset_id}"),
            timeout=self.timeout,
        )
        result = self._check_response(resp)
        ds = result["dataset"]

        shards = [
            ShardInfo(
                id=s["id"],
                dataset_id=ds["id"],
                index=s["index"],
                cid=s["cid"],
                size=s["size"],
                checksum=s["checksum"],
                created_at=s.get("created_at", ""),
            )
            for s in ds.get("shards", [])
        ]

        return Dataset(
            id=ds["id"],
            name=ds["name"],
            description=ds.get("description", ""),
            total_size=ds["total_size"],
            shard_count=ds["shard_count"],
            shard_size=ds.get("shard_size", 0),
            created_at=ds.get("created_at", ""),
            updated_at=ds.get("updated_at", ""),
            shards=shards,
        )

    def list_datasets(self) -> list[Dataset]:
        """List all datasets in the pipeline."""
        resp = self.session.get(
            self._url("/datasets"),
            timeout=self.timeout,
        )
        result = self._check_response(resp)

        datasets = []
        for ds in result.get("datasets", []):
            datasets.append(
                Dataset(
                    id=ds["id"],
                    name=ds["name"],
                    description=ds.get("description", ""),
                    total_size=ds["total_size"],
                    shard_count=ds["shard_count"],
                    shard_size=ds.get("shard_size", 0),
                    created_at=ds.get("created_at", ""),
                    updated_at=ds.get("updated_at", ""),
                )
            )
        return datasets

    def download_dataset(self, dataset_id: str, output_path: str) -> int:
        """Download and reconstruct a dataset to a local file.

        Returns the number of bytes written.
        """
        resp = self.session.get(
            self._url(f"/datasets/{dataset_id}/download"),
            timeout=self.timeout,
            stream=True,
        )

        if resp.status_code >= 400:
            raise PipelineError(f"Download failed ({resp.status_code})")

        total = 0
        with open(output_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                total += len(chunk)

        return total

    def delete_dataset(self, dataset_id: str) -> dict:
        """Delete a dataset and unpin its shards."""
        resp = self.session.delete(
            self._url(f"/datasets/{dataset_id}"),
            timeout=self.timeout,
        )
        return self._check_response(resp)

    # --- Shards ---

    def get_shard(self, cid: str) -> bytes:
        """Retrieve raw shard data by CID."""
        resp = self.session.get(
            self._url(f"/shards/{cid}"),
            timeout=self.timeout,
        )
        if resp.status_code >= 400:
            raise PipelineError(f"Shard retrieval failed ({resp.status_code})")
        return resp.content

    # --- Replication ---

    def get_replication_status(self, dataset_id: str) -> ReplicationStatus:
        """Get replication health for a dataset."""
        resp = self.session.get(
            self._url(f"/datasets/{dataset_id}/replication"),
            timeout=self.timeout,
        )
        result = self._check_response(resp)
        return ReplicationStatus(
            dataset_id=result["dataset_id"],
            total_shards=result["total_shards"],
            fully_replicated=result["fully_replicated"],
            under_replicated=result["under_replicated"],
            missing=result["missing"],
        )

    def trigger_replication_check(self) -> dict:
        """Trigger an immediate replication check."""
        resp = self.session.post(
            self._url("/replication/check"),
            timeout=self.timeout,
        )
        return self._check_response(resp)

    # --- Health ---

    def health(self) -> dict:
        """Get system health status."""
        resp = self.session.get(
            self._url("/health"),
            timeout=self.timeout,
        )
        return self._check_response(resp)

    def metrics(self) -> dict:
        """Get pipeline metrics."""
        resp = self.session.get(
            self._url("/metrics"),
            timeout=self.timeout,
        )
        return self._check_response(resp)


class PipelineError(Exception):
    """Raised when a pipeline API call fails."""

    pass
