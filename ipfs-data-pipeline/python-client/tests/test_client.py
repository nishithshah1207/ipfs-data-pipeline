"""Tests for the pipeline client."""

import pytest
from unittest.mock import MagicMock, patch
from pipeline_client.client import PipelineClient, PipelineError, Dataset, IngestResult


class TestPipelineClient:
    def setup_method(self):
        self.client = PipelineClient(base_url="http://localhost:8080")

    def test_url_construction(self):
        assert self.client._url("/health") == "http://localhost:8080/api/v1/health"
        assert self.client._url("/datasets") == "http://localhost:8080/api/v1/datasets"

    def test_url_trailing_slash(self):
        client = PipelineClient(base_url="http://localhost:8080/")
        assert client._url("/health") == "http://localhost:8080/api/v1/health"

    @patch("pipeline_client.client.requests.Session")
    def test_health_success(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "status": "healthy",
            "ipfs_nodes": [],
            "total_shards": 0,
            "pinned_ratio": 0.0,
            "uptime": "1h30m",
        }
        mock_session.get.return_value = mock_resp

        client = PipelineClient(base_url="http://localhost:8080")
        result = client.health()
        assert result["status"] == "healthy"

    @patch("pipeline_client.client.requests.Session")
    def test_list_datasets_empty(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"datasets": [], "total": 0}
        mock_session.get.return_value = mock_resp

        client = PipelineClient(base_url="http://localhost:8080")
        datasets = client.list_datasets()
        assert datasets == []

    @patch("pipeline_client.client.requests.Session")
    def test_api_error_handling(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.json.return_value = {"error": "dataset not found", "code": 404}
        mock_session.get.return_value = mock_resp

        client = PipelineClient(base_url="http://localhost:8080")
        with pytest.raises(PipelineError, match="dataset not found"):
            client.get_dataset("nonexistent")

    def test_fmt_size_helper(self):
        from pipeline_client.cli import _fmt_size

        assert _fmt_size(0) == "0 B"
        assert _fmt_size(512) == "512.0 B"
        assert _fmt_size(1024) == "1.0 KB"
        assert _fmt_size(1048576) == "1.0 MB"
        assert _fmt_size(1073741824) == "1.0 GB"
