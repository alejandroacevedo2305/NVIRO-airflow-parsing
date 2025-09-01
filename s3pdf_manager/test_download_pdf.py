"""Tests for `s3pdf_manager.download_pdf`.

These tests avoid real AWS by mocking the downloader and S3 client behavior.
"""

# %%
from __future__ import annotations

import io
from pathlib import Path
from typing import Dict
from zipfile import ZipFile

import pandas as pd
import pytest

from s3pdf_manager.download_pdf import (
    _build_output_path,
    _extract_pdf_from_zip_bytes,
    compute_indices_in_range,
    delete_by_index,
    delete_by_list,
    download_by_index,
)


def create_zip_with_files(files: Dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    buf.seek(0)
    return buf.read()


def test_compute_indices_in_range_basic():
    df = pd.DataFrame(index=["a", "b", "c", "d"])
    assert compute_indices_in_range(df, "b", "d") == ["b", "c", "d"]
    assert compute_indices_in_range(df, "d", "b") == ["b", "c", "d"]


def test_extract_pdf_from_zip_bytes_prefers_named_match():
    files = {
        "random.txt": b"hello",
        "report.pdf": b"%PDF-1.4 content A",
        "other.pdf": b"%PDF-1.4 content B",
    }
    zip_bytes = create_zip_with_files(files)
    name, content = _extract_pdf_from_zip_bytes(zip_bytes, prefer_name="other.pdf")
    assert name.endswith("other.pdf")
    assert content == files["other.pdf"]


def test_extract_pdf_from_zip_bytes_first_pdf_when_no_preference():
    files = {
        "a.pdf": b"%PDF-1.4 A",
        "b.pdf": b"%PDF-1.4 B",
    }
    zip_bytes = create_zip_with_files(files)
    name, content = _extract_pdf_from_zip_bytes(zip_bytes)
    assert name in files
    assert content in files.values()


class DummyDownloader:
    def __init__(self, content_map: Dict[str, bytes]):
        self.content_map = content_map

    def download_to_file(self, s3_key: str, destination: Path) -> None:
        destination.write_bytes(self.content_map[s3_key])

    def download_to_memory(self, s3_key: str) -> bytes:
        return self.content_map[s3_key]


def test_download_by_index_uncompressed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    df = pd.DataFrame(
        {
            "s3_key": ["key1"],
            "from_compressed_file": [False],
            "file_name": ["doc.pdf"],
        },
        index=pd.Index(["IDX_1"], name="id_type_anexes"),
    )

    # Patch metadata loader
    monkeypatch.setattr(
        "s3pdf_manager.download_pdf.load_metadata", lambda *_args, **_kw: df
    )
    # Env required by downloader factory
    monkeypatch.setenv("S3_BUCKET", "dummy-bucket")

    # Patch downloader factory
    dummy = DummyDownloader({"key1": b"%PDF-1.4 uncompressed"})
    monkeypatch.setattr(
        "s3pdf_manager.download_pdf.S3Downloader", lambda *_a, **_k: dummy
    )

    out_dir = tmp_path / "out"
    path = download_by_index(
        "IDX_1", output_dir=out_dir, skip_existing=True, dry_run=False
    )
    assert path.exists()
    assert path.read_bytes() == b"%PDF-1.4 uncompressed"
    assert path.name == _build_output_path("IDX_1", out_dir).name


def test_download_by_index_from_zip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    files = {"inside.pdf": b"%PDF-1.4 zipped"}
    zip_bytes = create_zip_with_files(files)

    df = pd.DataFrame(
        {
            "s3_key": ["zipkey"],
            "from_compressed_file": [True],
            "file_name": ["inside.pdf"],
        },
        index=pd.Index(["IDX_ZIP"], name="id_type_anexes"),
    )

    monkeypatch.setattr(
        "s3pdf_manager.download_pdf.load_metadata", lambda *_args, **_kw: df
    )
    monkeypatch.setenv("S3_BUCKET", "dummy-bucket")

    dummy = DummyDownloader({"zipkey": zip_bytes})
    monkeypatch.setattr(
        "s3pdf_manager.download_pdf.S3Downloader", lambda *_a, **_k: dummy
    )

    out_dir = tmp_path / "out"
    path = download_by_index(
        "IDX_ZIP", output_dir=out_dir, skip_existing=True, dry_run=False
    )
    assert path.exists()
    assert path.read_bytes() == files["inside.pdf"]


def test_skip_existing_file_uncompressed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    df = pd.DataFrame(
        {
            "s3_key": ["key1"],
            "from_compressed_file": [False],
            "file_name": ["doc.pdf"],
        },
        index=pd.Index(["IDX_EXIST"], name="id_type_anexes"),
    )

    monkeypatch.setattr(
        "s3pdf_manager.download_pdf.load_metadata", lambda *_args, **_kw: df
    )
    monkeypatch.setenv("S3_BUCKET", "dummy-bucket")

    # Create an existing file with known content
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    existing_path = _build_output_path("IDX_EXIST", out_dir)
    existing_path.write_bytes(b"EXISTING")

    # Dummy downloader that would otherwise overwrite
    dummy = DummyDownloader({"key1": b"%PDF-1.4 would-overwrite"})
    monkeypatch.setattr(
        "s3pdf_manager.download_pdf.S3Downloader", lambda *_a, **_k: dummy
    )

    result_path = download_by_index(
        "IDX_EXIST", output_dir=out_dir, skip_existing=True, dry_run=False
    )
    assert result_path == existing_path
    assert result_path.read_bytes() == b"EXISTING"


def test_skip_existing_file_from_zip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    df = pd.DataFrame(
        {
            "s3_key": ["zipkey"],
            "from_compressed_file": [True],
            "file_name": ["inside.pdf"],
        },
        index=pd.Index(["IDX_EXIST_ZIP"], name="id_type_anexes"),
    )

    monkeypatch.setattr(
        "s3pdf_manager.download_pdf.load_metadata", lambda *_args, **_kw: df
    )
    monkeypatch.setenv("S3_BUCKET", "dummy-bucket")

    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    existing_path = _build_output_path("IDX_EXIST_ZIP", out_dir)
    existing_path.write_bytes(b"EXISTING-ZIP")

    files = {"inside.pdf": b"%PDF-1.4 zipped"}
    zip_bytes = create_zip_with_files(files)
    dummy = DummyDownloader({"zipkey": zip_bytes})
    monkeypatch.setattr(
        "s3pdf_manager.download_pdf.S3Downloader", lambda *_a, **_k: dummy
    )

    result_path = download_by_index(
        "IDX_EXIST_ZIP", output_dir=out_dir, skip_existing=True, dry_run=False
    )
    assert result_path == existing_path
    assert result_path.read_bytes() == b"EXISTING-ZIP"


def test_delete_by_index(tmp_path: Path):
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    p = _build_output_path("IDX_DEL", out_dir)
    p.write_bytes(b"X")
    assert p.exists()
    assert delete_by_index("IDX_DEL", output_dir=out_dir) is True
    assert not p.exists()
    # idempotent second call
    assert delete_by_index("IDX_DEL", output_dir=out_dir) is False


def test_delete_by_list(tmp_path: Path):
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    p1 = _build_output_path("IDX1", out_dir)
    p2 = _build_output_path("IDX2", out_dir)
    p1.write_bytes(b"1")
    p2.write_bytes(b"2")
    count = delete_by_list(["IDX1", "IDX2", "IDX3"], output_dir=out_dir)
    assert count == 2
    assert not p1.exists() and not p2.exists()
