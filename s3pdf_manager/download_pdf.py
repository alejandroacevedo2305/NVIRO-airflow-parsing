"""Utilities to download PDFs from S3 using indexed metadata.

This module loads a parquet table with an index named ``id_type_anexes`` and
columns such as ``s3_key``, ``from_compressed_file`` and ``file_name``.

It provides functions to download one, a list, or a range of indices to the
local directory ``docs_colletions/PDFs``. Each downloaded file is saved as
``{index}.pdf`` regardless of its original name inside S3 or a ZIP archive.

Environment variables used:
- ``S3_BUCKET``: Name of the S3 bucket. Required for actual downloads.
- ``S3_ENDPOINT_URL``: Optional custom endpoint (e.g., MinIO), if needed.

Example usage (programmatic):
    download_by_index("2129356319_ei-document_")
    download_by_range("2129356319_ei-document_", "2129409044_ei-document_")
    download_by_list([
        "2129356319_ei-document_",
        "2129370353_ei-document_",
    ])

uv run s3pdf_manager/download_pdf.py
"""

from __future__ import annotations

import io
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Tuple
from zipfile import BadZipFile, ZipFile

import boto3
import botocore
import pandas as pd
from dotenv import load_dotenv

# Constants
PARQUET_PATH: str = "sql/metadata_table/flora_fauna_metadata_indexed.parquet"
OUTPUT_DIR: Path = Path("docs_colletions/PDFs")
INDEX_NAME: str = "id_type_anexes"


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=("%(asctime)s | %(levelname)s | %(name)s | %(message)s"),
        stream=sys.stdout,
    )


_configure_logging()
logger = logging.getLogger(__name__)

# Load env vars from a .env file if present (AWS creds / region, etc.)
load_dotenv()


def load_metadata(parquet_path: str = PARQUET_PATH) -> pd.DataFrame:
    """Load metadata parquet and ensure the index is ``id_type_anexes``.

    Raises a ``ValueError`` if the required index/column cannot be found.
    """
    df = pd.read_parquet(parquet_path)

    if df.index.name == INDEX_NAME:
        return df

    if INDEX_NAME in df.columns:
        df = df.set_index(INDEX_NAME)
        return df

    raise ValueError(
        (
            f"The metadata must contain index or column '{INDEX_NAME}'. "
            f"Columns: {list(df.columns)} | index name: {df.index.name}"
        )
    )


def ensure_output_dir(directory: Path = OUTPUT_DIR) -> None:
    """Ensure output directory exists."""
    directory.mkdir(parents=True, exist_ok=True)


def compute_indices_in_range(
    df: pd.DataFrame,
    start_index: str,
    end_index: str,
) -> List[str]:
    """Return the ordered slice of indices between start and end (inclusive).

    The order follows the DataFrame's existing index order.
    """
    if start_index not in df.index or end_index not in df.index:
        missing = [idx for idx in [start_index, end_index] if idx not in df.index]
        raise KeyError(f"Indices not found in metadata: {', '.join(missing)}")

    start_pos = df.index.get_loc(start_index)
    end_pos = df.index.get_loc(end_index)

    if isinstance(start_pos, slice) or isinstance(end_pos, slice):
        raise ValueError("Duplicate indices are not supported for range ops.")

    if start_pos <= end_pos:
        selected = df.index[start_pos : end_pos + 1]
    else:
        selected = df.index[end_pos : start_pos + 1]

    return list(selected)


def _sanitize_filename_component(value: str) -> str:
    """Sanitize a string to be safe as filename component."""
    # Keep it simple: replace os separators and strip whitespace.
    return value.replace(os.sep, "_").strip()


@dataclass
class S3Config:
    bucket: str
    endpoint_url: Optional[str] = None
    region_name: Optional[str] = None

    @classmethod
    def from_env(cls) -> "S3Config":
        bucket = os.getenv("S3_BUCKET")
        endpoint_url = os.getenv("S3_ENDPOINT_URL")
        region_name = os.getenv("AWS_DEFAULT_REGION")
        if not bucket:
            raise EnvironmentError(
                "S3_BUCKET environment variable is required to download files."
            )
        if region_name:
            logger.info("Using AWS region from env: %s", region_name)
        return cls(
            bucket=bucket,
            endpoint_url=endpoint_url,
            region_name=region_name,
        )


class S3Downloader:
    """Thin wrapper around boto3 for downloading and inspecting objects."""

    def __init__(self, config: S3Config):
        self.config = config
        self.client = boto3.client(
            "s3",
            endpoint_url=config.endpoint_url,
            region_name=config.region_name,
        )

    def download_to_file(self, s3_key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Downloading s3://%s/%s -> %s",
            self.config.bucket,
            s3_key,
            str(destination),
        )
        self.client.download_file(self.config.bucket, s3_key, str(destination))

    def download_to_memory(self, s3_key: str) -> bytes:
        logger.info(
            "Downloading to memory s3://%s/%s",
            self.config.bucket,
            s3_key,
        )
        buf = io.BytesIO()
        self.client.download_fileobj(self.config.bucket, s3_key, buf)
        buf.seek(0)
        return buf.read()


def _extract_pdf_from_zip_bytes(
    zip_bytes: bytes, prefer_name: Optional[str] = None
) -> Tuple[str, bytes]:
    """Extract a PDF file from a ZIP bytes blob.

    Returns a tuple of (internal_name, file_bytes). If ``prefer_name`` is
    provided and found (case-insensitive) inside the ZIP, that entry is used.
    Otherwise, the first entry ending with ``.pdf`` is extracted.
    Raises ``FileNotFoundError`` if no PDF entry is found.
    """
    try:
        with ZipFile(io.BytesIO(zip_bytes)) as zf:
            entries = zf.namelist()
            if prefer_name:
                needle = prefer_name.lower()
                for name in entries:
                    if name.lower().endswith(needle):
                        with zf.open(name) as fp:
                            return name, fp.read()

            for name in entries:
                if name.lower().endswith(".pdf"):
                    with zf.open(name) as fp:
                        return name, fp.read()
    except BadZipFile as exc:
        raise BadZipFile(f"Invalid or corrupt ZIP archive: {exc}") from exc

    raise FileNotFoundError("No PDF file found inside the ZIP archive.")


def _build_output_path(index_value: str, output_dir: Path = OUTPUT_DIR) -> Path:
    safe = _sanitize_filename_component(index_value)
    return output_dir / f"{safe}.pdf"


def delete_by_index(index_value: str, output_dir: Path = OUTPUT_DIR) -> bool:
    """Delete a local PDF file corresponding to the given index.

    Returns True if a file was deleted, False if it did not exist.
    """
    path = _build_output_path(index_value, output_dir)
    if path.exists():
        path.unlink()
        logger.info("Deleted: %s", path)
        return True
    logger.info("Not found (nothing to delete): %s", path)
    return False


def delete_by_list(indices: Sequence[str], output_dir: Path = OUTPUT_DIR) -> int:
    """Delete multiple PDFs by their indices; returns count of deleted files."""
    deleted = 0
    for idx in indices:
        try:
            if delete_by_index(idx, output_dir=output_dir):
                deleted += 1
        except OSError as exc:
            logger.error("Failed to delete '%s': %s", idx, exc)
    return deleted


def _download_single(
    df: pd.DataFrame,
    index_value: str,
    downloader: S3Downloader,
    output_dir: Path = OUTPUT_DIR,
    skip_existing: bool = True,
    dry_run: bool = False,
) -> Path:
    """Download a single index into ``output_dir`` and return local path.

    Honors ``from_compressed_file`` by extracting a PDF from the ZIP when
    necessary. Always writes to ``{index}.pdf`` regardless of original name.
    """
    if index_value not in df.index:
        raise KeyError(f"Index '{index_value}' not in metadata.")

    row = df.loc[index_value]

    # Allow both column- and attribute-style access depending on Series frame.
    s3_key = row["s3_key"]
    from_zip = bool(row.get("from_compressed_file", False))
    prefer_name = row.get("file_name")

    destination = _build_output_path(index_value, output_dir)
    ensure_output_dir(output_dir)

    if skip_existing and destination.exists():
        logger.info("Skipping existing file: %s", destination)
        return destination

    if dry_run:
        logger.info("[DRY-RUN] Would download %s to %s", s3_key, destination)
        return destination

    if not from_zip:
        downloader.download_to_file(s3_key, destination)
        return destination

    # Download to memory and extract the PDF from ZIP.
    content = downloader.download_to_memory(s3_key)
    internal_name, pdf_bytes = _extract_pdf_from_zip_bytes(
        content, prefer_name=prefer_name
    )
    logger.info("Extracted '%s' from ZIP for index '%s'", internal_name, index_value)
    destination.write_bytes(pdf_bytes)
    return destination


def download_by_index(
    index_value: str,
    parquet_path: str = PARQUET_PATH,
    output_dir: Path = OUTPUT_DIR,
    skip_existing: bool = True,
    dry_run: bool = False,
) -> Path:
    """Download a single PDF given its metadata index value."""
    df = load_metadata(parquet_path)
    config = S3Config.from_env() if not dry_run else S3Config("dry-run")
    downloader = (
        S3Downloader(config) if not dry_run else None  # type: ignore[assignment]
    )
    return _download_single(
        df=df,
        index_value=index_value,
        downloader=downloader if downloader else S3Downloader(config),
        output_dir=output_dir,
        skip_existing=skip_existing,
        dry_run=dry_run,
    )


def download_by_list(
    indices: Sequence[str],
    parquet_path: str = PARQUET_PATH,
    output_dir: Path = OUTPUT_DIR,
    skip_existing: bool = True,
    dry_run: bool = False,
) -> List[Path]:
    """Download multiple PDFs given a list of index values."""
    df = load_metadata(parquet_path)
    config = S3Config.from_env() if not dry_run else S3Config("dry-run")
    downloader = (
        S3Downloader(config) if not dry_run else None  # type: ignore[assignment]
    )
    results: List[Path] = []
    for idx in indices:
        try:
            saved_path = _download_single(
                df=df,
                index_value=idx,
                downloader=downloader if downloader else S3Downloader(config),
                output_dir=output_dir,
                skip_existing=skip_existing,
                dry_run=dry_run,
            )
            results.append(saved_path)
        except (
            KeyError,
            botocore.exceptions.BotoCoreError,
            botocore.exceptions.ClientError,
            BadZipFile,
            FileNotFoundError,
            OSError,
        ) as exc:
            logger.error("Failed to download '%s': %s", idx, exc)
    return results


def download_by_range(
    start_index: str,
    end_index: str,
    parquet_path: str = PARQUET_PATH,
    output_dir: Path = OUTPUT_DIR,
    skip_existing: bool = True,
    dry_run: bool = False,
) -> List[Path]:
    """Download all PDFs for indices between ``start`` and ``end`` inclusive."""
    df = load_metadata(parquet_path)
    indices = compute_indices_in_range(df, start_index, end_index)
    return download_by_list(
        indices,
        parquet_path=parquet_path,
        output_dir=output_dir,
        skip_existing=skip_existing,
        dry_run=dry_run,
    )


def _has_aws_creds() -> bool:
    return bool(os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))


def _demo_safe_download_plan(df: pd.DataFrame) -> None:
    """Print a side-effect safe plan for a couple of indices."""
    sample = list(df.index[:2])

    if not sample:
        logger.info("No indices available to demo.")
        return

    logger.info("Demo plan for first two indices: %s", sample)
    for idx in sample:
        out = _build_output_path(idx)
        logger.info("Would save to: %s", out)


if __name__ == "__main__":
    # Simple demonstration / self-tests (side-effect safe)
    logger.info("Loading metadata from: %s", PARQUET_PATH)
    try:
        metadata = load_metadata(PARQUET_PATH)
    except (FileNotFoundError, ValueError, OSError) as exc:
        logger.error("Failed to load metadata: %s", exc)
        sys.exit(1)

    logger.info("Loaded %d rows. Index name: %s", len(metadata), metadata.index.name)
    logger.info("First index (if any): %s", metadata.index[0] if len(metadata) else "-")

    # Self-tests for range computation with a tiny mock index
    mock = pd.DataFrame(index=["a", "b", "c", "d"])  # minimal structure
    r1 = compute_indices_in_range(mock, "b", "d")
    assert r1 == ["b", "c", "d"], f"Unexpected r1: {r1}"
    r2 = compute_indices_in_range(mock, "d", "b")
    assert r2 == ["b", "c", "d"], f"Unexpected r2: {r2}"
    logger.info("Range computation self-tests passed.")

    # Only plan actions unless bucket and credentials are provided
    bucket_name = os.getenv("S3_BUCKET")
    if not bucket_name or not _has_aws_creds():
        logger.info(
            "S3_BUCKET and/or AWS credentials not set. Running in DRY-RUN mode."
        )
        _demo_safe_download_plan(metadata)
        # Example dry-run for first index if available
        if len(metadata):
            first_idx = str(metadata.index[0])
            out_path = download_by_index(
                first_idx, parquet_path=PARQUET_PATH, dry_run=True
            )
            logger.info("Dry-run would create: %s", out_path)
        sys.exit(0)

    # If configured, perform a tiny real download of the first index only.
    try:
        if len(metadata):
            idx0 = str(metadata.index[0])
            downloaded_path = download_by_index(idx0, parquet_path=PARQUET_PATH)
            logger.info("Downloaded: %s", downloaded_path)
        else:
            logger.info("No entries to download.")
    except botocore.exceptions.ClientError as exc:  # pragma: no cover - runtime
        logger.error("AWS error during download: %s", exc)
    except (BadZipFile, FileNotFoundError, OSError) as exc:
        logger.error("Error during download: %s", exc)
