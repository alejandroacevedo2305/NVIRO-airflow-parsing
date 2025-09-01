"""Utilities to download and manage PDFs from S3 using indexed metadata.

This module loads a parquet table with an index named ``id_type_anexes`` and
columns such as ``s3_key``, ``from_compressed_file`` and ``file_name``.

It provides functions to download one, a list, or a range of indices to the
local directory ``docs_colletions/PDFs``. Each downloaded file is saved as
``{index}.pdf`` regardless of its original name inside S3 or inside a ZIP.

Environment variables (read from the environment and ``.env`` if present):
- ``S3_BUCKET``: Optional S3 bucket name. If not set, the bucket is resolved
  from the metadata row (supports fully qualified ``s3://bucket/key`` in
  ``s3_key`` or a dedicated bucket column, e.g., ``s3_bucket``/``bucket``).
- ``S3_ENDPOINT_URL``: Optional custom endpoint (e.g., MinIO).
- ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``: AWS credentials.
- ``AWS_DEFAULT_REGION``: Region for S3 API calls.

Key behaviors:
- Skip-existing: by default, if ``{index}.pdf`` already exists, it is not
  re-downloaded.
- Dry-run mode: when enabled, logs intended downloads without making changes.
- ZIP support: when ``from_compressed_file`` is true, a PDF is extracted from
  the ZIP (preferring ``file_name`` if present) and saved as ``{index}.pdf``.
- Deletion helpers: remove local PDFs by index.

Example usage (programmatic):
    download_by_index("2129356319_ei-document_")
    download_by_range("2129356319_ei-document_", "2129409044_ei-document_")
    download_by_list([
        "2129356319_ei-document_",
        "2129370353_ei-document_",
    ])

CLI demo:
    uv run s3pdf_manager/download_pdf.py
"""

# %%

from __future__ import annotations

import io
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse
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
    """Configure module-level logging to stdout at INFO level."""
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

    - Ensures the DataFrame index name is ``id_type_anexes``.
    - If the column exists but is not the index, it is set as the index.

    Raises ``ValueError`` if neither an index nor a column named
    ``id_type_anexes`` is found.
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
    """Ensure output directory exists.

    Parameters
    - directory: Target directory to create if it does not exist.
    """
    directory.mkdir(parents=True, exist_ok=True)


def compute_indices_in_range(
    df: pd.DataFrame,
    start_index: str,
    end_index: str,
) -> List[str]:
    """Return ordered slice of indices between ``start`` and ``end`` inclusive.

    The order follows the DataFrame's existing index order. ``start`` and
    ``end`` can be given in either order.

    Raises ``KeyError`` when either index is not present. Raises ``ValueError``
    for duplicated index labels (unsupported for range operations).
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
    """Sanitize a string to be safe as a filename component.

    Replaces path separators and trims whitespace.
    """
    # Keep it simple: replace os separators and strip whitespace.
    return value.replace(os.sep, "_").strip()


@dataclass
class S3Config:
    """Configuration to create an S3 client.

    Attributes
    - bucket: Optional default S3 bucket name; resolved per-row if not set.
    - endpoint_url: Optional endpoint URL (e.g., MinIO).
    - region_name: Optional AWS region name.
    """

    bucket: Optional[str] = None
    endpoint_url: Optional[str] = None
    region_name: Optional[str] = None

    @classmethod
    def from_env(cls) -> "S3Config":
        """Construct configuration from environment variables.

        ``S3_BUCKET`` is optional. When missing, the bucket will be resolved
        from each metadata row (e.g., from ``s3_key`` like
        ``s3://bucket/key`` or a dedicated bucket column).
        """
        bucket = os.getenv("S3_BUCKET")
        endpoint_url = os.getenv("S3_ENDPOINT_URL")
        region_name = os.getenv("AWS_DEFAULT_REGION")
        if region_name:
            logger.info("Using AWS region from env: %s", region_name)
        return cls(
            bucket=bucket,
            endpoint_url=endpoint_url,
            region_name=region_name,
        )


class S3Downloader:
    """Thin wrapper around boto3 for downloading S3 objects.

    Parameters
    - config: ``S3Config`` with bucket, endpoint and region.
    """

    def __init__(self, config: S3Config):
        self.config = config
        self.client = boto3.client(
            "s3",
            endpoint_url=config.endpoint_url,
            region_name=config.region_name,
        )

    def download_to_file(self, bucket: str, s3_key: str, destination: Path) -> None:
        """Download an object to disk.

        Parameters
        - s3_key: Key within the configured S3 bucket.
        - destination: Local file path to write to.
        """
        destination.parent.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Downloading s3://%s/%s -> %s",
            bucket,
            s3_key,
            str(destination),
        )
        self.client.download_file(bucket, s3_key, str(destination))

    def download_to_memory(self, bucket: str, s3_key: str) -> bytes:
        """Download an object into memory as bytes.

        Parameters
        - s3_key: Key within the configured S3 bucket.
        """
        logger.info("Downloading to memory s3://%s/%s", bucket, s3_key)
        buf = io.BytesIO()
        self.client.download_fileobj(bucket, s3_key, buf)
        buf.seek(0)
        return buf.read()


def _resolve_bucket_and_key(row: pd.Series) -> Tuple[str, str]:
    """Resolve (bucket, key) from a metadata row.

    Resolution order:
    1) When ``s3_key`` starts with ``s3://``, parse bucket/key from it.
    2) Otherwise, look for bucket in common columns: ``s3_bucket``, ``bucket``,
       ``bucket_name`` (case-insensitive). If not found, search any column whose
       name contains ``bucket`` and use its non-empty string value.
    3) If still not found, try parsing ``aws_url`` column for a bucket name.
       This supports common AWS console and virtual-hosted-style patterns.
    4) As a final fallback, use environment variable ``S3_BUCKET`` if present.
    5) For the key, prefer ``s3_key``; else look for ``key``, ``s3_object_key``,
       ``path``, ``s3_path``. Query parameters in ``aws_url`` (e.g., ``prefix``)
       are ignored for the key unless no key candidates are present.
    """
    key_candidates = ["s3_key", "key", "s3_object_key", "path", "s3_path"]

    s3_key_value = None
    for cand in key_candidates:
        if cand in row and isinstance(row[cand], str) and row[cand].strip():
            s3_key_value = row[cand].strip()
            break

    if not s3_key_value:
        raise KeyError(
            "Metadata row must contain a non-empty key in one of columns: "
            f"{key_candidates}"
        )

    if s3_key_value.lower().startswith("s3://"):
        without_scheme = s3_key_value[5:]  # strip 's3://'
        parts = without_scheme.split("/", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(
                f"Invalid S3 URI in s3_key: {s3_key_value!r} (expected s3://bucket/key)"
            )
        return parts[0], parts[1]

    bucket_value: Optional[str] = None
    for cand in ["s3_bucket", "bucket", "bucket_name"]:
        if cand in row and isinstance(row[cand], str) and row[cand].strip():
            bucket_value = row[cand].strip()
            break

    if not bucket_value:
        # Try any column whose name contains 'bucket'
        for col in row.index:
            if "bucket" in str(col).lower():
                val = row[col]
                if isinstance(val, str) and val.strip():
                    bucket_value = val.strip()
                    break

    # Attempt to extract from aws_url when available
    if not bucket_value and "aws_url" in row and isinstance(row["aws_url"], str):
        raw_url = row["aws_url"].strip()
        if raw_url:
            try:
                parsed = urlparse(raw_url)
                host = parsed.hostname or ""
                # Console pattern often includes bucket in host or path
                # Examples:
                # - https://bucket.s3.us-west-2.amazonaws.com/key
                # - https://s3.console.aws.amazon.com/s3/buckets/bucket?region=...
                # - https://bucket.us-west-2.console.aws.amazon.com/s3/...
                if host and ".s3." in host and not host.startswith("s3."):
                    # virtual-hosted-style: bucket.s3.region.amazonaws.com
                    bucket_value = host.split(".s3.", 1)[0]
                elif parsed.path:
                    parts = [p for p in parsed.path.split("/") if p]
                    # Console buckets path: /s3/buckets/<bucket>
                    if "buckets" in parts:
                        i = parts.index("buckets")
                        if i + 1 < len(parts):
                            bucket_value = parts[i + 1]
                    # Console object path: /s3/object/<bucket>
                    if not bucket_value and "object" in parts:
                        i = parts.index("object")
                        if i + 1 < len(parts):
                            bucket_value = parts[i + 1]
                # Some console hosts embed bucket as first label
                if not bucket_value and host and ".console.aws.amazon.com" in host:
                    # e.g. bucket.region.console.aws.amazon.com
                    first_label = host.split(".", 1)[0]
                    # Heuristic: ignore common labels like "s3" or regions
                    if first_label and first_label not in {"s3"}:
                        bucket_value = first_label
                # If still missing, check query params like bucket=bkt
                if not bucket_value and parsed.query:
                    q = parse_qs(parsed.query)
                    cand = q.get("bucket") or q.get("Bucket")
                    if cand and cand[0]:
                        bucket_value = cand[0]
            except Exception:
                # Best-effort; ignore parse errors
                pass

    if not bucket_value:
        # Fallback to environment variable if available
        env_bucket = os.getenv("S3_BUCKET")
        if env_bucket:
            bucket_value = env_bucket
        else:
            raise KeyError(
                "Could not resolve S3 bucket from metadata row. Provide a "
                "column like 's3_bucket'/'bucket', a fully-qualified 's3_key',"
                " or set S3_BUCKET in the environment."
            )

    return bucket_value, s3_key_value.lstrip("/")


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


def _looks_like_pdf(data: bytes) -> bool:
    """Heuristic: return True if bytes look like a PDF file."""
    header = data[:8]
    return header.startswith(b"%PDF")


def _build_output_path(index_value: str, output_dir: Path = OUTPUT_DIR) -> Path:
    """Build the local output path for a given index.

    Always returns ``{output_dir}/{index}.pdf`` (sanitized index).
    """
    safe = _sanitize_filename_component(index_value)
    return output_dir / f"{safe}.pdf"


def delete_by_index(index_value: str, output_dir: Path = OUTPUT_DIR) -> bool:
    """Delete the local PDF corresponding to ``index_value``.

    Parameters
    - index_value: Value of the ``id_type_anexes`` index to remove.
    - output_dir: Directory where PDFs are stored.

    Returns True if a file was deleted, False if nothing existed.
    """
    path = _build_output_path(index_value, output_dir)
    if path.exists():
        path.unlink()
        logger.info("Deleted: %s", path)
        return True
    logger.info("Not found (nothing to delete): %s", path)
    return False


def delete_by_list(indices: Sequence[str], output_dir: Path = OUTPUT_DIR) -> int:
    """Delete multiple PDFs by their indices.

    Parameters
    - indices: Sequence of ``id_type_anexes`` values to remove.
    - output_dir: Directory where PDFs are stored.

    Returns the number of files that were deleted.
    """
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
    """Download a single index into ``output_dir`` and return the local path.

    Behavior
    - If ``skip_existing`` and the target file exists, nothing is done.
    - If the metadata row has ``from_compressed_file=True``, the S3 object is
      treated as a ZIP archive. A PDF is extracted (preferably matching the
      ``file_name`` column case-insensitively), and the extracted bytes are
      saved.
    - Otherwise, the S3 object is saved directly.

    The output filename is always ``{index}.pdf`` based on the DataFrame index.
    """
    if index_value not in df.index:
        raise KeyError(f"Index '{index_value}' not in metadata.")

    row = df.loc[index_value]

    # Allow both column- and attribute-style access depending on Series frame.
    # Resolve bucket/key from metadata row
    bucket, s3_key = _resolve_bucket_and_key(row)
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

    # Always download to memory first; handle PDFs directly, else try ZIP.
    content = downloader.download_to_memory(bucket, s3_key)
    if _looks_like_pdf(content):
        destination.write_bytes(content)
        return destination

    try:
        internal_name, pdf_bytes = _extract_pdf_from_zip_bytes(
            content, prefer_name=prefer_name
        )
        logger.info(
            "Extracted '%s' from ZIP for index '%s'", internal_name, index_value
        )
        destination.write_bytes(pdf_bytes)
        return destination
    except (BadZipFile, FileNotFoundError):
        # Unknown content: propagate error
        raise


def download_by_index(
    index_value: str,
    parquet_path: str = PARQUET_PATH,
    output_dir: Path = OUTPUT_DIR,
    skip_existing: bool = True,
    dry_run: bool = False,
) -> Path:
    """Download a single PDF given its metadata index value.

    Parameters
    - index_value: Value of the ``id_type_anexes`` index to download.
    - parquet_path: Path to the metadata parquet file.
    - output_dir: Directory where PDFs are saved.
    - skip_existing: If True, skip when target file already exists.
    - dry_run: If True, only log the intended action without downloading.
    """
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
    """Download multiple PDFs given a list of index values.

    Returns a list of local paths (existing or newly created).
    """
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
    """Download all PDFs for indices between ``start`` and ``end`` inclusive.

    This uses the current ordering of the DataFrame's index and supports
    inverted ranges (``start`` after ``end``).
    """
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
    """Return True if AWS credentials appear present in the environment."""
    return bool(os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))


def _demo_safe_download_plan(df: pd.DataFrame) -> None:
    """Print a side-effect-safe plan for a couple of indices.

    This function only logs proposed output paths, avoiding external I/O.
    """
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

    # Only plan actions unless AWS credentials are provided
    if not _has_aws_creds():
        logger.info("AWS credentials not set. Running in DRY-RUN mode.")
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
