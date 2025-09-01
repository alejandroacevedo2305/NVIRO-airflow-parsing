"""Airflow DAG to download PDFs and convert them to Markdown.

This DAG performs the following steps on each run:
- Select the first N indices (default 10) from the metadata parquet
  at ``sql/metadata_table/flora_fauna_metadata_indexed.parquet``.
- Download the corresponding PDFs into ``docs_colletions/PDFs`` using the
  utilities in ``s3pdf_manager.download_pdf`` (credentials via ``.env``).
- Convert each downloaded PDF to Markdown and save it under
  ``docs_colletions/Markdowns`` using ``localPDFparse.parse.extract_markdown``.
- Remove the local PDFs after conversion using the delete helpers.
"""

# %%

from __future__ import annotations

import importlib
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

try:  # Allow local linting without Airflow installed
    _airflow_decorators = importlib.import_module("airflow.decorators")
    dag = getattr(_airflow_decorators, "dag")
    task = getattr(_airflow_decorators, "task")
except (ImportError, ModuleNotFoundError, AttributeError):  # pragma: no cover

    def dag(*_args, **_kwargs):
        def _wrap(func):
            return func

        return _wrap

    def task(*_args, **_kwargs):
        def _wrap(func):
            return func

        return _wrap


from dotenv import load_dotenv

# Eagerly load environment variables (e.g., AWS creds) from .env if present.
load_dotenv()


logger = logging.getLogger(__name__)


# Constants and defaults
PARQUET_PATH: str = "sql/metadata_table/flora_fauna_metadata_indexed.parquet"
PDF_OUTPUT_DIR: Path = Path("docs_colletions/PDFs")
MD_OUTPUT_DIR: Path = Path("docs_colletions/Markdowns")
NUM_ITEMS_DEFAULT: int = 10

"""
Ensure project root is in sys.path so Airflow can import local packages when
executing tasks in the scheduler/worker (e.g., ``s3pdf_manager`` and
``localPDFparse``).
"""
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


default_args = {
    "owner": "Alejandro",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="pdf_to_markdown_parser",
    start_date=datetime(2024, 10, 1, 0, 0),
    schedule_interval="@daily",
    catchup=False,
    tags=["pdf", "markdown", "s3"],
    default_args=default_args,
)
def pdf_to_markdown_parser(num_items: int = NUM_ITEMS_DEFAULT):
    """Pipeline: select metadata, download PDFs, convert to MD, cleanup."""

    @task()
    def select_indices(limit: int = num_items) -> List[str]:
        """Return the first ``limit`` indices from the metadata parquet."""
        from s3pdf_manager.download_pdf import load_metadata

        df = load_metadata(PARQUET_PATH)
        try:
            safe_limit = int(limit)
        except (TypeError, ValueError):
            safe_limit = NUM_ITEMS_DEFAULT
        idx_list = list(df.index)
        selected = idx_list[: max(0, safe_limit)]
        indices = [str(idx) for idx in selected]
        if not indices:
            logger.info("No indices found in metadata.")
        else:
            logger.info("Selected indices: %s", indices)
        return indices

    @task()
    def download_pdfs(indices: List[str]) -> List[str]:
        """Download PDFs for given indices and return local file paths."""
        from s3pdf_manager.download_pdf import download_by_list

        paths = download_by_list(
            indices,
            parquet_path=PARQUET_PATH,
            output_dir=PDF_OUTPUT_DIR,
            skip_existing=True,
            dry_run=False,
        )
        pdfs = [str(p) for p in paths]
        logger.info("Downloaded/available PDFs: %s", pdfs)
        return pdfs

    @task()
    def convert_to_markdown(pdf_paths: List[str]) -> List[str]:
        """Convert each PDF to Markdown into ``docs_colletions/Markdowns``."""
        from localPDFparse.parse import extract_markdown

        MD_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        results: List[str] = []
        for pdf in pdf_paths:
            if not pdf:
                continue
            extract_markdown(pdf, output_dir=str(MD_OUTPUT_DIR))
            md_path = str(MD_OUTPUT_DIR / (Path(pdf).stem + ".md"))
            results.append(md_path)
        logger.info("Generated Markdown files: %s", results)
        return results

    @task()
    def cleanup_pdfs(indices: List[str]) -> int:
        """Delete the local PDFs corresponding to the provided indices."""
        from s3pdf_manager.download_pdf import delete_by_list

        deleted = delete_by_list(indices, output_dir=PDF_OUTPUT_DIR)
        logger.info("Deleted %d PDFs from %s", deleted, str(PDF_OUTPUT_DIR))
        return deleted

    # Task dependency graph
    selected = select_indices()
    downloaded = download_pdfs(selected)
    _converted = convert_to_markdown(downloaded)
    cleanup_pdfs(selected)


# Instantiate DAG
dag_instance = pdf_to_markdown_parser()


if __name__ == "__main__":
    # Minimal, side-effect-safe demonstration
    print("DAG loaded:", dag_instance.dag_id)
    print("Parquet present:", Path(PARQUET_PATH).exists())
    print("PDF output dir:", PDF_OUTPUT_DIR)
    print("Markdown output dir:", MD_OUTPUT_DIR)
