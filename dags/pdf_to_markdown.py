"""Airflow DAG to download PDFs and convert them to Markdown.

This DAG performs the following steps on each run (processing one index per
batch):
- Select the next index from the metadata parquet
  at ``sql/metadata_table/flora_fauna_metadata_indexed.parquet``.
- Download the corresponding PDFs into ``docs_colletions/PDFs`` using the
  utilities in ``s3pdf_manager.download_pdf`` (credentials via ``.env``).
- Convert each downloaded PDF to Markdown and save it under
  ``docs_colletions/Markdowns`` using ``localPDFparse.parse.extract_markdown``.
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

try:
    _airflow_exceptions = importlib.import_module("airflow.exceptions")
    AirflowSkipException = getattr(_airflow_exceptions, "AirflowSkipException")
except Exception:  # pragma: no cover - local lint convenience

    class AirflowSkipException(Exception):
        pass


try:
    _airflow_models = importlib.import_module("airflow.models")
    AirflowVariable = getattr(_airflow_models, "Variable")
except Exception:  # pragma: no cover - local lint convenience

    class AirflowVariable:  # type: ignore
        @staticmethod
        def get(key: str, default_var: str | None = None) -> str | None:
            return default_var

        @staticmethod
        def set(key: str, value: str) -> None:  # noqa: D401 - no-op stub
            return None


try:
    _trigger_mod = importlib.import_module("airflow.operators.trigger_dagrun")
    TriggerDagRunOperator = getattr(_trigger_mod, "TriggerDagRunOperator")
except Exception:  # pragma: no cover - local lint convenience

    class TriggerDagRunOperator:  # type: ignore
        def __init__(self, *args, **kwargs):
            pass


try:
    _pyop_mod = importlib.import_module("airflow.operators.python")
    ShortCircuitOperator = getattr(_pyop_mod, "ShortCircuitOperator")
except Exception:  # pragma: no cover - local lint convenience

    class ShortCircuitOperator:  # type: ignore
        def __init__(self, *args, **kwargs):
            pass


# Eagerly load environment variables (e.g., AWS creds) from .env if present.
load_dotenv()


logger = logging.getLogger(__name__)


# Constants and defaults
PARQUET_PATH: str = "sql/metadata_table/flora_fauna_metadata_indexed.parquet"
PDF_OUTPUT_DIR: Path = Path("docs_colletions/PDFs")
MD_OUTPUT_DIR: Path = Path("docs_colletions/Markdowns")
NUM_ITEMS_DEFAULT: int = 10
BATCH_SIZE: int = 1
OFFSET_VAR_NAME: str = "pdf_to_markdown_batch_offset"

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

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
    )
    def select_indices(limit: int = num_items) -> dict:
        """Return the next batch of indices and tracking metadata.

        Returns
        - {"indices": [...], "next_offset": int, "total": int}
        """
        from s3pdf_manager.download_pdf import load_metadata

        if not Path(PARQUET_PATH).exists():
            raise AirflowSkipException(
                f"Parquet not found at {PARQUET_PATH}; skipping run."
            )

        df = load_metadata(PARQUET_PATH)
        # Read last processed offset; default to 0
        try:
            current_offset = int(AirflowVariable.get(OFFSET_VAR_NAME, "0") or 0)
        except Exception:
            current_offset = 0

        safe_limit = BATCH_SIZE
        idx_list = list(df.index)
        selected = idx_list[current_offset : current_offset + max(0, safe_limit)]
        indices = [str(idx) for idx in selected]
        total = len(idx_list)
        next_offset = min(current_offset + len(indices), total)

        if not indices:
            logger.info("No more indices to process. total=%d", total)
            raise AirflowSkipException("All indices processed.")

        logger.info(
            "Selected batch offset=%d size=%d next_offset=%d total=%d indices=%s",
            current_offset,
            len(indices),
            next_offset,
            total,
            indices,
        )
        return {"indices": indices, "next_offset": next_offset, "total": total}

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=15),
    )
    def download_pdfs(selection: dict) -> dict:
        """Download PDFs and return mapping with paths and indices.

        Returns
        - {"pdf_paths": [...], "indices": [...]} where indices are derived
          from path stems (successful downloads only).
        """
        from s3pdf_manager.download_pdf import download_by_list

        indices = list(selection.get("indices", []))
        paths = download_by_list(
            indices,
            parquet_path=PARQUET_PATH,
            output_dir=PDF_OUTPUT_DIR,
            skip_existing=True,
            dry_run=False,
        )
        pdf_paths = [str(p) for p in paths]
        ok_indices = [Path(p).stem for p in pdf_paths]
        logger.info("Downloaded/available PDFs: %s", pdf_paths)
        return {
            "pdf_paths": pdf_paths,
            "indices": ok_indices,
            "next_offset": int(selection.get("next_offset", 0)),
            "total": int(selection.get("total", 0)),
        }

    @task(
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=20),
    )
    def convert_to_markdown(download_result: dict) -> List[str]:
        """Convert each PDF to Markdown into ``docs_colletions/Markdowns``."""
        from localPDFparse.parse import extract_markdown

        MD_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        results: List[str] = []
        pdf_paths = list(download_result.get("pdf_paths", []))
        if not pdf_paths:
            logger.info("No PDFs to convert.")
            # Even if nothing converted, still advance offset to avoid blocking
            try:
                AirflowVariable.set(
                    OFFSET_VAR_NAME,
                    str(
                        min(
                            int(download_result.get("next_offset", 0)),
                            int(download_result.get("total", 0)),
                        )
                    ),
                )
            except Exception:
                pass
            return results

        for pdf in pdf_paths:
            if not pdf:
                continue
            try:
                extract_markdown(pdf, output_dir=str(MD_OUTPUT_DIR))
                md_path = str(MD_OUTPUT_DIR / (Path(pdf).stem + ".md"))
                results.append(md_path)
            except Exception as exc:
                logger.error("Failed to convert '%s' to markdown: %s", pdf, exc)
        logger.info("Generated Markdown files: %s", results)
        # Persist next batch offset so the next run continues after this batch
        try:
            AirflowVariable.set(
                OFFSET_VAR_NAME,
                str(
                    min(
                        int(download_result.get("next_offset", 0)),
                        int(download_result.get("total", 0)),
                    )
                ),
            )
        except Exception:
            pass
        return results

    # Task dependency graph
    selected = select_indices()
    downloaded = download_pdfs(selected)
    _converted = convert_to_markdown(downloaded)

    # Conditionally self-trigger next batch if there is more work left
    def _need_more_batches(**context) -> bool:
        ti = context["ti"]
        result = ti.xcom_pull(task_ids="download_pdfs") or {}
        try:
            next_offset = int(result.get("next_offset", 0))
            total = int(result.get("total", 0))
            return next_offset < total
        except Exception:
            return False

    check_more = ShortCircuitOperator(
        task_id="check_more_batches",
        python_callable=_need_more_batches,
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_next_batch",
        trigger_dag_id="pdf_to_markdown_parser",
        wait_for_completion=False,
        reset_dag_run=False,
    )

    _converted >> check_more >> trigger_next


# Instantiate DAG
dag_instance = pdf_to_markdown_parser()


if __name__ == "__main__":
    # Minimal, side-effect-safe demonstration
    print("DAG loaded:", dag_instance.dag_id)
    print("Parquet present:", Path(PARQUET_PATH).exists())
    print("PDF output dir:", PDF_OUTPUT_DIR)
    print("Markdown output dir:", MD_OUTPUT_DIR)
