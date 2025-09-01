"""Airflow DAG for parsing PDFs to Markdown with cleaning and refinement.

This DAG processes documents by:
1. Loading metadata from a Parquet file
2. Downloading PDFs from S3
3. Parsing PDFs to raw Markdown
4. Cleaning/refining Markdown using LLMs
5. Saving processed documents
"""

import logging
import os
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from sql.validate_available_metadata import load_indexed_metadata

# Configure logging
logger = logging.getLogger(__name__)

# ====================================================================
# CONFIGURATION VARIABLES
# ====================================================================

# AWS Configuration (can be set as Airflow Variables)
BUCKET_NAME = Variable.get("BUCKET_NAME", default_var="nviro-crawlers")
REGION_NAME = Variable.get("AWS_REGION", default_var="us-west-2")
AWS_ACCESS_KEY_ID = Variable.get(
    "AWS_ACCESS_KEY_ID", default_var=os.getenv("AWS_ACCESS_KEY_ID")
)
AWS_SECRET_ACCESS_KEY = Variable.get(
    "AWS_SECRET_ACCESS_KEY", default_var=os.getenv("AWS_SECRET_ACCESS_KEY")
)


MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 2.0
BATCH_SIZE = 10  # Process documents in batches

# Test mode for development
TEST_MODE = Variable.get("TEST_MODE", default_var=False, deserialize_json=True)

# ====================================================================
# DEFAULT ARGUMENTS
# ====================================================================

default_args = {
    "owner": "Alejandro",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

# ====================================================================
# HELPER FUNCTIONS
# ====================================================================


def setup_python_path():
    """Ensure the repo root is in sys.path for imports."""
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


def log_event(event: str, **kwargs) -> None:
    """Log events with timestamp and context."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if kwargs:
        extras = " ".join(f"{key}={value}" for key, value in kwargs.items())
        logger.info(f"[{timestamp}] {event} {extras}")
    else:
        logger.info(f"[{timestamp}] {event}")


setup_python_path()
(MARKDOWN_REFINED_COLLECTION_DIR,)

# ====================================================================
# TASK DEFINITIONS
# ====================================================================


@dag(
    dag_id="pdf_to_markdown_parser",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["pdf", "parsing", "markdown", "llm"],
    default_args=default_args,
    description="Parse PDFs to Markdown with LLM-based cleaning and refinement",
)
def pdf_to_markdown_parser():
    """Main DAG for PDF to Markdown parsing pipeline.

    This DAG orchestrates the entire document processing workflow:
    - Loads document metadata
    - Downloads PDFs from S3
    - Parses PDFs to raw Markdown
    - Cleans Markdown using LLMs
    - Saves processed documents
    """

    @task()
    def load_metadata() -> list[dict[str, Any]]:
        """Load document metadata from Parquet file.

        Returns:
            List of document metadata dictionaries
        """
        setup_python_path()

        log_event("metadata.load.start")

        try:
            # Load metadata DataFrame
            metadata_df = load_indexed_metadata()

            # Convert to list of dictionaries for serialization
            docs_metadata = []
            for _, row in metadata_df.iterrows():
                doc_dict = row.to_dict()
                # Ensure all values are serializable
                for key, value in doc_dict.items():
                    if pd.isna(value):
                        doc_dict[key] = None
                    elif isinstance(value, pd.Timestamp):
                        doc_dict[key] = value.isoformat()
                docs_metadata.append(doc_dict)

            log_event("metadata.load.success", count=len(docs_metadata))

            # In test mode, limit to first 3 documents
            if TEST_MODE:
                docs_metadata = docs_metadata[:3]
                log_event("metadata.test_mode", limited_count=3)

            return docs_metadata

        except Exception as e:
            log_event("metadata.load.error", error=str(e))
            raise

    @task()
    def check_existing_files(
        docs_metadata: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Check which documents need processing based on existing files.

        Args:
            docs_metadata: List of document metadata

        Returns:
            List of documents that need processing
        """

        log_event("check_existing.start", total_docs=len(docs_metadata))

        docs_to_process = []

        for doc_meta in docs_metadata:
            s3_key = doc_meta.get("s3_key")
            if not s3_key:
                log_event("check_existing.skip_no_key", doc=doc_meta.get("id"))
                continue

            # Generate filenames
            original_filename = Path(s3_key).name
            unique_id = uuid.uuid4().hex[:8]
            filename = f"round_2_{unique_id}_{original_filename}"
            md_filename = Path(filename).with_suffix(".md")

            # Store generated filenames in metadata
            doc_meta["filename"] = filename
            doc_meta["md_filename"] = str(md_filename)
            doc_meta["unique_id"] = unique_id

            # Check if all refined versions exist
            all_refined_exist = True
            for model_name in MODELS_TO_CLEAN:
                refined_path = (
                    MARKDOWN_REFINED_COLLECTION_DIR
                    / f"{md_filename.stem}_{model_name}.md"
                )
                if not refined_path.exists():
                    all_refined_exist = False
                    break

            if not all_refined_exist:
                docs_to_process.append(doc_meta)
                log_event("check_existing.needs_processing", filename=filename)
            else:
                log_event("check_existing.skip_exists", filename=filename)

        log_event(
            "check_existing.complete",
            to_process=len(docs_to_process),
            skipped=len(docs_metadata) - len(docs_to_process),
        )

        if not docs_to_process:
            raise AirflowSkipException("All documents already processed")

        return docs_to_process

    @task()
    def download_pdf_from_s3(doc_meta: dict[str, Any]) -> dict[str, Any]:
        """Download a PDF from S3 if it doesn't exist locally.

        Args:
            doc_meta: Document metadata including S3 key

        Returns:
            Updated document metadata with local path
        """
        setup_python_path()
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
        from src.config import PDF_COLLECTION_DIR

        s3_key = doc_meta["s3_key"]
        filename = doc_meta["filename"]
        local_path = PDF_COLLECTION_DIR / filename

        # Create directory if needed
        PDF_COLLECTION_DIR.mkdir(parents=True, exist_ok=True)

        # Check if file already exists locally
        if local_path.exists():
            log_event("pdf.exists_locally", path=str(local_path))
            doc_meta["local_pdf_path"] = str(local_path)
            doc_meta["pdf_downloaded"] = True
            return doc_meta

        log_event("pdf.download.start", s3_key=s3_key, local_path=str(local_path))

        # Initialize S3 client
        s3_client = boto3.client(
            "s3",
            region_name=REGION_NAME,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        try:
            # Download file from S3
            s3_client.download_file(BUCKET_NAME, s3_key, str(local_path))
            log_event("pdf.download.success", path=str(local_path))
            doc_meta["local_pdf_path"] = str(local_path)
            doc_meta["pdf_downloaded"] = True

        except (ClientError, BotoCoreError) as e:
            log_event("pdf.download.error", error=str(e), s3_key=s3_key)
            # Try get_object as fallback
            try:
                response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
                with open(local_path, "wb") as f:
                    f.write(response["Body"].read())
                log_event("pdf.download.fallback_success", path=str(local_path))
                doc_meta["local_pdf_path"] = str(local_path)
                doc_meta["pdf_downloaded"] = True
            except Exception as fallback_error:
                log_event("pdf.download.fallback_error", error=str(fallback_error))
                doc_meta["pdf_downloaded"] = False
                doc_meta["download_error"] = str(fallback_error)
                raise

        return doc_meta

    @task()
    def parse_pdf_to_markdown(doc_meta: dict[str, Any]) -> dict[str, Any]:
        """Parse PDF to raw Markdown format.

        Args:
            doc_meta: Document metadata with local PDF path

        Returns:
            Updated metadata with markdown content
        """
        setup_python_path()
        from src.config import MARKDOWN_RAW_COLLECTION_DIR

        if not doc_meta.get("pdf_downloaded", False):
            log_event("parse.skip_no_pdf", filename=doc_meta.get("filename"))
            raise AirflowSkipException("PDF not downloaded")

        local_pdf_path = Path(doc_meta["local_pdf_path"])
        md_filename = doc_meta["md_filename"]
        md_path = MARKDOWN_RAW_COLLECTION_DIR / md_filename

        # Check if raw markdown already exists
        if md_path.exists():
            log_event("parse.markdown_exists", path=str(md_path))
            with open(md_path, encoding="utf-8") as f:
                markdown_content = f.read()
            if markdown_content.strip():
                doc_meta["markdown_content"] = markdown_content
                doc_meta["markdown_path"] = str(md_path)
                doc_meta["parsing_needed"] = False
                return doc_meta

        log_event("parse.start", pdf_path=str(local_pdf_path))

        try:
            if USE_SAAS_PDF_PARSER:
                # Use Azure Document Intelligence
                from langchain_community.document_loaders import (
                    AzureAIDocumentIntelligenceLoader,
                )

                loader = AzureAIDocumentIntelligenceLoader(
                    api_endpoint=AZURE_OPENAI_ENDPOINT,
                    api_key=AZURE_OPENAI_API_KEY,
                    file_path=str(local_pdf_path),
                    api_model="prebuilt-layout",
                    mode="markdown",
                    analysis_features=["ocrHighResolution"],
                )
            else:
                # Use local PDF parser
                from localPDFparse.parse import LocalPDFMarkdownLoader

                loader = LocalPDFMarkdownLoader(file_path=str(local_pdf_path))

            # Load and parse the document
            raw_docs = loader.load()

            if not raw_docs or not raw_docs[0].page_content.strip():
                log_event("parse.empty_result", pdf_path=str(local_pdf_path))
                doc_meta["parsing_error"] = "Empty content extracted"
                raise ValueError("Empty content extracted from PDF")

            markdown_content = raw_docs[0].page_content

            # Save raw markdown
            MARKDOWN_RAW_COLLECTION_DIR.mkdir(parents=True, exist_ok=True)
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(markdown_content)

            log_event(
                "parse.success", path=str(md_path), content_length=len(markdown_content)
            )

            doc_meta["markdown_content"] = markdown_content
            doc_meta["markdown_path"] = str(md_path)
            doc_meta["parsing_needed"] = True

        except Exception as e:
            log_event("parse.error", error=str(e), pdf_path=str(local_pdf_path))
            doc_meta["parsing_error"] = str(e)

            # Try fallback parser if using SaaS
            if USE_SAAS_PDF_PARSER:
                try:
                    log_event("parse.fallback.start")
                    from langchain_community.document_loaders import (
                        AzureAIDocumentIntelligenceLoader,
                    )

                    fallback_loader = AzureAIDocumentIntelligenceLoader(
                        api_endpoint=AZURE_OPENAI_ENDPOINT,
                        api_key=AZURE_OPENAI_API_KEY,
                        file_path=str(local_pdf_path),
                        api_model="prebuilt-read",
                        mode="text",
                    )

                    raw_docs = fallback_loader.load()
                    if raw_docs and raw_docs[0].page_content.strip():
                        markdown_content = raw_docs[0].page_content

                        with open(md_path, "w", encoding="utf-8") as f:
                            f.write(markdown_content)

                        log_event("parse.fallback.success")
                        doc_meta["markdown_content"] = markdown_content
                        doc_meta["markdown_path"] = str(md_path)
                        doc_meta["parsing_needed"] = True
                    else:
                        raise

                except Exception as fallback_error:
                    log_event("parse.fallback.error", error=str(fallback_error))
                    raise
            else:
                raise

        return doc_meta

    @task()
    def clean_markdown_with_llm(
        doc_meta: dict[str, Any], model_name: str
    ) -> dict[str, Any]:
        """Clean and refine markdown using LLM.

        Args:
            doc_meta: Document metadata with markdown content
            model_name: Name of the model to use for cleaning

        Returns:
            Metadata about the cleaning result
        """
        setup_python_path()
        import random
        import time

        from langchain_core.prompts import ChatPromptTemplate
        from pydantic import BaseModel, Field
        from src.config import MARKDOWN_REFINED_COLLECTION_DIR
        from src.utils import get_llm

        if "markdown_content" not in doc_meta:
            log_event("clean.skip_no_content", filename=doc_meta.get("filename"))
            raise AirflowSkipException("No markdown content to clean")

        md_filename = doc_meta["md_filename"]
        refined_filename = f"{Path(md_filename).stem}_{model_name}.md"
        refined_path = MARKDOWN_REFINED_COLLECTION_DIR / refined_filename

        # Check if already cleaned
        if refined_path.exists():
            log_event("clean.already_exists", model=model_name, path=str(refined_path))
            return {
                "filename": doc_meta["filename"],
                "model": model_name,
                "refined_path": str(refined_path),
                "status": "already_exists",
            }

        log_event("clean.start", model=model_name, filename=doc_meta["filename"])

        # Define cleaning prompt
        PROMPT_TO_CLEAN_MARKDOWN = """
        You are a helpful assistant that refines and improves the quality and
        structure of the markdown files.
        You have to achieve a fully human readable markdown text.
        ALWAYS do the following:
            - keep the ENTIRE content and data of all tables, always preserve it.
            - keep the ENTIRE content and information of all texts, always preserve it.

        PROHIBITIONS:
            - DO NOT remove any content from the markdown file, NEVER DO IT.
            - DO NOT remove any data from the markdown file, NEVER DO IT.
            - DO NOT remove any information from the markdown file, NEVER DO IT.
            - DO NOT remove any metadata from the markdown file, NEVER DO IT.
        """

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", PROMPT_TO_CLEAN_MARKDOWN),
                ("human", "{markdown_content}"),
            ]
        )

        class CleanMarkdown(BaseModel):
            """The cleaned markdown string."""

            cleaned_markdown: str = Field(description="The cleaned markdown string.")

        # Get LLM and create chain
        llm = get_llm(
            provider="azure" if "gpt" in model_name else "bedrock", model=model_name
        )
        chain = prompt | llm.with_structured_output(CleanMarkdown)

        # Try cleaning with retries
        last_error = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = chain.invoke(
                    {"markdown_content": doc_meta["markdown_content"]}
                )

                if not response.cleaned_markdown.strip():
                    raise ValueError("Empty cleaned markdown returned")

                # Save cleaned markdown
                MARKDOWN_REFINED_COLLECTION_DIR.mkdir(parents=True, exist_ok=True)
                with open(refined_path, "w", encoding="utf-8") as f:
                    f.write(response.cleaned_markdown)

                log_event(
                    "clean.success",
                    model=model_name,
                    path=str(refined_path),
                    content_length=len(response.cleaned_markdown),
                )

                return {
                    "filename": doc_meta["filename"],
                    "model": model_name,
                    "refined_path": str(refined_path),
                    "status": "success",
                    "content_length": len(response.cleaned_markdown),
                }

            except Exception as e:
                last_error = str(e)
                log_event(
                    "clean.retry", attempt=f"{attempt}/{MAX_RETRIES}", error=str(e)
                )

                if attempt < MAX_RETRIES:
                    # Exponential backoff with jitter
                    sleep_time = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    sleep_time += random.uniform(0, 0.5)
                    time.sleep(sleep_time)

        # All retries failed
        log_event("clean.failed", model=model_name, error=last_error)
        return {
            "filename": doc_meta["filename"],
            "model": model_name,
            "status": "failed",
            "error": last_error,
        }

    @task()
    def summarize_results(cleaning_results: list[dict[str, Any]]) -> dict[str, Any]:
        """Summarize the results of the entire pipeline.

        Args:
            cleaning_results: List of cleaning results from all documents and models

        Returns:
            Summary statistics
        """
        log_event("summary.start", total_results=len(cleaning_results))

        summary = {
            "total_documents": len(set(r["filename"] for r in cleaning_results)),
            "total_cleanings": len(cleaning_results),
            "successful": sum(
                1 for r in cleaning_results if r.get("status") == "success"
            ),
            "already_existed": sum(
                1 for r in cleaning_results if r.get("status") == "already_exists"
            ),
            "failed": sum(1 for r in cleaning_results if r.get("status") == "failed"),
            "models_used": list(set(r["model"] for r in cleaning_results)),
            "timestamp": datetime.now().isoformat(),
        }

        # Log failures for debugging
        failures = [r for r in cleaning_results if r.get("status") == "failed"]
        for failure in failures:
            log_event(
                "summary.failure_detail",
                filename=failure["filename"],
                model=failure["model"],
                error=failure.get("error"),
            )

        log_event(
            "summary.complete",
            successful=summary["successful"],
            failed=summary["failed"],
            already_existed=summary["already_existed"],
        )

        return summary

    # ====================================================================
    # DAG WORKFLOW DEFINITION
    # ====================================================================

    # Load metadata
    metadata = load_metadata()

    # Check which documents need processing
    docs_to_process = check_existing_files(metadata)

    # Process each document in parallel batches
    with TaskGroup("process_documents") as process_group:
        # Download PDFs
        downloaded_docs = download_pdf_from_s3.expand(doc_meta=docs_to_process)

        # Parse PDFs to Markdown
        parsed_docs = parse_pdf_to_markdown.expand(doc_meta=downloaded_docs)

    # Clean markdown with each model
    cleaning_results = []
    with TaskGroup("clean_markdown") as clean_group:
        for model in MODELS_TO_CLEAN:
            # Clean each document with this model
            model_results = clean_markdown_with_llm.partial(model_name=model).expand(
                doc_meta=parsed_docs
            )
            cleaning_results.append(model_results)

    # Flatten cleaning results and summarize

    @task()
    def flatten_results(*args) -> list[dict[str, Any]]:
        """Flatten nested cleaning results into a single list."""
        flattened = []
        for arg in args:
            if isinstance(arg, list):
                flattened.extend(arg)
            else:
                flattened.append(arg)
        return flattened

    # Flatten all cleaning results
    all_results = flatten_results(*cleaning_results)

    # Generate summary
    summary = summarize_results(all_results)

    # Define task dependencies
    (
        metadata
        >> docs_to_process
        >> process_group
        >> clean_group
        >> all_results
        >> summary
    )


# Instantiate the DAG
dag_instance = pdf_to_markdown_parser()
