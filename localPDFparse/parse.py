"""Robust PDF to Markdown extraction using PyMuPDF4LLM.

Provides a small, reusable API to extract refined Markdown with
header detection based on the document's TOC (fallback to font-size
analysis). Defaults favor accurate text capture, preserving graphics
for table detection while ignoring images.
"""

# %%
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import pymupdf  # type: ignore
import pymupdf4llm


def _choose_header_identifier(
    doc: "pymupdf.Document",
    *,
    prefer_toc: bool = True,
    max_levels: int = 3,
) -> Any:
    """Return a header identifier suitable for to_markdown(hdr_info=...).

    Prefers TOC-based headers if available; otherwise falls back to
    font-size-based identification limited to a maximum depth.
    """
    toc = doc.get_toc()

    if prefer_toc and toc:
        return pymupdf4llm.TocHeaders(doc)

    return pymupdf4llm.IdentifyHeaders(doc, max_levels=max_levels)


def _is_header_sep(line: str) -> bool:
    """Heuristic: detect a Markdown table header separator line.

    Examples: "| --- | --- |" or "--- | :---: | ---:" with or without
    leading / trailing pipes.
    """
    text = line.strip()
    if not text:
        return False
    if "|" not in text and "-" not in text:
        return False
    # Must contain at least 3 dashes somewhere
    if "---" not in text:
        return False
    # Avoid bullets / horizontal rules confusion
    if set(text) <= {"-", " ", "|", ":"}:
        return True
    # Mixed with pipes is also a strong indicator
    if "|" in text and "-" in text:
        return True
    return False


def _split_md_row(row: str) -> list[str]:
    """Split a Markdown table row into cells, trimming outer pipes.

    This is a best-effort splitter and does not handle all edge cases
    like escaped pipes within cells.
    """
    s = row.strip()
    if s.startswith("|"):
        s = s[1:]
    if s.endswith("|"):
        s = s[:-1]
    parts = [part.strip() for part in s.split("|")]
    return parts


def _find_markdown_tables(md_text: str) -> list[list[list[str]]]:
    """Find tables in Markdown and return them as list of rows.

    Each table is a list of rows; each row is a list of cell strings.
    """
    lines = md_text.splitlines()
    tables: list[list[list[str]]] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if _is_header_sep(line) and i > 0:
            header_idx = i - 1
            header_line = lines[header_idx]
            # Basic validity: header row should contain pipes or multiple
            # columns separated by pipes
            if "|" not in header_line:
                i += 1
                continue

            rows: list[list[str]] = []
            rows.append(_split_md_row(header_line))
            # Skip the header separator line
            i += 1
            # Collect body rows until a blank or non-table line
            while i < len(lines):
                candidate = lines[i]
                if not candidate.strip():
                    break
                if "|" not in candidate:
                    break
                rows.append(_split_md_row(candidate))
                i += 1

            if len(rows) >= 2:
                tables.append(rows)
        else:
            i += 1
    return tables


def _normalize_markdown_tables(md_text: str) -> str:
    """Normalize Markdown tables: fix pipes and align column counts.

    This improves downstream parsing and readability by making sure
    each table row has a consistent number of columns.
    """
    lines = md_text.splitlines()
    out: list[str] = []
    i = 0
    while i < len(lines):
        if _is_header_sep(lines[i]) and i > 0 and "|" in lines[i - 1]:
            header = lines[i - 1]
            # Determine expected number of columns from header
            expected_cols = len(_split_md_row(header))

            # Rebuild header and separator
            header_cells = _split_md_row(header)
            header_line = "| " + " | ".join(header_cells) + " |"
            sep_cells = ["---"] * expected_cols
            sep_line = "| " + " | ".join(sep_cells) + " |"

            # Replace previous line already appended? If header was just
            # appended, remove it. Otherwise, adjust output stream.
            if out and out[-1].strip() == header.strip():
                out.pop()
            out.append(header_line)
            out.append(sep_line)

            i += 1
            # Append body rows normalized
            while i + 1 < len(lines) and "|" in lines[i + 1]:
                i += 1
                row_cells = _split_md_row(lines[i])
                # Pad or trim to expected column count
                if len(row_cells) < expected_cols:
                    row_cells += [""] * (expected_cols - len(row_cells))
                elif len(row_cells) > expected_cols:
                    row_cells = row_cells[:expected_cols]
                out.append("| " + " | ".join(row_cells) + " |")
            i += 1
            continue

        out.append(lines[i])
        i += 1

    return "\n".join(out)


def _render_table(rows: list[list[str]]) -> str:
    """Render a table (rows of cells) into Markdown table syntax."""
    if not rows:
        return ""
    header = rows[0]
    body = rows[1:]
    header_line = "| " + " | ".join(header) + " |"
    sep_line = "| " + " | ".join(["---"] * len(header)) + " |"
    body_lines = ["| " + " | ".join(r) + " |" for r in body]
    return "\n".join([header_line, sep_line, *body_lines])


def _append_annex(
    md_text: str,
    *,
    tables: list[list[list[str]]],
    annex_title: str,
) -> str:
    """Append a tables annex to the given Markdown text."""
    if not tables:
        return md_text
    parts: list[str] = [md_text, "", annex_title]
    for idx, table in enumerate(tables, start=1):
        parts.append(f"### Tabla {idx}")
        parts.append(_render_table(table))
    return "\n\n".join(parts)


def extract_markdown(
    input_path: str | Path,
    *,
    ignore_images: bool = True,
    ignore_graphics: bool = False,
    table_strategy: str = "lines_strict",
    prefer_toc_headers: bool = True,
    max_header_levels: int = 3,
    force_text: bool = True,
    margins: float | tuple[float, float] | tuple[float, float, float, float] = 0,
    page_chunks: bool = False,
    page_separators: bool = False,
    pages: Iterable[int] | range | None = None,
    show_progress: bool = False,
    output_dir: str | Path | None = "localPDFparse/markdown",
    overwrite: bool = True,
    encoding: str = "utf-8",
    use_glyphs: bool = True,
    normalize_tables: bool = True,
    append_tables_annex: bool = True,
    annex_title: str = "## Anexo: Tablas detectadas",
) -> str | list[dict]:
    """Extract Markdown from a PDF with optimized, robust defaults.

    Defaults are tuned for high-fidelity text and table recovery:
    - Header detection prefers TOC if present; falls back to font sizes.
    - Images ignored; graphics kept for table detection.
    - Table strategy "lines_strict" (with fallbacks) and use_glyphs=True
      by default.
    - Tables are normalized and appended as a Markdown annex.

    Returns Markdown text (or page chunks when page_chunks=True).
    """
    pdf_path = str(input_path)
    doc = pymupdf.open(pdf_path)

    hdr_info = _choose_header_identifier(
        doc,
        prefer_toc=prefer_toc_headers,
        max_levels=max_header_levels,
    )

    # Important: table detection requires graphics; do not set ignore_graphics
    # when you want to detect tables.
    md: str | list[dict]
    tried_strategies: list[str] = []
    fallback_order = [table_strategy, "lines_strict", "lines", "text"]
    # de-duplicate while preserving order
    seen: set[str] = set()
    strategies: list[str] = []
    for s in fallback_order:
        if s not in seen:
            seen.add(s)
            strategies.append(s)

    last_error: Exception | None = None
    for strat in strategies:
        tried_strategies.append(strat)
        try:
            md = pymupdf4llm.to_markdown(
                doc,
                detect_bg_color=True,
                embed_images=False,
                extract_words=False,
                force_text=force_text,
                hdr_info=hdr_info,
                ignore_alpha=False,
                ignore_code=False,
                ignore_graphics=ignore_graphics,
                ignore_images=ignore_images,
                image_format="png",
                image_path="",
                image_size_limit=0.05,
                margins=margins,
                page_chunks=page_chunks,
                page_separators=page_separators,
                pages=list(pages) if pages is not None else None,
                show_progress=show_progress,
                table_strategy=strat,
                use_glyphs=use_glyphs,
                write_images=False,
            )
            break
        except ValueError as err:
            last_error = err
            continue
    else:
        # No strategy succeeded
        if last_error is not None:
            raise last_error
        raise RuntimeError("Table extraction failed for all strategies")

    # Optionally normalize Markdown tables for better fidelity.
    if not isinstance(md, list) and normalize_tables:
        md = _normalize_markdown_tables(md)

    # Prepare tables for optional annex (only for string output mode).
    tables_for_annex: list[list[list[str]]] = []
    if not isinstance(md, list) and append_tables_annex:
        tables_for_annex = _find_markdown_tables(md)

    # Optionally write to disk, keeping the PDF stem as the filename.
    if output_dir is not None:
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        pdf_name = Path(pdf_path).stem + ".md"
        out_path = out_dir / pdf_name

        if not overwrite and out_path.exists():
            # Find a unique name: "name (n).md"
            counter = 1
            while True:
                candidate = out_dir / (f"{Path(pdf_path).stem} ({counter}).md")
                if not candidate.exists():
                    out_path = candidate
                    break
                counter += 1

        if isinstance(md, list):
            # Combine page chunks into a single markdown string.
            combined = "\n\n".join(page.get("markdown", "") for page in md)
            if append_tables_annex:
                tables_combined = _find_markdown_tables(combined)
                combined = _append_annex(
                    combined,
                    tables=tables_combined,
                    annex_title=annex_title,
                )
            out_path.write_text(combined, encoding=encoding)
        else:
            final_text = md
            if append_tables_annex:
                final_text = _append_annex(
                    md,
                    tables=tables_for_annex,
                    annex_title=annex_title,
                )
            out_path.write_text(final_text, encoding=encoding)

    # Return value mirrors input mode; include annex when returning str.
    if isinstance(md, list):
        return md
    if append_tables_annex:
        return _append_annex(md, tables=tables_for_annex, annex_title=annex_title)
    return md


if __name__ == "__main__":
    # Simple, side-effect-safe demo
    sample = (
        "localPDFparse/test_pdf/00.-Informe-T_e_cnico-"
        "Flora-y-Vegetaci_o_n-Valle-Noble.pdf"
    )

    result = extract_markdown(
        sample,
        # Most users can rely on defaults. Only override output_dir here.
        output_dir="localPDFparse/markdown",
    )

    if isinstance(result, list):
        text_preview = result[0].get("markdown", "") if result else ""
    else:
        text_preview = result

    preview = text_preview[:800]
    print(f"Markdown length: {len(text_preview)}")
    print("Preview:\n" + preview)

    # Print saved path for convenience
    saved_path = Path("localPDFparse/markdown") / (Path(sample).stem + ".md")
    print(f"Saved to: {saved_path}")
