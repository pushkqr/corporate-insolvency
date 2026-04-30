"""PDF-based financial statement cherrypick."""

from __future__ import annotations

import re
from typing import Iterable

try:
    import fitz
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("PyMuPDF is required. Install with pip install PyMuPDF") from exc


PATTERNS = {
    "Consolidated": {
        "BS": r"(?i)consolidated\s+balance\s+sheet\s+as\s+at\s+(?:31st\s+march|march\s+31|31\s+march)\s*,?\s*20\d{2}",
        "PL": r"(?i)consolidated\s+statement\s+of\s+profit\s+(?:and|&)\s+loss\s+for\s+the\s+year\s+ended\s+(?:31st\s+march|march\s+31|31\s+march)\s*,?\s*20\d{2}",
        "CF": r"(?i)consolidated\s+cash\s+flow\s+statement\s+for\s+the\s+year\s+ended\s+(?:31st\s+march|march\s+31|31\s+march)\s*,?\s*20\d{2}",
    },
    "Standalone": {
        "BS": r"(?i)(?:standalone\s+)?balance\s+sheet\s+as\s+at\s+(?:31st\s+march|march\s+31|31\s+march)\s*,?\s*20\d{2}",
        "PL": r"(?i)(?:standalone\s+)?statement\s+of\s+profit\s+(?:and|&)\s+loss\s+for\s+the\s+year\s+ended\s+(?:31st\s+march|march\s+31|31\s+march)\s*,?\s*20\d{2}",
        "CF": r"(?i)(?:standalone\s+)?cash\s+flow\s+statement\s+for\s+the\s+year\s+ended\s+(?:31st\s+march|march\s+31|31\s+march)\s*,?\s*20\d{2}",
    },
}


def get_number_density(text: str) -> float:
    if not text:
        return 0.0
    numbers = sum(ch.isdigit() for ch in text)
    return numbers / len(text)


def find_page(doc: fitz.Document, regex: str, min_density: float) -> int | None:
    for page_num in range(len(doc)):
        text = doc[page_num].get_text("text")
        if regex and text:
            if re.search(regex, text):
                if get_number_density(text) > min_density:
                    return page_num
    return None


def _unique_sorted_pages(pages: Iterable[int], doc_len: int) -> list[int]:
    return sorted({p for p in pages if 0 <= p < doc_len})


def extract_financial_cherrypick(
    pdf_path: str,
    output_path: str,
    min_density: float = 0.12,
) -> tuple[bool, str, list[str], list[int]]:
    doc = fitz.open(pdf_path)

    for mode in ["Consolidated", "Standalone"]:
        pages_to_keep: set[int] = set()
        statements_found: list[str] = []

        bs_page = find_page(doc, PATTERNS[mode]["BS"], min_density)
        pl_page = find_page(doc, PATTERNS[mode]["PL"], min_density)
        cf_page = find_page(doc, PATTERNS[mode]["CF"], min_density)

        if bs_page is not None:
            pages_to_keep.update([bs_page, bs_page + 1])
            statements_found.append("BS")
        if pl_page is not None:
            pages_to_keep.update([pl_page, pl_page + 1])
            statements_found.append("PL")
        if cf_page is not None:
            pages_to_keep.update([cf_page, cf_page + 1, cf_page + 2])
            statements_found.append("CF")

        if statements_found:
            valid_pages = _unique_sorted_pages(pages_to_keep, len(doc))
            doc.select(valid_pages)
            doc.save(output_path)
            doc.close()
            return True, mode, statements_found, valid_pages

    doc.close()
    return False, "Not Found", [], []
