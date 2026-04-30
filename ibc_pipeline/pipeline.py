"""Bankrupt PDF pipeline orchestration."""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from .constants import OUTPUT_COLUMNS
from .io_utils import append_row, ensure_csv_header
from .llm_extractor import extract_metrics_with_gemini
from .models import BankruptSeedCompany
from .pdf_extractor import extract_financial_cherrypick
from .pdf_fetcher import NSEDATAPipeline
from .seeds import load_bankrupt_seed_companies
from .utils import to_float


LOGGER = logging.getLogger("dataset_builder")


class LLMFinancialMetrics(BaseModel):
    total_debt: float | None = None
    intangible_assets: float | None = None
    cash_and_equivalents: float | None = None
    current_liabilities: float | None = None
    operating_cash_flow: float | None = None
    interest_expense: float | None = None
    net_income: float | None = None
    total_assets: float | None = None
    profit_before_tax: float | None = None
    depreciation_and_amortization: float | None = None


def _apply_llm_metrics(row: dict[str, Any], metrics: dict[str, Any]) -> None:
    row["total_debt"] = to_float(metrics.get("total_debt"))
    row["intangible_assets"] = to_float(metrics.get("intangible_assets"))
    row["cash_and_equivalents"] = to_float(metrics.get("cash_and_equivalents"))
    row["current_liabilities"] = to_float(metrics.get("current_liabilities"))
    row["operating_cash_flow"] = to_float(metrics.get("operating_cash_flow"))
    row["interest_expense"] = to_float(metrics.get("interest_expense"))
    row["net_income"] = to_float(metrics.get("net_income"))
    row["total_assets"] = to_float(metrics.get("total_assets"))
    pbt = to_float(metrics.get("profit_before_tax")) or 0.0
    interest = to_float(metrics.get("interest_expense")) or 0.0
    da = to_float(metrics.get("depreciation_and_amortization")) or 0.0
    row["ebitda"] = pbt + interest + da


def _parse_llm_json(payload: str) -> dict[str, Any]:
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", payload, re.DOTALL | re.IGNORECASE)
        if fenced:
            return json.loads(fenced.group(1))
        trimmed = payload.strip()
        start = trimmed.find("{")
        end = trimmed.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(trimmed[start : end + 1])
        raise


def _build_base_row(company: BankruptSeedCompany) -> dict[str, Any]:
    row = {key: None for key in OUTPUT_COLUMNS}
    row.update(
        {
            "ticker": company.ticker,
            "company_name": company.company_name,
            "sector": "",
            "industry": "",
            "business_summary": "",
            "target": 1,
            "market_cap": None,
            "total_debt": None,
            "intangible_assets": None,
            "cash_and_equivalents": None,
            "current_liabilities": None,
            "operating_cash_flow": None,
            "ebitda": None,
            "interest_expense": None,
            "net_income": None,
            "total_assets": None,
            "fiscal_year": str(company.year),
            "currency": "INR",
            "source_url": "",
            "extraction_status": "",
            "extraction_error": "",
        }
    )
    return row


def run_bankrupt_pipeline(
    seed_csv: Path,
    output_csv: Path,
    sleep_seconds: float,
    limit: int | None = None,
    min_density: float = 0.12,
    model_name: str | None = None,
    system_prompt: str | None = None,
) -> None:
    ensure_csv_header(output_csv)
    companies = load_bankrupt_seed_companies(seed_csv)
    if limit is not None:
        companies = companies[:limit]
    pipeline = NSEDATAPipeline()
    data_dir = Path(__file__).resolve().parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Starting bankrupt PDF run for %d companies", len(companies))

    for company in companies:
        row = _build_base_row(company)
        LOGGER.debug("Processing bankrupt ticker=%s year=%s", company.ticker, company.year)
        raw_pdf = data_dir / f"{company.ticker}_{company.year}_AR.pdf"
        ok, report_year, source_url, error = pipeline.download_annual_report(
            ticker=company.ticker,
            target_year=company.year,
            save_path=raw_pdf,
        )
        if not ok:
            row["extraction_status"] = "annual_report_fetch_failed"
            row["extraction_error"] = error or "fetch_failed"
            row["source_url"] = source_url or ""
            append_row(output_csv, row)
            LOGGER.warning("Annual report download failed for %s: %s", company.ticker, error)
            time.sleep(sleep_seconds)
            continue

        if report_year is not None:
            row["fiscal_year"] = str(report_year)
        row["source_url"] = source_url or ""
        LOGGER.debug("Annual report ready ticker=%s fiscal_year=%s", company.ticker, row["fiscal_year"])

        dense_name = f"{company.ticker}_{row['fiscal_year']}_dense.pdf"
        dense_path = data_dir / dense_name

        success, mode, statements, pages = extract_financial_cherrypick(
            pdf_path=str(raw_pdf),
            output_path=str(dense_path),
            min_density=min_density,
        )

        if not success:
            row["extraction_status"] = "pdf_parse_failed"
            row["extraction_error"] = "pdf_parse_failed"
            append_row(output_csv, row)
            LOGGER.warning("PDF parse failed for %s year=%s", company.ticker, row["fiscal_year"])
            time.sleep(sleep_seconds)
            continue

        LOGGER.debug(
            "PDF parse success ticker=%s year=%s mode=%s statements=%s pages=%s",
            company.ticker,
            row["fiscal_year"],
            mode,
            ",".join(statements),
            pages,
        )

        result_json = extract_metrics_with_gemini(
            pdf_path=str(dense_path),
            system_prompt=system_prompt,
            model_name=model_name,
        )

        if not result_json:
            row["extraction_status"] = "llm_empty_response"
            row["extraction_error"] = "llm_empty_response"
            append_row(output_csv, row)
            LOGGER.warning("LLM empty response for %s year=%s", company.ticker, row["fiscal_year"])
            time.sleep(sleep_seconds)
            continue

        try:
            metrics_raw = _parse_llm_json(result_json)
            metrics = LLMFinancialMetrics.model_validate(metrics_raw).model_dump()
            _apply_llm_metrics(row, metrics)
            row["extraction_status"] = f"pdf_llm_{mode.lower()}"
            row["extraction_error"] = ""
            LOGGER.info("LLM extraction succeeded ticker=%s year=%s", company.ticker, row["fiscal_year"])
        except (json.JSONDecodeError, ValidationError, TypeError) as exc:
            row["extraction_status"] = "llm_json_error"
            row["extraction_error"] = str(exc)[:1000]
            preview = result_json.strip().replace("\n", " ")[:200]
            LOGGER.warning(
                "LLM extraction failed ticker=%s year=%s error=%s preview=%s",
                company.ticker,
                row["fiscal_year"],
                exc,
                preview,
            )
            response_path = data_dir / f"{company.ticker}_{row['fiscal_year']}_llm_response.txt"
            try:
                response_path.write_text(result_json, encoding="utf-8")
                LOGGER.info("Saved LLM response ticker=%s path=%s", company.ticker, response_path)
            except Exception as write_err:
                LOGGER.warning("Failed to write LLM response ticker=%s error=%s", company.ticker, write_err)

        append_row(output_csv, row)
        LOGGER.info("BANKRUPT row=%s year=%s status=%s", company.ticker, row["fiscal_year"], row["extraction_status"])
        time.sleep(sleep_seconds)
