"""CSV and row I/O helpers."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pandas as pd

from .constants import KNOWN_POSITIVE_TICKERS, OUTPUT_COLUMNS
from .models import SeedCompany


def finance_url_from_ticker(company: SeedCompany) -> str:
    return f"https://finance.yahoo.com/quote/{company.ticker}.NS/balance-sheet/"


def build_base_row(company: SeedCompany) -> dict[str, Any]:
    return {
        "ticker": company.ticker,
        "company_name": company.hint_name,
        "sector": "",
        "industry": "",
        "business_summary": "",
        "target": 1 if company.ticker in KNOWN_POSITIVE_TICKERS else 0,
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
        "fiscal_year": "",
        "currency": "INR",
        "source_url": "",
        "extraction_status": "metadata_only",
        "extraction_error": "",
    }


def ensure_csv_header(output_csv: Path) -> None:
    if output_csv.exists():
        with output_csv.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, [])
        if header and header != OUTPUT_COLUMNS:
            raise RuntimeError(
                "Existing output CSV schema does not match current schema. "
                "Please remove the CSV or use a new output path."
            )
        return
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()


def append_row(output_csv: Path, row: dict[str, Any]) -> None:
    with output_csv.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writerow({key: row.get(key) for key in OUTPUT_COLUMNS})


def run_logic_audit(output_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(output_csv)
    if df.empty:
        return df

    anomalies = df[
        (df["total_debt"].fillna(0) < 0)
        | (df["current_liabilities"].fillna(0) < 0)
        | (
            df["intangible_assets"].notna()
            & df["total_assets"].notna()
            & (df["intangible_assets"] > df["total_assets"])
        )
        | (
            df["cash_and_equivalents"].notna()
            & df["total_assets"].notna()
            & (df["cash_and_equivalents"] > df["total_assets"])
        )
    ]
    return anomalies
