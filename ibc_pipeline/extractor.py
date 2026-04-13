"""yfinance quantitative extraction stage."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

from .constants import KNOWN_POSITIVE_TICKERS
from .io_utils import append_row, build_base_row, ensure_csv_header, finance_url_from_ticker, run_logic_audit
from .models import SeedCompany
from .seeds import load_seed_companies
from .utils import norm_label, prefer_numeric, to_float


LOGGER = logging.getLogger("dataset_builder")


def _value_from_statement(statement: pd.DataFrame, label_candidates: list[str], column: Any) -> float | None:
    if statement is None or statement.empty:
        return None

    normalized_map = {norm_label(str(idx)): idx for idx in statement.index}
    for candidate in label_candidates:
        idx = normalized_map.get(norm_label(candidate))
        if idx is None:
            continue
        parsed = to_float(statement.at[idx, column])
        if parsed is not None:
            return parsed
    return None


def _pick_latest_column(statement: pd.DataFrame) -> Any:
    if statement is None or statement.empty or len(statement.columns) == 0:
        return None
    return statement.columns[0]


def parse_metrics_with_yfinance(company: SeedCompany) -> dict[str, Any]:
    symbol = f"{company.ticker}.NS"
    ticker_obj = yf.Ticker(symbol)
    balance_sheet = ticker_obj.balance_sheet
    income_statement = ticker_obj.financials
    cash_flow_statement = ticker_obj.cashflow

    if balance_sheet is None or balance_sheet.empty:
        raise RuntimeError("Empty balance sheet from yfinance")

    latest_col = _pick_latest_column(balance_sheet)
    if latest_col is None:
        raise RuntimeError("No reporting columns in balance sheet")

    total_debt = _value_from_statement(balance_sheet, ["Total Debt", "Borrowings", "Total Borrowings"], latest_col)
    if total_debt is None:
        long_term = _value_from_statement(balance_sheet, ["Long Term Debt", "Long Term Borrowings"], latest_col)
        current = _value_from_statement(balance_sheet, ["Current Debt", "Short Term Debt", "Short Term Borrowings"], latest_col)
        total_debt = (long_term or 0.0) + (current or 0.0)

    intangible_assets = _value_from_statement(
        balance_sheet,
        ["Goodwill And Other Intangible Assets", "Other Intangible Assets", "Intangible Assets", "Goodwill"],
        latest_col,
    )
    cash_and_equivalents = _value_from_statement(
        balance_sheet,
        ["Cash Cash Equivalents And Short Term Investments", "Cash And Cash Equivalents", "Cash Equivalents"],
        latest_col,
    )
    current_liabilities = _value_from_statement(balance_sheet, ["Current Liabilities", "Total Current Liabilities"], latest_col)
    total_assets = _value_from_statement(balance_sheet, ["Total Assets"], latest_col)

    net_income = None
    ebitda = None
    interest_expense = None
    if income_statement is not None and not income_statement.empty:
        income_col = _pick_latest_column(income_statement)
        net_income = _value_from_statement(
            income_statement,
            ["Net Income", "Net Income Common Stockholders", "Net Income Continuous Operations", "Normalized Income"],
            income_col,
        )
        ebitda = _value_from_statement(income_statement, ["EBITDA"], income_col)
        interest_expense = _value_from_statement(
            income_statement,
            ["Interest Expense", "Interest Expense Non Operating", "Interest Expense Operating"],
            income_col,
        )

    operating_cash_flow = None
    if cash_flow_statement is not None and not cash_flow_statement.empty:
        cash_col = _pick_latest_column(cash_flow_statement)
        operating_cash_flow = _value_from_statement(
            cash_flow_statement,
            [
                "Operating Cash Flow",
                "Cash Flow From Continuing Operating Activities",
                "Net Cash Provided By Operating Activities",
            ],
            cash_col,
        )

    try:
        info = ticker_obj.fast_info or {}
        currency = str(info.get("currency") or "INR")
        market_cap = to_float(info.get("market_cap"))
    except Exception:
        currency = "INR"
        market_cap = None

    company_name = company.hint_name
    sector = ""
    industry = ""
    business_summary = ""
    try:
        info = ticker_obj.info or {}
        company_name = str(info.get("shortName") or company_name)
        sector = str(info.get("sector") or "")
        industry = str(info.get("industry") or "")
        business_summary = str(info.get("longBusinessSummary") or "")
        if market_cap is None:
            market_cap = to_float(info.get("marketCap"))
        if ebitda is None:
            ebitda = to_float(info.get("ebitda"))
    except Exception:
        pass

    fiscal_year = ""
    try:
        fiscal_year = str(getattr(latest_col, "year", "") or "")
    except Exception:
        pass

    return {
        "ticker": company.ticker,
        "company_name": company_name,
        "sector": sector,
        "industry": industry,
        "business_summary": business_summary,
        "target": 1 if company.ticker in KNOWN_POSITIVE_TICKERS else 0,
        "market_cap": market_cap,
        "total_debt": prefer_numeric(total_debt, 0),
        "intangible_assets": prefer_numeric(intangible_assets, 0),
        "cash_and_equivalents": prefer_numeric(cash_and_equivalents, 0),
        "current_liabilities": prefer_numeric(current_liabilities, 0),
        "operating_cash_flow": operating_cash_flow,
        "ebitda": ebitda,
        "interest_expense": interest_expense,
        "net_income": net_income,
        "total_assets": total_assets,
        "fiscal_year": fiscal_year,
        "currency": currency,
        "source_url": finance_url_from_ticker(company),
    }


def run_extract_pipeline(output_csv: Path, seed_csv: Path | None, sleep_seconds: float, limit: int | None) -> None:
    companies = load_seed_companies(seed_csv)
    if limit is not None:
        companies = companies[:limit]
    LOGGER.info("Starting extraction run for %d companies", len(companies))

    ensure_csv_header(output_csv)
    processed = 0

    for company in companies:
        LOGGER.debug("Processing ticker=%s hint_name=%s", company.ticker, company.hint_name)
        row = build_base_row(company)
        error_messages: list[str] = []
        source_url = finance_url_from_ticker(company)
        row["source_url"] = source_url

        try:
            yf_row = parse_metrics_with_yfinance(company)
            for key, value in yf_row.items():
                if key not in row:
                    continue
                if value is None:
                    continue
                if isinstance(value, str) and not value.strip():
                    continue
                row[key] = value
            row["extraction_status"] = "yfinance"
        except Exception as err:
            error_messages.append(f"yfinance:{err}")
            row["extraction_status"] = "metadata_only"
            LOGGER.warning("yfinance extraction failed for %s: %s", company.ticker, err)

        if error_messages:
            row["extraction_error"] = " | ".join(error_messages)[:2000]

        append_row(output_csv, row)
        processed += 1
        LOGGER.info("ROW %s status=%s", company.ticker, row["extraction_status"])
        time.sleep(sleep_seconds)

    LOGGER.info("Finished. Processed=%d, Total=%d", processed, len(companies))
    anomalies = run_logic_audit(output_csv)
    if anomalies.empty:
        LOGGER.info("Logic audit: no anomalies found.")
    else:
        LOGGER.warning("Logic audit anomalies found: %d", len(anomalies))
        LOGGER.warning(
            "%s",
            anomalies[
                [
                    "ticker",
                    "total_debt",
                    "current_liabilities",
                    "net_income",
                    "intangible_assets",
                    "cash_and_equivalents",
                    "total_assets",
                ]
            ].to_string(index=False),
        )
