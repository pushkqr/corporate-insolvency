"""Seed company management."""

from __future__ import annotations

import csv
from pathlib import Path

from .models import BankruptSeedCompany, SeedCompany


def default_seed_companies() -> list[SeedCompany]:
    tickers = [
        ("RELIANCE", "Reliance Industries"),
    ]
    return [SeedCompany(ticker=t[0], hint_name=t[1]) for t in tickers]


def load_seed_companies(seed_csv: Path | None) -> list[SeedCompany]:
    if seed_csv is None:
        return default_seed_companies()

    if not seed_csv.exists():
        raise FileNotFoundError(f"Seed file not found: {seed_csv}")

    companies: list[SeedCompany] = []
    with seed_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ticker = (row.get("ticker") or "").strip().upper()
            hint_name = (row.get("company_name") or ticker).strip()
            if ticker:
                companies.append(SeedCompany(ticker=ticker, hint_name=hint_name))

    if not companies:
        raise RuntimeError("No valid companies found in seed file.")
    return companies


def load_bankrupt_seed_companies(seed_csv: Path) -> list[BankruptSeedCompany]:
    if not seed_csv.exists():
        raise FileNotFoundError(f"Seed file not found: {seed_csv}")

    companies: list[BankruptSeedCompany] = []
    with seed_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ticker = (row.get("ticker") or "").strip().upper()
            company_name = (row.get("company_name") or ticker).strip()
            year_raw = (row.get("year") or "").strip()
            if not ticker or not year_raw:
                continue
            try:
                year = int(year_raw)
            except ValueError:
                continue
            companies.append(BankruptSeedCompany(ticker=ticker, company_name=company_name, year=year))

    if not companies:
        raise RuntimeError("No valid bankrupt companies found in seed file.")
    return companies
