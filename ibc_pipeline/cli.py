"""CLI orchestration for extract stages."""

from __future__ import annotations

import argparse
from pathlib import Path

from .config import configure_logging, load_environment
from .pipeline import run_bankrupt_pipeline
from .extractor import run_extract_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build insolvency dataset.")
    parser.add_argument(
        "--pipeline",
        choices=["listed", "bankrupt"],
        default="listed",
        help="Choose listed (yfinance) or bankrupt (PDF) pipeline.",
    )
    parser.add_argument(
        "--mode",
        choices=["extract"],
        default="extract",
        help="Pipeline mode: extract base data.",
    )
    parser.add_argument("--output", default="insolvency_dataset.csv", help="Base extraction output CSV path.")
    parser.add_argument("--seed-csv", default="", help="Optional seed CSV with columns: ticker,company_name")
    parser.add_argument(
        "--bankrupt-seed-csv",
        default="ibc_pipeline/seeds/bankrupt_seeds.csv",
        help="Seed CSV for bankrupt pipeline with columns: ticker,company_name,year",
    )
    parser.add_argument("--sleep-seconds", type=float, default=2.0, help="Throttle duration between companies.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of rows in the current mode run. If omitted, process all rows.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()
    load_environment()

    if args.pipeline == "bankrupt":
        seed_csv = Path(args.bankrupt_seed_csv)
        output_csv = Path(args.output)
        run_bankrupt_pipeline(
            seed_csv=seed_csv,
            output_csv=output_csv,
            sleep_seconds=args.sleep_seconds,
            limit=args.limit,
        )
        return

    if args.mode == "extract":
        output_csv = Path(args.output)
        seed_csv = Path(args.seed_csv) if args.seed_csv else None
        run_extract_pipeline(
            output_csv=output_csv,
            seed_csv=seed_csv,
            sleep_seconds=args.sleep_seconds,
            limit=args.limit,
        )
