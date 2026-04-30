"""CLI orchestration for extract/enrich stages."""

from __future__ import annotations

import argparse
from pathlib import Path

from .config import configure_logging, load_environment
from .pipeline import run_bankrupt_pipeline
from .enricher import run_enrich_pipeline
from .extractor import run_extract_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and enrich insolvency dataset.")
    parser.add_argument(
        "--pipeline",
        choices=["listed", "bankrupt"],
        default="listed",
        help="Choose listed (yfinance) or bankrupt (PDF) pipeline.",
    )
    parser.add_argument(
        "--mode",
        choices=["extract", "enrich", "all"],
        default="extract",
        help="Pipeline mode: extract base data, enrich existing data, or run both.",
    )
    parser.add_argument("--output", default="insolvency_dataset.csv", help="Base extraction output CSV path.")
    parser.add_argument("--input", default="insolvency_dataset.csv", help="Input CSV path for enrich mode.")
    parser.add_argument(
        "--enriched-output",
        default="insolvency_dataset_enriched.csv",
        help="Output CSV path for enrich mode.",
    )
    parser.add_argument("--seed-csv", default="", help="Optional seed CSV with columns: ticker,company_name")
    parser.add_argument(
        "--bankrupt-seed-csv",
        default="ibc_pipeline/bankrupt_seeds.csv",
        help="Seed CSV for bankrupt pipeline with columns: ticker,company_name,year",
    )
    parser.add_argument("--sleep-seconds", type=float, default=2.0, help="Throttle duration between companies.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of rows in the current mode run. If omitted, process all rows.",
    )
    parser.add_argument(
        "--safe-mode",
        action="store_true",
        help="Use conservative local-LLM generation settings for stability when fallback is active.",
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

    if args.mode in {"extract", "all"}:
        output_csv = Path(args.output)
        seed_csv = Path(args.seed_csv) if args.seed_csv else None
        run_extract_pipeline(
            output_csv=output_csv,
            seed_csv=seed_csv,
            sleep_seconds=args.sleep_seconds,
            limit=args.limit,
        )

    if args.mode in {"enrich", "all"}:
        input_csv = Path(args.input if args.mode == "enrich" else args.output)
        enriched_output = Path(args.enriched_output)
        run_enrich_pipeline(
            input_csv=input_csv,
            output_csv=enriched_output,
            sleep_seconds=args.sleep_seconds,
            limit=args.limit,
            safe_mode=args.safe_mode,
        )
