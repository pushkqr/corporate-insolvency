"""AI enrichment stage."""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .config import resolve_enrich_model, resolve_llm_api_key
from .constants import ENRICH_COLUMNS
from .utils import safe_text, to_plain_number

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover
    OpenAI = None


LOGGER = logging.getLogger("dataset_builder")


@dataclass
class LLMEndpoint:
    name: str
    base_url: str | None
    api_key: str
    model_name: str


def _build_system_prompt() -> str:
    return (
        "You are an expert Insolvency and Bankruptcy Code (IBC) analyst. "
        "Assess liquidation risk using only the provided company data fields from yfinance. "
        "Do not use insolvency labels or targets. "
        "Task: "
        "1) Return ibc_liquidation_risk as exactly one of High, Medium, Low. "
        "2) Return asset_illusion_rationale as exactly one sentence. "
        "Risk guidance: "
        "Do not assign High from a single signal (for example high intangibles) alone. "
        "Consider whether earnings and cash generation can service obligations. "
        "Consider asset liquidity from business type (physical-assets-heavy vs brand or IP-heavy). "
        "Use Medium when evidence is mixed. "
        "Output strictly JSON with keys ibc_liquidation_risk and asset_illusion_rationale."
    )


def _normalize_ai_output(parsed: dict[str, Any]) -> dict[str, str]:
    risk = safe_text(parsed.get("ibc_liquidation_risk"))
    if risk not in {"High", "Medium", "Low"}:
        risk = "Medium"

    rationale = safe_text(parsed.get("asset_illusion_rationale"))
    if not rationale:
        rationale = "Insufficient context to generate a specific liquidation rationale."

    return {
        "ibc_liquidation_risk": risk,
        "asset_illusion_rationale": rationale,
    }


def _build_endpoints() -> tuple[LLMEndpoint, LLMEndpoint | None]:
    frontier_url = os.getenv("BASE_URL", "").strip() or None
    frontier_model = (os.getenv("MODEL_NAME", "").strip() or "")
    if not frontier_model:
        frontier_model = resolve_enrich_model(frontier_url, "")

    google_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if google_key:
        frontier_key = google_key
    else:
        frontier_key = resolve_llm_api_key(frontier_url)

    frontier = LLMEndpoint(
        name="frontier",
        base_url=frontier_url,
        api_key=frontier_key,
        model_name=frontier_model,
    )

    local_url = os.getenv("LOCAL_URL", "").strip() or None
    local_model = os.getenv("LOCAL_MODEL_NAME", "").strip()
    local_key = os.getenv("OPENAI_API_KEY", "").strip()
    local_endpoint: LLMEndpoint | None = None
    if local_url and local_model and local_key:
        local_endpoint = LLMEndpoint(
            name="local",
            base_url=local_url,
            api_key=local_key,
            model_name=local_model,
        )

    return frontier, local_endpoint


def _create_completion(client: Any, endpoint: LLMEndpoint, prompt: str, payload: dict[str, Any], safe_mode: bool) -> Any:
    request_kwargs: dict[str, Any] = {
        "model": endpoint.model_name,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(payload)},
        ],
        "temperature": 0,
    }

    # Safe mode is intended for local inference stability on resource-constrained laptops.
    if safe_mode and endpoint.name == "local":
        request_kwargs["max_tokens"] = 120
        request_kwargs["extra_body"] = {
            "options": {
                "num_predict": 120,
                "num_thread": 2,
                "num_ctx": 2048,
            }
        }

    return client.chat.completions.create(**request_kwargs)


def enrich_row_with_ai(client: Any, endpoint: LLMEndpoint, row: pd.Series, safe_mode: bool) -> dict[str, str]:
    prompt = _build_system_prompt()

    payload = {
        "company_name": safe_text(row.get("company_name")),
        "ticker": safe_text(row.get("ticker")),
        "sector": safe_text(row.get("sector")),
        "industry": safe_text(row.get("industry")),
        "business_summary": safe_text(row.get("business_summary")),
        "market_cap": to_plain_number(row.get("market_cap")),
        "total_debt": to_plain_number(row.get("total_debt")),
        "intangible_assets": to_plain_number(row.get("intangible_assets")),
        "cash_and_equivalents": to_plain_number(row.get("cash_and_equivalents")),
        "current_liabilities": to_plain_number(row.get("current_liabilities")),
        "operating_cash_flow": to_plain_number(row.get("operating_cash_flow")),
        "ebitda": to_plain_number(row.get("ebitda")),
        "interest_expense": to_plain_number(row.get("interest_expense")),
        "net_income": to_plain_number(row.get("net_income")),
        "total_assets": to_plain_number(row.get("total_assets")),
    }

    completion = _create_completion(client, endpoint, prompt, payload, safe_mode=safe_mode)

    content = completion.choices[0].message.content or "{}"
    parsed = json.loads(content)
    return _normalize_ai_output(parsed)


def run_enrich_pipeline(
    input_csv: Path,
    output_csv: Path,
    sleep_seconds: float,
    limit: int | None,
    safe_mode: bool,
) -> None:
    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")
    if OpenAI is None:
        raise RuntimeError("openai package is required for enrich mode. Install with pip install openai")

    frontier_endpoint, local_endpoint = _build_endpoints()
    client = OpenAI(api_key=frontier_endpoint.api_key, base_url=frontier_endpoint.base_url)
    active_endpoint = frontier_endpoint
    LOGGER.info(
        "Using frontier endpoint base_url=%s model=%s",
        frontier_endpoint.base_url or "(default)",
        frontier_endpoint.model_name,
    )
    if local_endpoint is not None:
        LOGGER.info(
            "Local fallback configured base_url=%s model=%s safe_mode=%s",
            local_endpoint.base_url,
            local_endpoint.model_name,
            safe_mode,
        )
    else:
        LOGGER.info("Local fallback not configured (requires LOCAL_URL, LOCAL_MODEL_NAME, OPENAI_API_KEY).")

    df = pd.read_csv(input_csv)
    if limit is not None:
        df = df.head(limit).copy()

    for col in ENRICH_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    LOGGER.info("Starting enrichment run for %d rows", len(df))
    for i, row in df.iterrows():
        try:
            enriched = enrich_row_with_ai(client, active_endpoint, row, safe_mode=safe_mode)
            df.at[i, "ibc_liquidation_risk"] = enriched["ibc_liquidation_risk"]
            df.at[i, "asset_illusion_rationale"] = enriched["asset_illusion_rationale"]
            df.at[i, "enrichment_status"] = "ai_enriched"
            df.at[i, "enrichment_error"] = ""
        except Exception as err:
            switched = False
            if active_endpoint.name == "frontier" and local_endpoint is not None:
                LOGGER.warning(
                    "Frontier call failed for row=%s ticker=%s. Switching to local fallback. error=%s",
                    i,
                    row.get("ticker"),
                    err,
                )
                client = OpenAI(api_key=local_endpoint.api_key, base_url=local_endpoint.base_url)
                active_endpoint = local_endpoint
                switched = True

            if switched:
                try:
                    enriched = enrich_row_with_ai(client, active_endpoint, row, safe_mode=safe_mode)
                    df.at[i, "ibc_liquidation_risk"] = enriched["ibc_liquidation_risk"]
                    df.at[i, "asset_illusion_rationale"] = enriched["asset_illusion_rationale"]
                    df.at[i, "enrichment_status"] = "ai_enriched_local_fallback"
                    df.at[i, "enrichment_error"] = str(err)[:1000]
                except Exception as local_err:
                    df.at[i, "ibc_liquidation_risk"] = "Medium"
                    df.at[i, "asset_illusion_rationale"] = "Insufficient context to generate a specific liquidation rationale."
                    df.at[i, "enrichment_status"] = "fallback_default"
                    df.at[i, "enrichment_error"] = f"frontier:{err} | local:{local_err}"[:1000]
                    LOGGER.warning("Local fallback failed for row=%s ticker=%s: %s", i, row.get("ticker"), local_err)
            else:
                df.at[i, "ibc_liquidation_risk"] = "Medium"
                df.at[i, "asset_illusion_rationale"] = "Insufficient context to generate a specific liquidation rationale."
                df.at[i, "enrichment_status"] = "fallback_default"
                df.at[i, "enrichment_error"] = str(err)[:1000]
                LOGGER.warning("Enrichment failed for row=%s ticker=%s: %s", i, row.get("ticker"), err)

        LOGGER.info("ENRICH row=%d ticker=%s status=%s", i, row.get("ticker"), df.at[i, "enrichment_status"])
        time.sleep(sleep_seconds)

    df.to_csv(output_csv, index=False)
    LOGGER.info("Enrichment completed. Wrote %d rows to %s", len(df), output_csv)
