"""LLM extraction for PDF metrics."""

from __future__ import annotations

import logging
import os
import time

try:
    from google import genai
    from google.genai import types
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("google-genai is required. Install with pip install google-genai") from exc

from .config import get_required_env, load_environment


LOGGER = logging.getLogger("dataset_builder")
DEFAULT_MODEL_NAME = "gemini-2.5-flash"

DEFAULT_SYSTEM_PROMPT = (
    "Role: You are a strict forensic accounting parser analyzing Indian corporate "
    "financial statements (Ind AS).\n"
    "Task: Extract specific quantitative metrics from the provided text/images into a "
    "strict JSON schema.\n\n"
    "Absolute Rules:\n"
    "1. Zero Hallucination: If a value is missing, obscured, or ambiguous, output null. "
    "Do not calculate, estimate, or infer missing numbers.\n"
    "2. Format: Strip all commas, spaces, and currency symbols. Return only raw numbers.\n"
    "3. Scale Awareness: Indian financials often report in 'Lakhs' (100000) or "
    "'Crores' (10000000). Look for the unit declaration at the top of the table and "
    "mathematically convert the extracted numbers into absolute INR values before outputting.\n"
    "4. Consolidated Priority: If both Standalone and Consolidated columns/figures are visible, "
    "strictly extract the Consolidated figures.\n"
    "5. Output Restriction: Return ONLY valid, parseable JSON. Do not include markdown formatting, "
    "explanations, warnings, or preamble text.\n\n"
    "Required JSON Schema:\n"
    "{\n"
    "  \"total_debt\": <float or null>,\n"
    "  \"intangible_assets\": <float or null>,\n"
    "  \"cash_and_equivalents\": <float or null>,\n"
    "  \"current_liabilities\": <float or null>,\n"
    "  \"operating_cash_flow\": <float or null>,\n"
    "  \"interest_expense\": <float or null>,\n"
    "  \"net_income\": <float or null>,\n"
    "  \"total_assets\": <float or null>,\n"
    "  \"profit_before_tax\": <float or null>,\n"
    "  \"depreciation_and_amortization\": <float or null>\n"
    "}"
)


def extract_metrics_with_gemini(
    pdf_path: str,
    system_prompt: str | None = None,
    model_name: str | None = None,
) -> str:
    load_environment()
    api_key = get_required_env("GOOGLE_API_KEY")
    client = genai.Client(api_key=api_key)

    resolved_model = (model_name or os.getenv("MODEL_NAME", DEFAULT_MODEL_NAME)).strip() or DEFAULT_MODEL_NAME
    prompt = system_prompt or DEFAULT_SYSTEM_PROMPT

    try:
        sample_file = client.files.upload(file=pdf_path)
    except TypeError:
        sample_file = client.files.upload(path=pdf_path, display_name=os.path.basename(pdf_path))
    LOGGER.info("Uploaded file: %s", sample_file.name)

    while sample_file.state.name == "PROCESSING":
        LOGGER.info("Waiting for file to be processed...")
        time.sleep(2)
        sample_file = client.files.get(sample_file.name)

    if sample_file.state.name == "FAILED":
        raise RuntimeError(sample_file.state.name)

    response = client.models.generate_content(
        model=resolved_model,
        contents=["Extract the required JSON schema from this document.", sample_file],
        config=types.GenerateContentConfig(
            system_instruction=prompt,
            response_mime_type="application/json",
        ),
    )
    text = (response.text or "") if response else ""
    LOGGER.info("Gemini response received chars=%d", len(text))
    if not text.strip():
        LOGGER.warning("Gemini returned empty response")
    return text
