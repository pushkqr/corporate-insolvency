from typing import Dict
import json
import os
import logging

import requests
from fastapi import HTTPException

from schemas import AnalyzeResponse, AnalystModelInput

try:
    from google import genai
    from google.genai import types
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("google-genai is required. Install with pip install google-genai") from exc


LOGGER = logging.getLogger("app")
BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"
ANALYST_SYSTEM_PROMPT = (
    "Role: You are a forensic financial analyst validating algorithmic insolvency predictions.\n"
    "Task: Evaluate whether the provided news snippets support or contradict the machine learning model's predictions.\n"
    "Context:\n"
    "1. The model predicts corporate insolvency (Target 1 = Distressed/Bankrupt, Target 0 = Healthy/Surviving).\n"
    "2. 'Optimal' represents the statistically balanced prediction. 'Tuned' represents a high-recall, highly pessimistic threshold designed to flag any potential distress.\n"
    "Evaluation Logic:\n"
    "A) If the prediction is 0 (Healthy): News indicating routine operations, profitability, expansion, or a general absence of acute financial distress SUPPORTS the model. Do not output 'contradicts_model' unless there is explicit evidence of liquidity crises, defaults, or bankruptcy.\n"
    "B) If the prediction is 1 (Distressed): News indicating credit downgrades, debt defaults, restructuring, going-concern warnings, or severe cash shortages SUPPORTS the model. News of record profits, successful debt repayment, or massive capital raises CONTRADICTS the model.\n"
    "Output Format: Return ONLY JSON with keys 'optimal' and 'tuned'. Each key must have exactly the following structure:\n"
    "{\"verdict\": \"supports_model\"|\"contradicts_model\"|\"insufficient_evidence\",\n"
    "\"summary\": \"<1-2 sentences summarizing the financial sentiment>\",\n"
    "\"rationale\": \"<concise reasoning mapping the news directly to the 0 or 1 prediction>\",\n"
    "\"sources\": [ {\"title\":\"...\", \"url\":\"...\"} ] }\n"
    "Rules: Base your rationale strictly on the provided snippets. Never inject external knowledge. If the snippets lack concrete financial relevance, return 'insufficient_evidence'."
)

def brave_search(query: str, count: int = 5):
    LOGGER.debug("Brave search query: %s", query)
    api_key = os.getenv("BRAVE_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=500, detail="BRAVE_API_KEY is not set.")

    headers = {
        "X-Subscription-Token": api_key,
        "Accept": "application/json",
    }
    params = {"q": query, "count": count}
    res = requests.get(BRAVE_SEARCH_URL, headers=headers, params=params, timeout=15)
    if not res.ok:
        raise HTTPException(status_code=502, detail=f"Brave search failed: {res.status_code}")

    data = res.json()
    results = data.get("web", {}).get("results", [])
    LOGGER.debug("Brave search results=%d", len(results))
    return [
        {
            "title": item.get("title", ""),
            "url": item.get("url", ""),
            "snippet": item.get("description", ""),
        }
        for item in results
    ]


def generate_analyst_insight(
    company_name: str,
    ticker: str,
    predictions: Dict[str, AnalystModelInput],
):
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=500, detail="GOOGLE_API_KEY is not set.")

    query = f"{company_name} {ticker} financial news insolvency debt restructuring"
    sources = brave_search(query)
    LOGGER.debug("Analyst sources count=%d", len(sources))

    client = genai.Client(api_key=api_key)
    payload = {
        "company_name": company_name,
        "ticker": ticker,
        "predictions": {k: v.model_dump() for k, v in predictions.items()},
        "sources": sources,
    }

    model_name = os.getenv("ANALYST_MODEL_NAME", "gemini-2.5-flash").strip()
    response = client.models.generate_content(
        model=model_name,
        contents=[
            "Analyze company risk vs predictions using sources.",
            json.dumps(payload, ensure_ascii=True),
        ],
        config=types.GenerateContentConfig(
            system_instruction=ANALYST_SYSTEM_PROMPT,
            response_mime_type="application/json",
        ),
    )

    text = (response.text or "").strip() if response else ""
    LOGGER.debug("Gemini response chars=%d", len(text))
    if not text:
        raise HTTPException(status_code=502, detail="Gemini returned empty response.")

    return text


def parse_analyst_response(raw: str) -> AnalyzeResponse:
    try:
        return AnalyzeResponse.model_validate_json(raw)
    except Exception:
        pass

    data = json.loads(raw)
    if "insights" not in data and {"optimal", "tuned"}.issubset(data.keys()):
        data = {"insights": data}
    return AnalyzeResponse.model_validate(data)
