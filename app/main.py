from pathlib import Path
import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

from schemas import (
    AnalyzeRequest,
    AnalyzeResponse,
    AnalystModelInput,
    PredictRequest,
    PredictResponse,
    PredictWithAnalysisRequest,
    PredictWithAnalysisResponse,
)
from services.analyst import generate_analyst_insight, parse_analyst_response
from services.predict import add_ratio_features, build_predictions, load_models


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
LOGGER = logging.getLogger("app")
if os.getenv("DEBUG", "").lower() == "true":
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

MODELS = load_models(ROOT)

app = FastAPI(title="Insolvency Predictor", version="0.1.0")


@app.post("/predict", response_model=PredictResponse)
async def predict(payload: PredictRequest):
    feature_map = add_ratio_features(dict(payload.features))
    results = build_predictions(feature_map, MODELS)
    return PredictResponse(predictions=results)


@app.post("/analyze", response_model=AnalyzeResponse)
async def analyze(payload: AnalyzeRequest):
    if "optimal" not in payload.predictions or "tuned" not in payload.predictions:
        raise HTTPException(
            status_code=400,
            detail="predictions must include both 'optimal' and 'tuned'.",
        )

    raw = generate_analyst_insight(payload.company_name, payload.ticker, payload.predictions)
    LOGGER.debug("Raw analyst JSON: %s", raw[:1000])
    try:
        return parse_analyst_response(raw)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Invalid analyst JSON: {exc}") from exc


@app.post("/predict-with-analysis", response_model=PredictWithAnalysisResponse)
async def predict_with_analysis(payload: PredictWithAnalysisRequest):
    feature_map = add_ratio_features(dict(payload.features))
    predictions = build_predictions(feature_map, MODELS)

    analyst_payload = {
        key: AnalystModelInput(
            prediction=value.prediction,
            probability=value.probability,
            threshold=value.threshold,
        )
        for key, value in predictions.items()
    }

    raw = generate_analyst_insight(payload.company_name, payload.ticker, analyst_payload)
    LOGGER.debug("Raw analyst JSON: %s", raw[:1000])
    try:
        insights = parse_analyst_response(raw).insights
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Invalid analyst JSON: {exc}") from exc

    return PredictWithAnalysisResponse(predictions=predictions, insights=insights)

from nicegui import ui
from ui import build_ui

build_ui(predict_with_analysis)
ui.run_with(app, mount_path="/", title="Insolvency Predictor AI")
