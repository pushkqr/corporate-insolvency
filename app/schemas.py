from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class PredictRequest(BaseModel):
    features: Dict[str, float] = Field(
        ...,
        description="Feature name to value mapping. Must include all model features.",
    )


class ModelResult(BaseModel):
    prediction: int
    probability: Optional[float]
    threshold: Optional[float]
    model_name: str
    features_used: List[str]


class PredictResponse(BaseModel):
    predictions: Dict[str, ModelResult]


class AnalystModelInput(BaseModel):
    prediction: int
    probability: Optional[float] = None
    threshold: Optional[float] = None


class AnalyzeRequest(BaseModel):
    ticker: str
    company_name: str
    predictions: Dict[str, AnalystModelInput]


class AnalystEvidence(BaseModel):
    title: str
    url: str
    snippet: Optional[str] = None


class AnalystInsight(BaseModel):
    verdict: str
    summary: str
    rationale: str
    sources: List[AnalystEvidence]


class AnalyzeResponse(BaseModel):
    insights: Dict[str, AnalystInsight]


class PredictWithAnalysisRequest(BaseModel):
    ticker: str
    company_name: str
    features: Dict[str, float] = Field(
        ...,
        description="Feature name to value mapping. Can include base fields for ratios.",
    )


class PredictWithAnalysisResponse(BaseModel):
    predictions: Dict[str, ModelResult]
    insights: Dict[str, AnalystInsight]
