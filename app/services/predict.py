from pathlib import Path
from typing import Dict, List, Optional

import joblib
import numpy as np
from fastapi import HTTPException

from schemas import ModelResult


def load_bundle(path: Path):
    obj = joblib.load(path)
    if isinstance(obj, dict):
        model = obj.get("model", obj.get("estimator", obj))
        threshold = float(obj.get("threshold", obj.get("best_threshold", 0.5)))
        features = obj.get("features")
        name = obj.get("model_name", obj.get("name", path.stem))
        return model, threshold, features, name

    model = obj
    threshold = 0.5
    features = getattr(model, "feature_names_in_", None)
    name = path.stem
    return model, threshold, features, name


def get_required_features(features: Optional[List[str]], sample: Dict[str, float]):
    if features:
        return list(features)
    return sorted(sample.keys())


def add_ratio_features(feature_map: Dict[str, float]):
    if "debt_to_assets" not in feature_map:
        if "total_debt" in feature_map and "total_assets" in feature_map:
            total_assets = feature_map["total_assets"]
            feature_map["debt_to_assets"] = (
                feature_map["total_debt"] / total_assets if total_assets else np.nan
            )

    if "cash_to_current_liabilities" not in feature_map:
        if "cash_and_equivalents" in feature_map and "current_liabilities" in feature_map:
            current_liabilities = feature_map["current_liabilities"]
            feature_map["cash_to_current_liabilities"] = (
                feature_map["cash_and_equivalents"] / current_liabilities
                if current_liabilities
                else np.nan
            )

    if "interest_coverage_proxy" not in feature_map:
        if "ebitda" in feature_map and "interest_expense" in feature_map:
            interest_expense = feature_map["interest_expense"]
            feature_map["interest_coverage_proxy"] = (
                feature_map["ebitda"] / interest_expense if interest_expense else np.nan
            )

    if "tangible_asset_coverage" not in feature_map:
        if (
            "total_assets" in feature_map
            and "intangible_assets" in feature_map
            and "total_debt" in feature_map
        ):
            total_debt = feature_map["total_debt"]
            feature_map["tangible_asset_coverage"] = (
                (feature_map["total_assets"] - feature_map["intangible_assets"]) / total_debt
                if total_debt
                else np.nan
            )

    return feature_map


def load_models(root: Path):
    model_paths = {
        "optimal": root / "artifacts" / "optimal.joblib",
        "tuned": root / "artifacts" / "tuned.joblib",
    }
    return {
        key: load_bundle(path)
        for key, path in model_paths.items()
        if path.exists()
    }


def build_predictions(feature_map: Dict[str, float], models) -> Dict[str, ModelResult]:
    if not models:
        raise HTTPException(status_code=500, detail="No model bundles found.")

    results: Dict[str, ModelResult] = {}
    for key, (model, threshold, model_features, model_name) in models.items():
        required = get_required_features(model_features, feature_map)
        missing = [name for name in required if name not in feature_map]
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Missing features for {key}: {missing}",
            )

        row = [feature_map[name] for name in required]
        X = np.array([row], dtype=float)

        if hasattr(model, "predict_proba"):
            proba = float(model.predict_proba(X)[:, 1][0])
            pred = int(proba >= threshold)
        else:
            proba = None
            pred = int(model.predict(X)[0])

        results[key] = ModelResult(
            prediction=pred,
            probability=proba,
            threshold=float(threshold) if threshold is not None else None,
            model_name=str(model_name),
            features_used=required,
        )

    return results
