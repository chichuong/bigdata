from __future__ import annotations

from pathlib import Path

import joblib
import pandas as pd

from src.features.feature_engineering import create_features


MODEL_PATH = Path("src/model/saved_model/fraud_model.pkl")
_MODEL = None
_FEATURE_COLUMNS: list[str] | None = None


def _load_model_bundle() -> tuple[object, list[str]]:
    global _MODEL, _FEATURE_COLUMNS
    if _MODEL is not None and _FEATURE_COLUMNS is not None:
        return _MODEL, _FEATURE_COLUMNS

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model file not found at {MODEL_PATH}. Run training first: python -m src.model.train_model"
        )

    bundle = joblib.load(MODEL_PATH)
    if isinstance(bundle, dict) and "model" in bundle and "feature_columns" in bundle:
        _MODEL = bundle["model"]
        _FEATURE_COLUMNS = bundle["feature_columns"]
    else:
        # Backward compatibility with legacy plain-model artifact.
        _MODEL = bundle
        _FEATURE_COLUMNS = []
    return _MODEL, _FEATURE_COLUMNS


def prepare_model_input(df: pd.DataFrame) -> pd.DataFrame:
    _, feature_columns = _load_model_bundle()
    result = create_features(df)

    base_features = result[
        [
            "amount",
            "oldbalanceOrg",
            "newbalanceOrig",
            "oldbalanceDest",
            "newbalanceDest",
            "device_change",
            "balanceDiffOrig",
            "balanceDiffDest",
            "isLargeAmount",
            "isUnusualLogin",
            "isRapidTransfer",
            "type",
        ]
    ].copy()

    encoded = pd.get_dummies(base_features, columns=["type"])

    if feature_columns:
        for col in feature_columns:
            if col not in encoded.columns:
                encoded[col] = 0
        encoded = encoded.reindex(columns=feature_columns, fill_value=0)
    return encoded


def predict(df: pd.DataFrame):
    model, _ = _load_model_bundle()
    X = prepare_model_input(df)
    return model.predict(X)