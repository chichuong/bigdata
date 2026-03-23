from __future__ import annotations

import argparse
from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split

from src.features.feature_engineering import create_features


DEFAULT_DATA_PATH = "data/processed/transactions_processed.csv"
MODEL_DIR = Path("src/model/saved_model")
MODEL_PATH = MODEL_DIR / "fraud_model.pkl"


def build_training_matrix(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, list[str]]:
    data = create_features(df)

    feature_df = data[
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

    feature_df = pd.get_dummies(feature_df, columns=["type"])
    y = data["isFraud"].astype(int)
    feature_columns = sorted(feature_df.columns.tolist())
    feature_df = feature_df.reindex(columns=feature_columns, fill_value=0)
    return feature_df, y, feature_columns


def train_and_save_model(input_path: str = DEFAULT_DATA_PATH, output_path: Path = MODEL_PATH) -> None:
    df = pd.read_csv(input_path)
    X, y, feature_columns = build_training_matrix(df)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    model = RandomForestClassifier(n_estimators=120, max_depth=12, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    print("Precision:", round(precision_score(y_test, y_pred, zero_division=0), 4))
    print("Recall:", round(recall_score(y_test, y_pred, zero_division=0), 4))
    print("F1-score:", round(f1_score(y_test, y_pred, zero_division=0), 4))
    print("\nClassification Report:\n")
    print(classification_report(y_test, y_pred, zero_division=0))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "feature_columns": feature_columns}, output_path)
    print(f"Model saved at: {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Train fraud detection model and save model bundle.")
    parser.add_argument("--input", default=DEFAULT_DATA_PATH, help="Processed CSV path")
    parser.add_argument("--output", default=str(MODEL_PATH), help="Model output path")
    args = parser.parse_args()

    train_and_save_model(input_path=args.input, output_path=Path(args.output))


if __name__ == "__main__":
    main()