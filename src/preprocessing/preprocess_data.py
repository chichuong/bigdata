from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = [
    "event_time",
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "login_country",
    "user_home_country",
    "device_change",
    "isFraud",
]


def load_data(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()

    missing = [c for c in REQUIRED_COLUMNS if c not in work.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    work = work[REQUIRED_COLUMNS]
    work["event_time"] = pd.to_datetime(work["event_time"], errors="coerce")
    work = work.dropna(subset=["event_time", "type", "nameOrig", "nameDest"])

    numeric_cols = [
        "step",
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
        "device_change",
        "isFraud",
    ]
    for col in numeric_cols:
        work[col] = pd.to_numeric(work[col], errors="coerce")

    work = work.dropna(subset=numeric_cols)
    work["device_change"] = work["device_change"].astype(int)
    work["isFraud"] = work["isFraud"].astype(int)
    work["type"] = work["type"].astype(str)

    # Remove impossible records where transaction amount is not positive.
    work = work[work["amount"] > 0].sort_values("event_time").reset_index(drop=True)
    return work


def save_processed(df: pd.DataFrame, path: str) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Preprocess transaction data for model training.")
    parser.add_argument("--input", default="data/raw/transactions_synthetic.csv", help="Input CSV path")
    parser.add_argument("--output", default="data/processed/transactions_processed.csv", help="Output CSV path")
    args = parser.parse_args()

    df = load_data(args.input)
    df = preprocess(df)
    save_processed(df, args.output)

    print(f"Preprocessing complete: {args.output}")
    print(df.head(3).to_string(index=False))


if __name__ == "__main__":
    main()