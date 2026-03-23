from __future__ import annotations

import argparse
import json
import time

import pandas as pd
from kafka import KafkaProducer


DEFAULT_DATA_PATH = "data/raw/transactions_synthetic.csv"
TOPIC = "transactions"
BOOTSTRAP_SERVERS = "localhost:9092"


def build_producer(bootstrap_servers: str = BOOTSTRAP_SERVERS) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def load_transactions(path: str, limit: int | None = None) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "event_time" in df.columns:
        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
        df = df.sort_values("event_time")
    if limit is not None:
        df = df.head(limit)
    return df


def stream_transactions(
    data_path: str = DEFAULT_DATA_PATH,
    topic: str = TOPIC,
    bootstrap_servers: str = BOOTSTRAP_SERVERS,
    delay_seconds: float = 0.2,
    limit: int | None = 300,
) -> None:
    producer = build_producer(bootstrap_servers=bootstrap_servers)
    df = load_transactions(data_path, limit=limit)

    required = [
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
    ]

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        msg = {
            "event_time": str(pd.to_datetime(row["event_time"])),
            "step": int(row["step"]),
            "type": str(row["type"]),
            "amount": float(row["amount"]),
            "nameOrig": str(row["nameOrig"]),
            "oldbalanceOrg": float(row["oldbalanceOrg"]),
            "newbalanceOrig": float(row["newbalanceOrig"]),
            "nameDest": str(row["nameDest"]),
            "oldbalanceDest": float(row["oldbalanceDest"]),
            "newbalanceDest": float(row["newbalanceDest"]),
            "login_country": str(row["login_country"]),
            "user_home_country": str(row["user_home_country"]),
            "device_change": int(row["device_change"]),
        }

        if any(col not in row for col in required):
            raise ValueError("Input CSV is missing required columns for producer payload")

        producer.send(topic, value=msg)
        print(f"[{i}] Sent transaction from {msg['nameOrig']} to {msg['nameDest']} amount={msg['amount']}")
        time.sleep(delay_seconds)

    producer.flush()
    print("Producer finished streaming transactions to Kafka.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream transaction records from CSV to Kafka.")
    parser.add_argument("--data", default=DEFAULT_DATA_PATH, help="Input CSV path")
    parser.add_argument("--topic", default=TOPIC, help="Kafka topic name")
    parser.add_argument("--bootstrap", default=BOOTSTRAP_SERVERS, help="Kafka bootstrap server")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay between messages in seconds")
    parser.add_argument("--limit", type=int, default=300, help="Number of rows to stream")
    args = parser.parse_args()

    stream_transactions(
        data_path=args.data,
        topic=args.topic,
        bootstrap_servers=args.bootstrap,
        delay_seconds=args.delay,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()