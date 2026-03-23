from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.detection.fraud_detector import detect_fraud, get_detector


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")


SCHEMA = StructType(
    [
        StructField("event_time", StringType()),
        StructField("step", IntegerType()),
        StructField("type", StringType()),
        StructField("amount", DoubleType()),
        StructField("nameOrig", StringType()),
        StructField("oldbalanceOrg", DoubleType()),
        StructField("newbalanceOrig", DoubleType()),
        StructField("nameDest", StringType()),
        StructField("oldbalanceDest", DoubleType()),
        StructField("newbalanceDest", DoubleType()),
        StructField("login_country", StringType()),
        StructField("user_home_country", StringType()),
        StructField("device_change", IntegerType()),
        StructField("_corrupt_record", StringType()),
    ]
)


def create_spark_session(app_name: str = "FraudDetectionStreaming") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )


def run_streaming_job(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "transactions",
    checkpoint_dir: str = "data/output/checkpoints/fraud_stream",
    output_file: str = "data/output/fraud_predictions.jsonl",
    trigger_interval: str = "5 seconds",
) -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Warm-up detector once at startup to avoid first-batch latency spike.
    _ = get_detector()

    source_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "2000")
        .load()
    )

    parsed_df = (
        source_df.select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("raw_value"),
        )
        .select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "raw_value",
            from_json(
                col("raw_value"),
                SCHEMA,
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
            ).alias("data"),
        )
        .select("topic", "partition", "offset", "kafka_timestamp", "raw_value", "data.*")
    )

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    def _format_tx_line(row: pd.Series, fraud: bool) -> str:
        prefix = "🚨 FRAUD DETECTED" if fraud else "✅ NORMAL TX"
        reasons = row.get("fraud_reasons", "none")
        return (
            f"{prefix} | amount={row.get('amount')} | sender={row.get('nameOrig')} "
            f"| receiver={row.get('nameDest')} | type={row.get('type')} "
            f"| ml={row.get('ml_prediction')} | rule={row.get('rule_flag')} "
            f"| final={row.get('final_flag')} | reason={reasons}"
        )

    def process_batch(batch_df, batch_id):
        try:
            if batch_df.isEmpty():
                LOGGER.info("Batch %s is empty", batch_id)
                return

            pdf = batch_df.toPandas()
            if pdf.empty:
                LOGGER.info("Batch %s has no rows after conversion", batch_id)
                return

            # Safe handling for invalid JSON records.
            invalid_mask = pdf["_corrupt_record"].notna() if "_corrupt_record" in pdf.columns else pd.Series([False] * len(pdf))
            invalid_count = int(invalid_mask.sum())
            if invalid_count > 0:
                LOGGER.warning("Batch %s contains %s invalid JSON records. They will be skipped.", batch_id, invalid_count)

            valid_pdf = pdf.loc[~invalid_mask].copy()
            if valid_pdf.empty:
                LOGGER.info("Batch %s has no valid records after JSON validation", batch_id)
                return

            detected = detect_fraud(valid_pdf)

            keep_cols = [
                "event_time",
                "nameOrig",
                "nameDest",
                "type",
                "amount",
                "ml_prediction",
                "rule_flag",
                "final_flag",
                "fraud_reasons",
            ]
            for col_name in keep_cols:
                if col_name not in detected.columns:
                    detected[col_name] = None

            fraud_df = detected[detected["final_flag"] == 1][keep_cols]
            normal_df = detected[detected["final_flag"] == 0][keep_cols]

            print("\n" + "=" * 100)
            print(f"BATCH {batch_id} SUMMARY | total={len(detected)} | fraud={len(fraud_df)} | normal={len(normal_df)}")
            print("=" * 100)

            if not fraud_df.empty:
                for _, row in fraud_df.head(30).iterrows():
                    print(_format_tx_line(row, fraud=True))

            if not normal_df.empty:
                for _, row in normal_df.head(30).iterrows():
                    print(_format_tx_line(row, fraud=False))

            LOGGER.info(
                "batch_id=%s total=%s fraud=%s normal=%s invalid_json=%s sink=%s",
                batch_id,
                len(detected),
                len(fraud_df),
                len(normal_df),
                invalid_count,
                output_path,
            )

            # Bonus sink: append all scored records to JSONL.
            with output_path.open("a", encoding="utf-8") as f:
                detected.to_json(f, orient="records", lines=True)

        except Exception as exc:
            # Keep stream alive on batch failures.
            LOGGER.exception("Error while processing batch %s. Batch skipped: %s", batch_id, exc)
            return

    query = (
        parsed_df.writeStream.foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=trigger_interval)
        .start()
    )

    print("Spark streaming job is running. Press Ctrl+C to stop.")
    query.awaitTermination()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Spark Structured Streaming fraud detection consumer.")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="transactions", help="Kafka topic")
    parser.add_argument("--checkpoint", default="data/output/checkpoints/fraud_stream", help="Checkpoint directory")
    parser.add_argument("--output", default="data/output/fraud_predictions.jsonl", help="Output JSONL file")
    parser.add_argument("--trigger", default="5 seconds", help="Micro-batch trigger interval")
    args = parser.parse_args()

    run_streaming_job(
        bootstrap_servers=args.bootstrap,
        topic=args.topic,
        checkpoint_dir=args.checkpoint,
        output_file=args.output,
        trigger_interval=args.trigger,
    )


if __name__ == "__main__":
    main()