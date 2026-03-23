from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

from src.model.train_model import train_and_save_model
from src.preprocessing.generate_synthetic_data import generate_synthetic_transactions
from src.preprocessing.preprocess_data import preprocess, save_processed
from src.producer.kafka_producer import stream_transactions


def prepare_data(raw_path: str, processed_path: str, rows: int, seed: int) -> None:
	df = generate_synthetic_transactions(rows=rows, seed=seed)
	save_processed(df, raw_path)
	processed = preprocess(df)
	save_processed(processed, processed_path)
	print(f"Raw synthetic data saved to {raw_path}")
	print(f"Processed data saved to {processed_path}")


def run_streaming_subprocess() -> subprocess.Popen:
	cmd = [
		sys.executable,
		"-m",
		"src.streaming.spark_streaming_job",
	]
	return subprocess.Popen(cmd)


def run_demo(limit: int, delay: float) -> None:
	print("Starting Spark streaming consumer...")
	streaming_proc = run_streaming_subprocess()
	try:
		time.sleep(10)
		print("Starting Kafka producer...")
		stream_transactions(limit=limit, delay_seconds=delay)
		print("Producer finished. Keep streaming running for another 10 seconds.")
		time.sleep(10)
	finally:
		print("Stopping Spark streaming consumer...")
		streaming_proc.terminate()


def main() -> None:
	parser = argparse.ArgumentParser(description="Orchestrator for local real-time fraud detection demo.")
	subparsers = parser.add_subparsers(dest="command", required=True)

	p_prepare = subparsers.add_parser("prepare-data", help="Generate synthetic data and preprocess it")
	p_prepare.add_argument("--rows", type=int, default=2000)
	p_prepare.add_argument("--seed", type=int, default=42)
	p_prepare.add_argument("--raw", default="data/raw/transactions_synthetic.csv")
	p_prepare.add_argument("--processed", default="data/processed/transactions_processed.csv")

	p_train = subparsers.add_parser("train-model", help="Train and save fraud model")
	p_train.add_argument("--input", default="data/processed/transactions_processed.csv")
	p_train.add_argument("--output", default="src/model/saved_model/fraud_model.pkl")

	p_producer = subparsers.add_parser("run-producer", help="Run Kafka producer")
	p_producer.add_argument("--data", default="data/raw/transactions_synthetic.csv")
	p_producer.add_argument("--limit", type=int, default=300)
	p_producer.add_argument("--delay", type=float, default=0.2)

	subparsers.add_parser("run-streaming", help="Run Spark streaming consumer")

	p_demo = subparsers.add_parser("run-demo", help="Run producer and streaming together")
	p_demo.add_argument("--limit", type=int, default=300)
	p_demo.add_argument("--delay", type=float, default=0.2)

	args = parser.parse_args()

	if args.command == "prepare-data":
		prepare_data(raw_path=args.raw, processed_path=args.processed, rows=args.rows, seed=args.seed)
	elif args.command == "train-model":
		train_and_save_model(input_path=args.input, output_path=Path(args.output))
	elif args.command == "run-producer":
		stream_transactions(data_path=args.data, limit=args.limit, delay_seconds=args.delay)
	elif args.command == "run-streaming":
		subprocess.run([sys.executable, "-m", "src.streaming.spark_streaming_job"], check=True)
	elif args.command == "run-demo":
		run_demo(limit=args.limit, delay=args.delay)


if __name__ == "__main__":
	main()
