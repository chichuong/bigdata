# Real-time Fraud Detection Capstone (Kafka + Spark + ML + Rules)

This project is a demo-ready capstone product for final-year defense. It simulates an e-wallet fraud monitoring platform with real-time data ingestion, stream processing, model inference, rule-based checks, and a live dashboard.

## 1. Problem Statement

E-wallet systems process large transaction volumes continuously. Fraud patterns are mixed:
- Statistical anomalies that ML can learn
- Business constraints that rules can enforce immediately

A strong detection platform should combine both in real time:
- ML catches hidden patterns
- Rules enforce explicit risk policies

## 2. Why ML + Rule Engine

Using only ML can miss strict policy violations. Using only rules can miss subtle anomalies.

Fusion strategy used in this project:
- ml_prediction: output from trained fraud model
- rule_flag: output from rules (blacklist, large amount, unusual login, rapid transfers)
- final_flag: 1 if ml_prediction == 1 OR rule_flag == 1

This design is practical for real systems where explainability and recall are both important.

## 3. Architecture (Text Diagram)

```text
[Synthetic CSV Transactions]
          |
          v
[Kafka Producer] ---> [Kafka Topic: transactions]
                              |
                              v
                   [Spark Structured Streaming]
                              |
                              v
                [Fraud Detector (ML + Rules)]
                   |                    |
                   |                    +--> Rule signals:
                   |                         - blacklist account
                   |                         - large amount
                   |                         - rapid repeated transfers
                   |                         - unusual login
                   |
                   +--> ML signal: model prediction
                              |
                              v
                 [Final Decision + Explanations]
                              |
                              +--> Console Logs (fraud + normal)
                              |
                              +--> JSONL Sink (data/output/fraud_predictions.jsonl)
                                              |
                                              v
                                  [Streamlit Dashboard]
```

## 4. Tech Stack

- Python 3.10+
- Apache Kafka (local via Docker)
- Spark Structured Streaming (PySpark)
- Scikit-learn (Random Forest model)
- Pandas / NumPy
- Streamlit dashboard

## 5. Project Structure

```text
main.py
requirements.txt
README.md
PRESENTATION.md
DEMO_SCRIPT.md

data/
  blacklist/blacklist_accounts.csv
  raw/transactions_synthetic.csv
  processed/transactions_processed.csv
  output/fraud_predictions.jsonl

docker/
  docker-compose.yml

src/
  preprocessing/
    generate_synthetic_data.py
    preprocess_data.py
  features/feature_engineering.py
  model/
    train_model.py
    predict_model.py
    saved_model/fraud_model.pkl
  rules/rule_engine.py
  detection/fraud_detector.py
  producer/kafka_producer.py
  streaming/spark_streaming_job.py
  dashboard/app.py
```

## 6. Setup

### 6.1 Prerequisites

- Python installed
- Java installed (required by Spark)
- Docker Desktop running (required by local Kafka)

### 6.2 Install dependencies

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### 6.3 Start Kafka

```bash
docker compose -f docker/docker-compose.yml up -d
```

Check container:

```bash
docker ps
```

## 7. Data and Model Preparation

```bash
python main.py prepare-data --rows 2500 --seed 42
python main.py train-model
```

Expected output:
- Raw data generated in data/raw/transactions_synthetic.csv
- Processed data generated in data/processed/transactions_processed.csv
- Model saved to src/model/saved_model/fraud_model.pkl

## 8. Demo Flow (3 Terminals)

### Terminal 1: Run streaming consumer

```bash
python main.py run-streaming
```

Expected output:
- Spark starts successfully
- Batch summaries printed periodically
- Fraud lines shown as:
  - "FRAUD DETECTED | amount=... | sender=... | receiver=... | reason=..."
- Normal lines shown as:
  - "NORMAL TX | amount=... | sender=... | receiver=..."

### Terminal 2: Run producer

```bash
python main.py run-producer --limit 300 --delay 0.2
```

Expected output:
- Producer sends transactions continuously to Kafka
- Streaming terminal starts printing batch updates almost immediately

### Terminal 3: Run dashboard

```bash
streamlit run src/dashboard/app.py
```

Expected output in browser:
- Total transactions
- Fraud count
- Fraud rate (%)
- Fraud vs normal chart
- Recent fraud transactions table (with explanation reasons)

## 9. Real-time Detection Internals

For each micro-batch in Spark:
1. Read raw messages from Kafka topic transactions
2. Parse JSON with explicit schema in permissive mode
3. Skip invalid JSON rows safely
4. Convert valid rows to pandas micro-batch
5. Run detector:
   - sanitize missing fields
   - ML prediction
   - rule_engine checks
   - combine to final_flag
   - attach explanation labels
6. Print structured logs
7. Append full scored records to JSONL sink

## 10. Log and Explanation Format

Each flagged transaction includes explanation details:
- ml_prediction
- rule_flag
- fraud_reasons (example: blacklist_account|large_amount|ml_high_risk)

This makes the system easy to explain in front of evaluators.

## 11. Quick One-command Demo

```bash
python main.py run-demo --limit 250 --delay 0.2
```

This command starts streaming, runs producer, waits for processing, then stops consumer.

## 12. Troubleshooting

- If producer shows NoBrokersAvailable:
  - Ensure Docker Desktop is running
  - Ensure Kafka container is up
- If Spark fails to start:
  - Verify Java installation
- If dashboard has no data:
  - Confirm streaming is running and output file is growing

## 13. Stop Services

```bash
docker compose -f docker/docker-compose.yml down
```

## 14. Suggested Evaluation Highlights

- Real-time stream architecture, not batch-only analysis
- Hybrid detection strategy (ML + Rule) with explainability
- Robustness features: invalid JSON handling, batch-level exception handling
- End-to-end visual monitoring via Streamlit
