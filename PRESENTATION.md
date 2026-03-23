# Presentation Content (6 Slides)

## Slide 1 - Problem
- E-wallet platforms process continuous high-volume transactions
- Fraud patterns evolve quickly and can occur within seconds
- Traditional offline checks are too slow for prevention
- Need real-time, explainable, and scalable fraud detection

## Slide 2 - Proposed Solution
- Build a real-time fraud pipeline using Kafka + Spark + ML + Rules
- Stream transactions from producer to Kafka topic
- Process each micro-batch in Spark Structured Streaming
- Combine model-based and rule-based decisions into one final flag

## Slide 3 - System Architecture
- Data source: synthetic e-wallet transactions
- Ingestion: Kafka producer
- Processing: Spark Structured Streaming
- Decision layer: FraudDetector
  - ML prediction (trained model)
  - Rule engine checks (blacklist, large amount, rapid transfers, unusual login)
- Output: console alerts + JSONL sink + Streamlit dashboard

## Slide 4 - Detection Logic and Explainability
- final_flag = 1 when ml_prediction == 1 OR rule_flag == 1
- Rule triggers are attached as reason labels
- Fraud output includes why a transaction is flagged
- Supports both high recall and easy interpretation for operations teams

## Slide 5 - Demo Flow (3 Terminals)
- Terminal 1: start streaming consumer
- Terminal 2: run producer to push live transactions
- Terminal 3: open dashboard to monitor metrics and latest fraud cases
- Evaluators can see instant fraud alerts and explanation fields

## Slide 6 - Results and Value
- End-to-end local deployment, no cluster required for demo
- Real-time processing with micro-batch architecture
- Strong capstone qualities: architecture, ML, streaming, explainability, visualization
- Ready for future extension: PostgreSQL sink, alert APIs, model retraining automation
