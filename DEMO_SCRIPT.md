# Demo Speaking Script (Final-year Defense)

## 1. Opening (30-45s)
- Good morning lecturers, this project is a real-time fraud detection system for e-wallet transactions.
- The objective is to detect suspicious transactions immediately, not after offline analysis.
- The key point is combining machine learning with rule-based controls for both intelligence and explainability.

## 2. Architecture Walkthrough (60-90s)
- On the left, a producer streams transaction events into Kafka.
- Spark Structured Streaming consumes data from the transactions topic in micro-batches.
- Each batch is sent to a FraudDetector module.
- FraudDetector runs two engines in parallel logic:
  - ML model prediction for hidden patterns.
  - Rule engine for policy-based checks such as blacklist and large transfer alerts.
- The system outputs final_flag and explanation reasons, then writes to JSONL and dashboard.

## 3. Live Demo Flow (2-3 min)
- In Terminal 1, I start the Spark streaming consumer.
- In Terminal 2, I run the producer to push live transactions to Kafka.
- In Terminal 3, I open Streamlit to monitor total transactions, fraud count, fraud rate, and recent fraud events.
- As transactions arrive, the console shows clear fraud lines and reason codes.

## 4. What to Say During Logs
- Here we can see a flagged transaction with amount, sender, receiver, and reason labels.
- The reason field indicates whether this was detected by ML, rules, or both.
- This makes the system operationally explainable, not just predictive.

## 5. Key Sentences to Impress Evaluators
- This is not just a model demo. It is a streaming decision system.
- We optimized for both latency and reliability using micro-batch processing with safe error handling.
- Our hybrid detection design balances pattern learning and policy enforcement.
- Every alert is explainable, which is essential for financial compliance and trust.

## 6. Closing (20-30s)
- In summary, this capstone demonstrates production-style architecture: ingestion, stream processing, decision intelligence, and observability.
- It is ready for next-step extensions like PostgreSQL persistence and real alert integrations.
