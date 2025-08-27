# Content Moderation Pipeline

** End-to-end pipeline for detecting toxic content.
**Batch Scoring (Airflow) lands partitioned Parquet to S3-compatible storage.
**FastAPI service exposes real-time '/moderate'.

## System Diagram (Draft)

```mermaid
flowchart LR
  A[Raw Comments (batch/stream)] --> B[Airflow: Extract & Clean]
  B --> C[Airflow: Score Toxicity (HF Transformer)]
  C --> D[(S3/MinIO: scored Parquet)]
  D --> E[Daily Summary / Analytics]
  G[[FastAPI /moderate]] --> C
