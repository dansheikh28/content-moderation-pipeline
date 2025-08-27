# Content Moderation Pipeline

#### End-to-end pipeline for detecting toxic content.

#### Batch Scoring (Airflow) lands partitioned Parquet to S3-compatible storage.

#### FastAPI service exposes real-time '/moderate'.

## System Diagram (Draft)

```mermaid
flowchart LR
  A[Raw Comments (batch/stream)]
  B[Airflow: Extract & Clean]
  C[Airflow: Score Toxicity (HF Transformer)]
  D[(S3/MinIO: scored Parquet)]
  E[Daily Summary / Analytics]
  G[[FastAPI /moderate]]

  A --> B --> C --> D --> E
  G --> C
