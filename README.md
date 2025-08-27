# Content Moderation Pipeline

#### End-to-end pipeline for detecting toxic content.

#### Batch Scoring (Airflow) lands partitioned Parquet to S3-compatible storage.

#### FastAPI service exposes real-time '/moderate'.

## System Diagram (Draft)

```mermaid
graph LR
  A["Raw Comments (batch/stream)"]
  B["Airflow: Extract & Clean"]
  C["Score Toxicity (HF Transformer)"]
  D["Scored Parquet (S3/MinIO)"]
  E["Daily Summary / Analytics"]
  G["FastAPI /moderate"]

  A --> B
  B --> C
  C --> D
  D --> E
  G --> C
