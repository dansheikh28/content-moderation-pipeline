# Content Moderation Pipeline

#### End-to-end pipeline for detecting toxic content.

#### Batch Scoring (Airflow) lands partitioned Parquet to S3-compatible storage.

#### FastAPI service exposes real-time '/moderate'.

## System Diagram (Draft)

```mermaid
flowchart LR
  A[Start] --> B[OK]
