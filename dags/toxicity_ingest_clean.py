from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Use your existing code in src/moderation/*
from moderation.pipeline import clean, ingest, score_toxicity, store

# Resolve repo root: .../content-moderation-pipeline
REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_CSV = REPO_ROOT / "data" / "raw" / "raw_comments.csv"
OUT_PARQUET_CLEAN = REPO_ROOT / "data" / "clean" / "cleaned_comments.parquet"
OUT_PARQUET_SCORED = REPO_ROOT / "data" / "clean" / "scored_comments.parquet"

default_args = {"owner": "ml", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="toxicity_ingest_clean",
    description="Ingest -> Clean -> (Store Clean + Score -> Store Scored)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # run manually for now
    catchup=False,
    tags=["moderation", "etl"],
) as dag:

    def _extract(**context):
        df = ingest(RAW_CSV)
        context["ti"].xcom_push(key="raw_records", value=df.to_dict(orient="records"))

    def _clean(**context):
        import pandas as pd

        records = context["ti"].xcom_pull(key="raw_records", task_ids="extract")
        df = pd.DataFrame.from_records(records)
        cleaned = clean(df)
        context["ti"].xcom_push(key="clean_records", value=cleaned.to_dict(orient="records"))

    def _store_clean(**context):
        import pandas as pd

        records = context["ti"].xcom_pull(key="clean_records", task_ids="clean_text")
        df = pd.DataFrame.from_records(records)
        path = store(df, OUT_PARQUET_CLEAN)
        return str(path)

    def _score(**context):
        import pandas as pd

        records = context["ti"].xcom_pull(key="clean_records", task_ids="clean_text")
        df = pd.DataFrame.from_records(records)
        # Compute toxicity on clean_text
        df["toxicity_score"] = score_toxicity(df["clean_text"].tolist())
        context["ti"].xcom_push(key="scored_records", value=df.to_dict(orient="records"))

    def _store_scored(**context):
        import pandas as pd

        records = context["ti"].xcom_pull(key="scored_records", task_ids="score")
        df = pd.DataFrame.from_records(records)
        OUT_PARQUET_SCORED.parent.mkdir(parents=True, exist_ok=True)
        path = store(df, OUT_PARQUET_SCORED)
        return str(path)

    extract = PythonOperator(task_id="extract", python_callable=_extract)
    clean_text = PythonOperator(task_id="clean_text", python_callable=_clean)
    store_clean = PythonOperator(task_id="store_clean", python_callable=_store_clean)
    score = PythonOperator(task_id="score", python_callable=_score)
    store_scored = PythonOperator(task_id="store_scored", python_callable=_store_scored)

    # Graph
    extract >> clean_text
    clean_text >> store_clean
    clean_text >> score >> store_scored
