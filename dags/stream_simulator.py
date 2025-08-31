from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from moderation.io import already_processed
from moderation.pipeline import clean, ingest, score_multilabel, store

REPO_ROOT = Path(__file__).resolve().parents[1]
INCOMING_DIR = REPO_ROOT / "data" / "incoming"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
SCORED_DIR = REPO_ROOT / "data" / "scored"
QUARANTINE_DIR = REPO_ROOT / "data" / "quarantine"
MAX_STATE_ENTRIES = 5000  # simple cap for .processed_index.json

# TODO(Storage): switch to fsspec (S3/MinIO) via env flag; keep local paths for now.


default_args = {"owner": "ml", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="stream_simulator",
    description="Simulated streaming pipeline: watch incoming CSVs, process, archive",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/10 * * * *",  # every 10 min
    catchup=False,
    tags=["moderation", "stream"],
) as dag:

    def _list_new_files(**context):
        files = list(INCOMING_DIR.glob("*.csv"))
        new_files = [str(f) for f in files if not already_processed(f)]
        context["ti"].xcom_push(key="new_files", value=new_files)
        return new_files

    def _process_file(ds=None, **context):
        from pathlib import Path

        ti = context["ti"]
        new_files = ti.xcom_pull(key="new_files", task_ids="list_new_files") or []
        if not new_files:
            logging.info("No new files to process.")
            # push empty lists so downstream doesn't break
            ti.xcom_push(key="processed_files", value=[])
            ti.xcom_push(key="failed_files", value=[])
            return None

        ds_str = ds or context.get("ds")
        out_dir = SCORED_DIR / f"dt={ds_str}"
        out_dir.mkdir(parents=True, exist_ok=True)

        processed_files = []
        failed_files = []

        for fpath in new_files:
            src_path = Path(fpath)
            try:
                logging.info("Processing file: %s", src_path.name)

                # 1) Ingest
                df = ingest(src_path)

                # 2) Clean
                df = clean(df)

                # 3) Score (multi-label)
                ml = score_multilabel(df["clean_text"].tolist())

                # attach scores + flagged
                for col in ml.columns:
                    df[col] = ml[col].values
                if "toxicity_score" not in df.columns and "toxic" in df.columns:
                    df["toxicity_score"] = df["toxic"]

                # 4) Store to partitioned scored dir
                out_path = out_dir / f"stream_{src_path.stem}.parquet"
                store(df, out_path)

                processed_files.append(str(src_path))
                logging.info("Wrote: %s", out_path)
            except Exception:
                logging.exception("Failed processing %s", src_path.name)
                failed_files.append(str(src_path))

        # XCom for archive step
        ti.xcom_push(key="processed_files", value=processed_files)
        ti.xcom_push(key="failed_files", value=failed_files)

        return {"processed_count": len(processed_files), "failed_count": len(failed_files)}

    def _archive_file(**context):
        import json
        import shutil
        from pathlib import Path

        from moderation.io import STATE_FILE, mark_processed  # ensure available

        ti = context["ti"]
        processed_files = ti.xcom_pull(key="processed_files", task_ids="process_file") or []
        failed_files = ti.xcom_pull(key="failed_files", task_ids="process_file") or []

        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

        # Move processed -> processed dir and mark processed
        for p in processed_files:
            src = Path(p)
            if not src.exists():
                continue
            dst = PROCESSED_DIR / src.name
            shutil.move(str(src), str(dst))
            mark_processed(dst)
            logging.info("Archived: %s", dst)

        # Move failed -> quarantine dir (do NOT mark processed)
        for p in failed_files:
            src = Path(p)
            if not src.exists():
                continue
            dst = QUARANTINE_DIR / src.name
            shutil.move(str(src), str(dst))
            logging.info("Quarantined: %s", dst)

        # Compact the state file if it grows too large
        try:
            if STATE_FILE.exists():
                data = json.loads(STATE_FILE.read_text() or "[]")
                if isinstance(data, list) and len(data) > MAX_STATE_ENTRIES:
                    # Keep the most recent entries (simple tail strategy)
                    trimmed = data[-MAX_STATE_ENTRIES:]
                    STATE_FILE.write_text(json.dumps(trimmed))
                    logging.info(
                        "Compacted state index from %d -> %d entries", len(data), len(trimmed)
                    )
        except Exception:
            logging.exception("Failed to compact processed state file")

        return {
            "archived": len(processed_files),
            "quarantined": len(failed_files),
        }

    list_new_files = PythonOperator(task_id="list_new_files", python_callable=_list_new_files)
    process_file = PythonOperator(task_id="process_file", python_callable=_process_file)
    archive_file = PythonOperator(task_id="archive_file", python_callable=_archive_file)

    list_new_files >> process_file >> archive_file
