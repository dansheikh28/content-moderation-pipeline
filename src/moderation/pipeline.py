from __future__ import annotations

from pathlib import Path

import pandas as pd

from .cleaning import basic_clean
from .io import read_raw_csv, write_parquet


def ingest(raw_csv_path: str | Path) -> pd.DataFrame:
    return read_raw_csv(raw_csv_path)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned["clean_text"] = cleaned["text"].map(basic_clean)
    # Keep a tidy schema: id (if present), text (raw), clean_text
    cols = ["clean_text"]
    if "text" in cleaned.columns:
        cols.insert(0, "text")
    if "id" in cleaned.columns:
        cols.insert(0, "id")
    return cleaned[cols]


def store(df: pd.DatFrame, out_parquet: str | Path) -> Path:
    return write_parquet(df, out_parquet)


def run_local(raw_csv_path: str | Path, out_parquet: str | Path) -> Path:
    df = ingest(raw_csv_path)
    df = clean(df)
    return store(df, out_parquet)
