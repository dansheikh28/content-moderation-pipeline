from __future__ import annotations

from pathlib import Path

import pandas as pd
from transformers import pipeline

from .cleaning import basic_clean
from .io import read_raw_csv, write_parquet

# Lazy load - so the model doesn not reload every call
_toxicity_model = None


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


def store(df: pd.DataFrame, out_parquet: str | Path) -> Path:
    return write_parquet(df, out_parquet)


def get_toxicity_model():
    global _toxicity_model
    if _toxicity_model is None:
        _toxicity_model = pipeline(
            "text-classification",
            model="unitary/toxic-bert",
            tokenizer="unitary/toxic-bert",
            top_k=None,
        )
    return _toxicity_model


def score_toxicity(texts):
    """
    Given a list of texts, return toxicity probabilities.
    """
    model = get_toxicity_model()
    results = model(texts, truncation=True)

    # results is a list of [{label: prob, lab: prob, ...}]
    # Extract "toxic" probability
    scores = []
    for r in results:
        toxic_prob = None
        for item in r:
            if item["label"].lower() == "toxic":
                toxic_prob = item["score"]
        scores.append(toxic_prob if toxic_prob is not None else 0.0)

    return scores


def run_local(raw_csv_path: str | Path, out_parquet: str | Path) -> Path:
    # 1. Ingest raw
    df = ingest(raw_csv_path)

    # 2. Clean
    df = clean(df)

    # 3. Score toxicity
    df["toxicity_score"] = score_toxicity(df["clean_text"].tolist())

    # 4. Store
    return store(df, out_parquet)
