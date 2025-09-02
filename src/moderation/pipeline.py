from __future__ import annotations

from pathlib import Path

import pandas as pd
from transformers import pipeline

from .cleaning import basic_clean
from .io import read_raw_csv, write_parquet

# Lazy load - so the model doesn not reload every call
_toxicity_model = None
MODEL_VERSION = "unitary/toxic-bert@v1"

# Multi label thresholding
MULTI_LABELS = ["toxic", "severe_toxic", "obscene", "threat", "insult", "identity_hate"]
THRESHOLD = 0.5  # flag a row if any label >= 0.5


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


def score_multilabel(texts) -> pd.DataFrame:
    """
    Given a list of texts, return a DataFrame with columns for each label in MULTI_LABELS,
    plus a boolean 'flagged' column indicating any label >= THRESHOLD.
    """
    if not isinstance(texts, list):
        texts = list(texts)

    model = get_toxicity_model()
    results = model(texts, truncation=True)  # list[list[{'label', 'score'},...]]

    rows = []
    for r in results:
        row_scores = {label: 0.0 for label in MULTI_LABELS}
        for item in r:
            lab = item["label"].lower()
            if lab in row_scores:
                row_scores[lab] = float(item["score"])
        rows.append(row_scores)

    out = pd.DataFrame(rows)
    out["flagged"] = (out[MULTI_LABELS] >= THRESHOLD).any(axis=1)
    return out


def run_local(raw_csv_path: str | Path, out_parquet: str | Path) -> Path:
    # 1. Ingest raw
    df = ingest(raw_csv_path)

    # 2. Clean
    df = clean(df)

    # 3. Score (multi-label)
    ml = score_multilabel(df["clean_text"].tolist())
    df["toxicity_score"] = ml["toxic"]

    # Merge the rest of the labels + flagged
    for col in MULTI_LABELS + ["flagged"]:
        df[col] = ml[col]

    # 4. Store
    return store(df, out_parquet)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run local moderation pipeline")
    parser.add_argument("raw_csv", help="Path to raw CSV with comments")
    parser.add_argument("out_parquet", help="Path to output scored Parquet file")
    args = parser.parse_args()

    out_path = run_local(args.raw_csv, args.out_parquet)
    print(f"Wrote scored file to {out_path}")
