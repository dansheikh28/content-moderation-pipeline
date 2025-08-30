from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCORED_DIR = ROOT / "data" / "scored"
LABELS = ["toxic", "severe_toxic", "obscene", "threat", "insult", "identity_hate"]


def latest_partition_path() -> Path:
    """Pick latest dt=YYYY-MM-DD/cleaned_scored.parquet under data/scored."""
    parts = []

    if SCORED_DIR.exists():
        for p in SCORED_DIR.iterdir():
            if p.is_dir() and p.name.startswith("dt="):
                parts.append(p)

    if not parts:
        raise FileNotFoundError("No partition folders like dt=YYYY-MM-DD found in data/scored/")

    # sort by data string inside dt =...
    parts.sort(key=lambda p: p.name.replace("dt=", ""))
    return parts[-1] / "cleaned_scored.parquet"


def main():
    out_path = latest_partition_path()
    print(f"Reading: {out_path}")
    df = pd.read_parquet(out_path)

    print(df.head(), "\n")
    print("Columns:", list(df.columns))
    print("Rows:", len(df))

    # Presence
    if "clean_text" not in df.columns:
        raise ValueError("❌ missing clean_text column")
    for col in LABELS:
        if col not in df.columns:
            raise ValueError(f"❌ missing label column: {col}")
    if "flagged" not in df.columns:
        raise ValueError("❌ missing 'flagged' column")

    # Ranges
    for col in LABELS:
        if not df[col].between(0.0, 1.0, inclusive="both").all():
            raise ValueError(f"❌ values out of [0,1] in {col}")

    # Types
    if str(df["flagged"].dtype) != "bool":
        raise ValueError("❌ 'flagged' must be boolean dtype")

    # Legacy still present
    if "toxicity_score" not in df.columns:
        raise ValueError("❌ missing legacy 'toxicity_score' column")

    print("\n✅ all checks passed")


if __name__ == "__main__":
    main()
