from pathlib import Path

import pandas as pd

OUT_PATH = Path("data/scored/cleaned_scored.parquet")


def main():
    df = pd.read_parquet(OUT_PATH)

    print(df.head(), "\n")
    print("Columns:", list(df.columns))
    print("Rows:", len(df))

    # Existing check: clean_text exists & not empty
    if "clean_text" not in df.columns:
        raise ValueError("❌ missing clean_text column")
    if not df["clean_text"].str.len().gt(0).all():
        raise ValueError("❌ found empty clean_text")

    if "toxicity_score" not in df.columns:
        raise ValueError("❌ missing toxicity_score column")
    if not df["toxicity_score"].between(0, 1).all():
        raise ValueError("❌ scores not in [0,1]")

    print("\n✅ all checks passed")


if __name__ == "__main__":
    main()
