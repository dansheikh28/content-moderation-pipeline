from pathlib import Path

from moderation.pipeline import run_local

if __name__ == "__main__":
    raw = Path("data/raw/raw_comments.csv")
    out = Path("data/clean/cleaned_comments.parquet")
    out_path = run_local(raw, out)
    print(f"Cleaned data written to: {out_path.resolve()}")
