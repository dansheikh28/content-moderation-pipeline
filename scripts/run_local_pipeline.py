from pathlib import Path

from moderation.pipeline import run_local

if __name__ == "__main__":
    raw = Path("data/raw/raw_comments.csv")
    out = Path("data/scored/cleaned_scored.parquet")
    out_path = run_local(raw, out)
    print(f"Scored data written to: {out_path.resolve()}")
