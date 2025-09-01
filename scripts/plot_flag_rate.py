from __future__ import annotations

import datetime as dt
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

IN_DIR = Path("data/scored")
OUT_DIR = Path("reports")
OUT_DIR.mkdir(parents=True, exist_ok=True)
png_path = OUT_DIR / "flag_rate.png"


def load_scored() -> pd.DataFrame:
    files = sorted(IN_DIR.glob("*.parquet"))
    if not files:
        raise SystemExit("No Parquet files in data/scored/")
    dfs = []
    for f in files:
        df = pd.read_parquet(f)
        # ensure boolean flagged column exists
        if "flagged" not in df.columns:
            raise SystemExit(f"'flagged' column missing in {f}")
        # derive a date column
        if "timestamp" in df.columns:
            date = pd.to_datetime(df["timestamp"]).dt.date
        elif "created_at" in df.columns:
            date = pd.to_datetime(df["created_at"]).dt.date
        else:
            # fallback: file mtime as the date
            date = pd.Series([dt.date.fromtimestamp(f.stat().st_mtime)] * len(df))
        df = df.assign(day=date)
        dfs.append(df[["flagged", "day"]])
    return pd.concat(dfs, ignore_index=True)


def main():
    df = load_scored()
    daily = (
        df.groupby("day")["flagged"]
        .agg(total="count", flagged="sum")
        .assign(flag_rate=lambda x: (x["flagged"] / x["total"]) * 100.0)
        .reset_index()
    )
    # Plot
    plt.figure()
    plt.plot(daily["day"], daily["flag_rate"], marker="o")
    plt.title("Daily Flag Rate (%)")
    plt.xlabel("Day")
    plt.ylabel("Flag Rate (%)")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(png_path)
    print(f"Wrote {png_path}")


if __name__ == "__main__":
    main()
