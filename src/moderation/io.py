from __future__ import annotations

from pathlib import Path

import pandas as pd


def read_raw_csv(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    df = pd.read_csv(path)
    # Expect at least a 'text' column. If there's is a 'common_text' column
    # mirror it into 'text' for consistency
    if "text" not in df.columns:
        if "comment_text" in df.columns:
            df = df.rename(columns={"comment_text": "text"})
        else:
            raise ValueError("Input CSV must contain 'text' or 'common_text' column.")
    return df


def write_parquet(df: pd.DataFrame, out_path: str | Path) -> Path:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    return out
