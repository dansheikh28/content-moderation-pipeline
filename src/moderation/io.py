from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

STATE_FILE = Path("data/processed/.processed_index.json")


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


def _load_state() -> set[str]:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                return set(json.load(f))
        except json.JSONDecodeError:
            return set()
    return set()


def _save_state(processed: set[str]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(list(processed), f)


def _file_hash(path: str | Path) -> str:
    """Generate a simple hash based on filename + size + mtime."""
    p = Path(path)
    stat = p.stat()
    base = f"{p.name}:{stat.st_size}:{stat.st_mtime}"
    return hashlib.sha256(base.encode()).hexdigest()


def already_processed(path: str | Path) -> bool:
    """Check if a file has already been processed."""
    processed = _load_state()
    return _file_hash(path) in processed


def mark_processed(path: str | Path) -> None:
    """Mark a file as processed by adding its hash to the state file."""
    processed = _load_state()
    processed.add(_file_hash(path))
    _save_state(processed)
