from pathlib import Path

import pandas as pd
from moderation.pipeline import clean, run_local


def test_pipeline_clean_adds_clean_text():
    df = pd.DataFrame({"id": [1], "text": ["Hi <i>There</i> www.site.com"]})
    out = clean(df)
    assert "clean_text" in out.columns
    assert out.loc[0, "clean_text"] == "hi there"


def test_run_local_pipeline_end_to_end(tmp_path):
    # Use raw sample file
    raw = Path("data/raw/raw_comments.csv")
    out = tmp_path / "test_output.parquet"

    out_path = run_local(raw, out)
    df = pd.read_parquet(out_path)

    # Check columns
    assert "clean_text" in df.columns
    assert "toxicity_score" in df.columns

    # Validate toxicity_score range
    assert df["toxicity_score"].between(0, 1).all()
