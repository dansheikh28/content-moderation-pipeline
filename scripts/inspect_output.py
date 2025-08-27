import pandas as pd

path = "data/clean/cleaned_comments.parquet"
df = pd.read_parquet(path)
print(df.head(10))
print("\nColumns:", list(df.columns))
print("Rows:", len(df))
assert "clean_text" in df.columns  # noqa: S101
assert df["clean_text"].str.len().gt(0).all()  # noqa: S101
print("\n✅ basic checks passed")
