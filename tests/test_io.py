# tests/test_io.py
import pandas as pd
from moderation.io import (
    already_processed,
    mark_processed,
    read_raw_csv,
    write_parquet,
)


def test_idempotency_helpers(tmp_path):
    # Arrange: make a fake incoming CSV
    csv_path = tmp_path / "incoming.csv"
    pd.DataFrame({"text": ["hello", "world"]}).to_csv(csv_path, index=False)

    # Ensure state file is isolated to this test run
    # Redirect STATE_FILE to a temp location (same structure under tmp_path)
    # NOTE: We rebind the module-level STATE_FILE for this test only.
    #       This avoids touching repo's real data/processed/.
    tmp_state_dir = tmp_path / "data" / "processed"
    tmp_state_dir.mkdir(parents=True, exist_ok=True)
    tmp_state_file = tmp_state_dir / ".processed_index.json"

    import moderation.io as io_mod

    io_mod.STATE_FILE = tmp_state_file

    # Act & Assert
    assert already_processed(csv_path) is False

    mark_processed(csv_path)
    assert already_processed(csv_path) is True

    # Modifying the file should change hash → not processed yet
    with open(csv_path, "a") as f:
        f.write("\n")

    assert already_processed(csv_path) is False


def test_read_and_write_roundtrip(tmp_path):
    # Arrange
    df = pd.DataFrame({"text": ["a", "b", "c"]})
    parquet_path = tmp_path / "out.parquet"

    # Act
    out = write_parquet(df, parquet_path)
    assert out.exists()

    # Create a CSV and read it back with read_raw_csv
    csv_path = tmp_path / "in.csv"
    df.to_csv(csv_path, index=False)
    df2 = read_raw_csv(csv_path)

    # Assert
    assert list(df2.columns) == ["text"]
    assert len(df2) == 3
