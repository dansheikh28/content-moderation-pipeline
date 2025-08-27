from pathlib import Path

import pandas as pd

EXAMPLE = [
    {"id": 1, "text": "I <b>love</b> this! https://example.com"},
    {"id": 2, "text": "This is SO BAD!!! <script>alert('x')</script> 😡"},
    {"id": 3, "text": "Check www.test.com for more info \n\n New lines."},
]

if __name__ == "__main__":
    out = Path("data/raw/raw_comments.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(EXAMPLE).to_csv(out, index=False)
    print(f"Wrote sample data -> {out.resolve()}")
