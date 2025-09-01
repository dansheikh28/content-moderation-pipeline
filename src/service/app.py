from fastapi import FastAPI, HTTPException
from moderation.pipeline import MULTI_LABELS, score_multilabel
from pydantic import BaseModel

app = FastAPI(title="Moderatioon API", version="0.1.0")


class ModerationIn(BaseModel):
    text: str


def _moderate(text: str):
    t = (text or "").strip()
    if not t:
        raise HTTPException(status_code=400, detail="text is required")
    df = score_multilabel([t])  # returns 1-row DataFrame
    labels = {k: float(df.loc[0, k]) for k in MULTI_LABELS}
    resp = {
        "flagged": bool(df.loc[0, "flagged"]),
        "score": float(df.loc[0, "toxic"]),
        "labels": labels,
    }
    return resp


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/moderate")
def moderate_get(text: str):
    return _moderate(text)


@app.post("/moderate")
def moderate_post(payload: ModerationIn):
    return _moderate(payload.text)
