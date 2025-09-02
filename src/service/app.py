import hashlib
import logging
import time

from fastapi import FastAPI, HTTPException
from moderation.cache import get as cache_get
from moderation.cache import put as cache_put
from moderation.metrics import meter
from moderation.pipeline import MODEL_VERSION, MULTI_LABELS, score_multilabel
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)


app = FastAPI(title="Moderation API", version="0.1.0")


class ModerationIn(BaseModel):
    text: str


def _moderate(text: str):
    t0 = time.perf_counter()

    # 1) validate & normalize
    t = (text or "").strip()
    if not t:
        raise HTTPException(status_code=400, detail="text is required")

    # 2) build deterministic cache key (text + model version)
    key = f"{t}|{MODEL_VERSION}"
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()

    # 3) cache lookup
    cached = cache_get(h)
    if cached:
        latency_ms = (time.perf_counter() - t0) * 1000.0
        meter.inc(cache_hit=True, flagged=bool(cached["flagged"]), latency_ms=latency_ms)
        logging.info(
            "moderate",
            extra={
                "route": "/moderate",
                "cached": True,
                "flagged": bool(cached["flagged"]),
                "latency_ms": latency_ms,
                "text_len": len(t),
            },
        )
        return {
            "flagged": bool(cached["flagged"]),
            "score": float(cached["score"]),
            "labels": {k: float(v) for k, v in cached["labels"].items()},
            "cached": True,
        }

    # 4) compute fresh
    df = score_multilabel([t])  # 1-row DataFrame
    labels = {k: float(df.loc[0, k]) for k in MULTI_LABELS}
    resp = {
        "flagged": bool(df.loc[0, "flagged"]),
        "score": float(df.loc[0, "toxic"]),
        "labels": labels,
        "cached": False,
    }

    # 5) write-through cache
    cache_put(h, resp["score"], resp["flagged"], labels)

    # 6) metrics + log
    latency_ms = (time.perf_counter() - t0) * 1000.0
    meter.inc(cache_hit=False, flagged=resp["flagged"], latency_ms=latency_ms)
    logging.info(
        "moderate",
        extra={
            "route": "/moderate",
            "cached": False,
            "flagged": resp["flagged"],
            "latency_ms": latency_ms,
            "text_len": len(t),
        },
    )

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


@app.get("/metrics")
def metrics():
    return meter.snapshot()
