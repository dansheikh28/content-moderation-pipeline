from __future__ import annotations

import json
import sqlite3
from pathlib import Path

DB_PATH = Path(".data/mod_cache.db")


def _conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS requests (
            request_hash TEXT PRIMARY KEY,
            score REAL NOT NULL,
            flagged INTEGER NOT NULL,
            labels_json TEXT NOT NULL
        )
    """
    )
    return con


def get(request_hash: str):
    con = _conn()
    cur = con.execute(
        "SELECT score, flagged, labels_json FROM requests WHERE request_hash=?",
        (request_hash,),
    )
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    score, flagged, labels_json = row
    return {"score": float(score), "flagged": bool(flagged), "labels": json.loads(labels_json)}


def put(request_hash: str, score: float, flagged: bool, labels: dict) -> None:
    con = _conn()
    con.execute(
        "INSERT OR REPLACE INTO requests(request_hash, score, flagged, labels_json) VALUES(?,?,?,?)",
        (request_hash, float(score), int(bool(flagged)), json.dumps(labels, separators=(",", ":"))),
    )
    con.commit()
    con.close()
