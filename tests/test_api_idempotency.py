from fastapi.testclient import TestClient
from service.app import app

client = TestClient(app)


def test_idempotent_moderate():
    payload = {"text": "fresh text for idempotency 001"}

    r1 = client.post("/moderate", json=payload)
    assert r1.status_code == 200, r1.text
    data1 = r1.json()
    assert "cached" in data1
    assert data1["cached"] is False

    r2 = client.post("/moderate", json=payload)
    assert r2.status_code == 200, r2.text
    data2 = r2.json()
    assert data2["cached"] is True

    # score/labels identical
    assert data1["score"] == data2["score"]
    assert data1["labels"] == data2["labels"]
