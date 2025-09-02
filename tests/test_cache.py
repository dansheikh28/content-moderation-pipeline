def test_cache_roundtrip(tmp_path, monkeypatch):
    # point DB to a temp location
    db_path = tmp_path / "mod_cache.db"
    monkeypatch.setenv("PYTHONHASHSEED", "0")  # deterministic
    # monkeypatch the module-level DB_PATH if you want isolation:
    import importlib

    import moderation.cache as mc

    mc.DB_PATH = db_path
    importlib.reload(mc)

    key = "abc123"
    val = {"score": 0.42, "flagged": True, "labels": {"toxic": 0.42}}
    mc.put(key, val["score"], val["flagged"], val["labels"])
    got = mc.get(key)

    assert got is not None
    assert got["score"] == val["score"]
    assert got["flagged"] == val["flagged"]
    assert got["labels"] == val["labels"]
