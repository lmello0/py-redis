import store


def _freeze_time(monkeypatch, initial: float = 100.0) -> dict[str, float]:
    current = {"value": initial}
    monkeypatch.setattr(store.time, "time", lambda: current["value"])
    return current


def test_store_set_and_get_without_ttl(monkeypatch) -> None:
    _freeze_time(monkeypatch)
    data = store.make_store()
    data = store.store_set(data, "name", "redis")

    entry = store.store_get(data, "name")
    assert entry == store.StoreEntry("redis", None)


def test_store_get_returns_none_for_expired_entry(monkeypatch) -> None:
    now = _freeze_time(monkeypatch)
    data = store.make_store()
    data = store.store_set(data, "token", "abc", ttl_seconds=5)

    now["value"] = 106.0
    assert store.store_get(data, "token") is None


def test_store_delete_returns_new_store_and_deleted_count(monkeypatch) -> None:
    _freeze_time(monkeypatch)
    data = store.make_store()
    data = store.store_set(data, "a", "1")
    data = store.store_set(data, "b", "2")

    new_data, deleted = store.store_delete(data, "a", "missing")

    assert deleted == 1
    assert "a" not in new_data
    assert "b" in new_data
    assert "a" in data


def test_store_exists_ignores_expired_entries(monkeypatch) -> None:
    now = _freeze_time(monkeypatch)
    data = store.make_store()
    data = store.store_set(data, "live", "1")
    data = store.store_set(data, "ephemeral", "1", ttl_seconds=1)

    now["value"] = 105.0
    assert store.store_exists(data, "live", "ephemeral", "missing") == 1


def test_store_keys_applies_pattern_and_ignores_expired_entries(monkeypatch) -> None:
    now = _freeze_time(monkeypatch)
    data = store.make_store()
    data = store.store_set(data, "foo", "1")
    data = store.store_set(data, "bar", "1")
    data = store.store_set(data, "foo-temp", "1", ttl_seconds=1)

    now["value"] = 102.0
    assert store.store_keys(data, "foo*") == ["foo"]


def test_store_ttl_returns_expected_sentinels_and_remaining(monkeypatch) -> None:
    now = _freeze_time(monkeypatch)
    data = store.make_store()

    assert store.store_ttl(data, "missing") == -2

    data = store.store_set(data, "persistent", "1")
    assert store.store_ttl(data, "persistent") == -1

    data = store.store_set(data, "session", "1", ttl_seconds=10)
    now["value"] = 105.0
    assert store.store_ttl(data, "session") == 5
