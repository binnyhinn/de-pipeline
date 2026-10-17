from datetime import datetime, timezone

from transform import transform_prices

NOW = datetime(2026, 10, 17, tzinfo=timezone.utc)


def make_record(**overrides):
    record = {
        "id": "bitcoin",
        "current_price": 65000.0,
        "market_cap": 1_200_000_000_000,
        "total_volume": 30_000_000_000,
        "price_change_percentage_24h": 1.5,
    }
    record.update(overrides)
    return record


def test_valid_record_is_kept_and_renamed():
    rows = transform_prices([make_record()], NOW)
    assert len(rows) == 1
    assert rows[0]["coin_id"] == "bitcoin"
    assert rows[0]["price_usd"] == 65000.0
    assert rows[0]["fetched_at"] == NOW


def test_missing_price_is_dropped():
    assert transform_prices([make_record(current_price=None)], NOW) == []


def test_zero_or_negative_price_is_dropped():
    records = [make_record(current_price=0), make_record(id="eth", current_price=-5)]
    assert transform_prices(records, NOW) == []


def test_missing_id_is_dropped():
    assert transform_prices([make_record(id=None)], NOW) == []


def test_duplicates_are_removed():
    records = [make_record(), make_record()]
    assert len(transform_prices(records, NOW)) == 1