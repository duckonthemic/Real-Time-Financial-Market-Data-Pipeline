from __future__ import annotations

import pytest

from market_pipeline.analytics.ohlcv import MAX_EVENTS, aggregate_ohlcv


def event(event_id: str, time_ms: int, price: float, volume: int):
    return {"run_id": "run-gold-01", "event_id": event_id, "symbol": "AAPL", "event_time_ms": time_ms, "price": price, "volume": volume}


def test_exact_ohlcv_and_vwap_with_deterministic_tie_break() -> None:
    start = 1735828200000
    rows = [event("b", start, 101.0, 2), event("a", start, 100.0, 1), event("c", start + 1000, 99.0, 3)]
    result = aggregate_ohlcv(rows)[0]
    assert result["open"] == 100.0
    assert result["high"] == 101.0
    assert result["low"] == 99.0
    assert result["close"] == 99.0
    assert result["volume"] == 6
    assert result["trade_count"] == 3
    assert result["vwap"] == pytest.approx((100 + 202 + 297) / 6)


def test_gold_rejects_unbounded_fixture() -> None:
    row = event("a", 1735828200000, 100.0, 1)
    with pytest.raises(ValueError, match=str(MAX_EVENTS)):
        aggregate_ohlcv([row] * (MAX_EVENTS + 1))
