"""Bounded deterministic OHLCV/VWAP aggregation over a completed Silver run."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

MAX_EVENTS = 25_000
MAX_EVENTS_PER_SYMBOL_WINDOW = 2_000
WINDOW_MS = 5 * 60 * 1000


def aggregate_ohlcv(events: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    materialized = list(events)
    if len(materialized) > MAX_EVENTS:
        raise ValueError(f"bounded Gold supports at most {MAX_EVENTS} events")
    grouped: dict[tuple[str, int], list[Mapping[str, Any]]] = defaultdict(list)
    for event in materialized:
        window_start_ms = int(event["event_time_ms"]) // WINDOW_MS * WINDOW_MS
        key = (str(event["symbol"]), window_start_ms)
        grouped[key].append(event)
        if len(grouped[key]) > MAX_EVENTS_PER_SYMBOL_WINDOW:
            raise ValueError(
                f"bounded Gold supports at most {MAX_EVENTS_PER_SYMBOL_WINDOW} events per symbol/window"
            )
    output = []
    for (symbol, window_start_ms), rows in sorted(grouped.items()):
        ordered = sorted(rows, key=lambda row: (int(row["event_time_ms"]), str(row["event_id"])))
        total_volume = sum(int(row["volume"]) for row in ordered)
        notional = sum(float(row["price"]) * int(row["volume"]) for row in ordered)
        start = datetime.fromtimestamp(window_start_ms / 1000, tz=UTC)
        output.append(
            {
                "run_id": str(ordered[0]["run_id"]),
                "symbol": symbol,
                "window_date": start.date(),
                "window_start": start,
                "window_end": start + timedelta(minutes=5),
                "open": float(ordered[0]["price"]),
                "high": max(float(row["price"]) for row in ordered),
                "low": min(float(row["price"]) for row in ordered),
                "close": float(ordered[-1]["price"]),
                "volume": total_volume,
                "trade_count": len(ordered),
                "vwap": notional / total_volume,
            }
        )
    return output
