"""Finalize deterministic five-minute Gold OHLCV/VWAP from a completed Silver run."""

from __future__ import annotations

from datetime import UTC
from pathlib import Path
from typing import Any

from market_pipeline.analytics.ohlcv import aggregate_ohlcv
from market_pipeline.ops.runtime import artifact, atomic_json, run_config_from_env


def _event(row: Any, run_id: str) -> dict[str, Any]:
    event_time = row.event_time
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=UTC)
    return {
        "run_id": run_id,
        "event_id": str(row.event_id),
        "symbol": str(row.symbol),
        "event_time_ms": int(event_time.timestamp() * 1000),
        "price": float(row.price),
        "volume": int(row.volume),
    }


def main() -> int:
    from cassandra.cluster import Cluster

    config = run_config_from_env()
    progress_path = Path(config.artifact_path) / "gold-progress.json"
    cluster = Cluster([config.cassandra_host])
    session = cluster.connect(config.cassandra_keyspace)
    try:
        silver = list(
            session.execute(
                "SELECT event_id,symbol,event_time,price,volume FROM silver_events_by_run WHERE run_id=%s",
                (config.run_id,),
            )
        )
        rows = aggregate_ohlcv(_event(row, config.run_id) for row in silver)
        statement = session.prepare(
            "INSERT INTO gold_ohlcv_5m_by_symbol_day (run_id,symbol,window_date,window_start,window_end,open,high,low,close,volume,trade_count,vwap) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
        )
        for row in rows:
            session.execute(
                statement,
                (
                    row["run_id"],
                    row["symbol"],
                    row["window_date"],
                    row["window_start"],
                    row["window_end"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                    row["trade_count"],
                    row["vwap"],
                ),
            )
        atomic_json(
            progress_path,
            artifact(
                "gold-progress",
                config.run_id,
                state="COMPLETED",
                source_silver_records=len(silver),
                gold_rows=len(rows),
            ),
        )
        return 0
    except Exception as exc:
        atomic_json(
            progress_path,
            artifact("gold-progress", config.run_id, state="FAILED", error=str(exc)[:1000]),
        )
        raise
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
