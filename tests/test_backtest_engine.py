"""Tests for BacktestEngine limit fill and Friday week-close cancellation."""

from __future__ import annotations

import pandas as pd

from src.backtest.backtest_engine import BacktestEngine


def _make_df(rows: list[dict]) -> pd.DataFrame:
    out: list[dict] = []
    for r in rows:
        r = dict(r)
        if "timestamp" in r:
            r["timestamp"] = pd.Timestamp(r["timestamp"]).tz_localize("UTC")
        out.append(r)
    return pd.DataFrame(out)


def _session_friday_close(*, avoid_h: int = 5, close_h: int = 21) -> dict:
    return {
        "enabled": True,
        "friday_avoid_hours_before_week_close": avoid_h,
        "friday_week_close_hour_utc": close_h,
        "friday_week_close_minute_utc": 0,
    }


class TestFindLimitFillFridayClose:
    """Friday close window = last N hours before friday_week_close_hour_utc (UTC)."""

    def test_fill_late_friday_when_cancel_off(self) -> None:
        eng = BacktestEngine(
            {
                "backtest": {"cancel_pending_limits_in_friday_close_window": False},
                "session_filters": _session_friday_close(),
            }
        )
        # Thu 12:00 → Fri 17:00 UTC (inside 16:00–21:00 window when avoid=5, close=21)
        df = _make_df(
            [
                {"timestamp": "2025-01-02 12:00:00", "open": 101, "high": 102, "low": 100.5, "close": 100, "volume": 1},
                {"timestamp": "2025-01-03 17:00:00", "open": 100, "high": 102, "low": 99.5, "close": 101, "volume": 1},
            ]
        )
        fb, price, exp = eng._find_limit_fill(df, 0, "BUY", 100.0, 10)
        assert fb == 1
        assert price == 100.0
        assert exp == -1

    def test_cancel_inside_friday_close_window_before_fill(self) -> None:
        eng = BacktestEngine(
            {
                "backtest": {"cancel_pending_limits_in_friday_close_window": True},
                "session_filters": _session_friday_close(avoid_h=5, close_h=21),
            }
        )
        df = _make_df(
            [
                {"timestamp": "2025-01-02 12:00:00", "open": 101, "high": 102, "low": 100.5, "close": 100, "volume": 1},
                {"timestamp": "2025-01-03 17:00:00", "open": 100, "high": 102, "low": 99.0, "close": 101, "volume": 1},
            ]
        )
        # Fri 17:00 UTC in blackout → cancel before attempting fill on that bar
        fb, price, exp = eng._find_limit_fill(df, 0, "BUY", 100.0, 10)
        assert fb == -1
        assert price == 0.0
        assert exp == 1

    def test_thursday_fill_before_friday_close_window(self) -> None:
        eng = BacktestEngine(
            {
                "backtest": {"cancel_pending_limits_in_friday_close_window": True},
                "session_filters": _session_friday_close(),
            }
        )
        df = _make_df(
            [
                {"timestamp": "2025-01-01 12:00:00", "open": 101, "high": 102, "low": 100.5, "close": 101, "volume": 1},
                {"timestamp": "2025-01-02 12:00:00", "open": 101, "high": 101, "low": 99, "close": 100, "volume": 1},
            ]
        )
        fb, price, exp = eng._find_limit_fill(df, 0, "BUY", 100.0, 10)
        assert fb == 1
        assert price == 100.0
        assert exp == -1

    def test_friday_morning_not_blackout_can_fill(self) -> None:
        """Friday before 16:00 UTC is outside default 5h-before-21:00 window."""
        eng = BacktestEngine(
            {
                "backtest": {"cancel_pending_limits_in_friday_close_window": True},
                "session_filters": _session_friday_close(),
            }
        )
        df = _make_df(
            [
                {"timestamp": "2025-01-02 12:00:00", "open": 101, "high": 102, "low": 100.5, "close": 100, "volume": 1},
                {"timestamp": "2025-01-03 12:00:00", "open": 100, "high": 102, "low": 99.5, "close": 101, "volume": 1},
            ]
        )
        fb, price, exp = eng._find_limit_fill(df, 0, "BUY", 100.0, 10)
        assert fb == 1
        assert price == 100.0
        assert exp == -1
