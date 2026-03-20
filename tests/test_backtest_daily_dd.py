"""Tests for daily max drawdown metric."""

from __future__ import annotations

import pandas as pd

from src.backtest.backtest_engine import BacktestEngine


class TestMaxDrawdownDaily:
    def test_single_day_loss(self) -> None:
        eng = BacktestEngine({"backtest": {}})
        # One trade, exit same day — equity 10000 -> 9000 => 10% DD that day
        ts = pd.Timestamp("2025-06-01 12:00:00", tz="UTC")
        trades = [
            {
                "exit_timestamp": ts,
                "pips": 0.0,
            }
        ]
        equity_curve = [10000.0, 9000.0]
        dd = eng._max_drawdown_daily_pct(trades, equity_curve, "UTC")
        assert dd == 10.0

    def test_two_days_peak_trough(self) -> None:
        eng = BacktestEngine({"backtest": {}})
        t1 = pd.Timestamp("2025-06-01 10:00:00", tz="UTC")
        t2 = pd.Timestamp("2025-06-02 10:00:00", tz="UTC")
        trades = [
            {"exit_timestamp": t1},
            {"exit_timestamp": t2},
        ]
        # Day1: 10000 -> 8000 (20% DD intraday). Day2: carry 8000 -> 9000 (no worse daily)
        equity_curve = [10000.0, 8000.0, 9000.0]
        dd = eng._max_drawdown_daily_pct(trades, equity_curve, "UTC")
        assert dd == 20.0
