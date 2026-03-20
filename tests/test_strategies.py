"""Unit tests for Strategy classes."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

from src.strategies.base_strategy import Signal, BaseStrategy
from src.strategies.macd_crossover import MACDCrossoverStrategy
from src.strategies.rsi_ema import RSI_EMA_Strategy
from src.data.mock_source import generate_ohlcv


class TestSignal:
    def test_actionable_buy(self):
        s = Signal(action="BUY", symbol="XAUUSD", timeframe="H1",
                   strategy_name="test", entry=2150.0, sl_pips=30.0)
        assert s.is_actionable() is True

    def test_not_actionable_none(self):
        s = Signal(action="NONE", symbol="XAUUSD", timeframe="H1",
                   strategy_name="test", entry=0.0, sl_pips=0.0)
        assert s.is_actionable() is False

    def test_not_actionable_zero_sl(self):
        s = Signal(action="BUY", symbol="XAUUSD", timeframe="H1",
                   strategy_name="test", entry=2150.0, sl_pips=0.0)
        assert s.is_actionable() is False


class TestMACDCrossover:
    def setup_method(self):
        self.strategy = MACDCrossoverStrategy(
            symbol="XAUUSD",
            timeframe="H1",
            parameters={"fast_period": 12, "slow_period": 26, "signal_period": 9},
        )

    def test_returns_signal_object(self):
        df = _make_df(300)
        result = self.strategy.on_new_bar("XAUUSD", "H1", df)
        # May be None (not enough cross detected) or a Signal
        assert result is None or isinstance(result, Signal)

    def test_not_enough_bars_returns_none(self):
        df = _make_df(10)
        result = self.strategy.on_new_bar("XAUUSD", "H1", df)
        assert result is None

    def test_calculate_indicators_adds_columns(self):
        df = _make_df(300)
        enriched = self.strategy.calculate_indicators(df)
        assert "macd" in enriched.columns
        assert "macd_signal" in enriched.columns
        assert "macd_hist" in enriched.columns
        assert "atr" in enriched.columns

    def test_signal_action_valid(self):
        """Any generated signal has valid action field."""
        df = _make_df(500, seed=999)
        enriched = self.strategy.calculate_indicators(df.copy())
        signal = self.strategy.generate_signal(enriched)
        assert signal.action in ("BUY", "SELL", "NONE")

    def test_buy_signal_has_positive_sl_pips(self):
        """BUY signals always have sl_pips > 0."""
        df = _make_df(500, seed=999)
        enriched = self.strategy.calculate_indicators(df.copy())
        signal = self.strategy.generate_signal(enriched)
        if signal.action == "BUY":
            assert signal.sl_pips > 0

    def test_name_property(self):
        assert self.strategy.name == "MACDCrossoverStrategy"


def _make_df(n_bars: int, symbol: str = "XAUUSD", tf: str = "H1", seed: int = 123) -> pd.DataFrame:
    return generate_ohlcv(symbol, tf, n_bars=n_bars, seed=seed)


class TestRSI_EMA:
    def setup_method(self):
        self.strategy = RSI_EMA_Strategy(
            symbol="EURUSD",
            timeframe="M15",
            parameters={"rsi_period": 14, "ema_fast": 9, "ema_slow": 21},
        )

    def test_returns_signal_on_enough_data(self):
        df = _make_df(200, symbol="EURUSD", tf="M15")
        result = self.strategy.on_new_bar("EURUSD", "M15", df)
        assert result is None or isinstance(result, Signal)

    def test_too_few_bars_returns_none(self):
        df = _make_df(5, symbol="EURUSD", tf="M15")
        result = self.strategy.on_new_bar("EURUSD", "M15", df)
        assert result is None

    def test_calculate_indicators_adds_columns(self):
        df = _make_df(200, symbol="EURUSD", tf="M15")
        enriched = self.strategy.calculate_indicators(df)
        assert "rsi" in enriched.columns
        assert "ema_fast" in enriched.columns
        assert "ema_slow" in enriched.columns
        assert "atr" in enriched.columns

    def test_signal_symbol_matches(self):
        df = _make_df(200, symbol="EURUSD", tf="M15")
        enriched = self.strategy.calculate_indicators(df.copy())
        signal = self.strategy.generate_signal(enriched)
        assert signal.symbol == "EURUSD"

    def test_signal_strategy_name(self):
        df = _make_df(200, symbol="EURUSD", tf="M15")
        enriched = self.strategy.calculate_indicators(df.copy())
        signal = self.strategy.generate_signal(enriched)
        assert signal.strategy_name == "RSI_EMA_Strategy"
