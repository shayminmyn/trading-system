"""
RSI + EMA Crossover Strategy.

Signal rules:
  BUY  — Fast EMA crosses above Slow EMA AND RSI is in oversold recovery
         (RSI was below oversold_level, now rising above it)
  SELL — Fast EMA crosses below Slow EMA AND RSI is in overbought reversal
         (RSI was above overbought_level, now falling below it)

Stop Loss: placed at the recent swing low/high (ATR-based fallback).
"""

from __future__ import annotations

import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange

from .base_strategy import BaseStrategy, Signal
from ..utils.logger import get_logger

logger = get_logger("rsi_ema")


class RSI_EMA_Strategy(BaseStrategy):
    """
    Parameters (via `parameters` dict):
      rsi_period       : int   default 14
      ema_fast         : int   default 9
      ema_slow         : int   default 21
      rsi_overbought   : int   default 70
      rsi_oversold     : int   default 30
      sl_atr_multiplier: float default 1.5
      atr_period       : int   default 14
    """

    def __init__(self, symbol: str, timeframe: str, parameters: dict | None = None) -> None:
        super().__init__(symbol, timeframe, parameters)
        p = self.parameters
        self._rsi_period: int = p.get("rsi_period", 14)
        self._ema_fast: int = p.get("ema_fast", 9)
        self._ema_slow: int = p.get("ema_slow", 21)
        self._rsi_ob: int = p.get("rsi_overbought", 70)
        self._rsi_os: int = p.get("rsi_oversold", 30)
        self._sl_atr_mult: float = p.get("sl_atr_multiplier", 1.5)
        self._atr_period: int = p.get("atr_period", 14)
        self._min_bars = max(self._ema_slow, self._rsi_period) + 10

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        # RSI — uses `ta` library (pure Python, Python 3.14 compatible)
        df["rsi"] = RSIIndicator(close=df["close"], window=self._rsi_period).rsi()

        # EMAs
        df["ema_fast"] = EMAIndicator(close=df["close"], window=self._ema_fast).ema_indicator()
        df["ema_slow"] = EMAIndicator(close=df["close"], window=self._ema_slow).ema_indicator()

        # ATR
        df["atr"] = AverageTrueRange(
            high=df["high"], low=df["low"], close=df["close"], window=self._atr_period
        ).average_true_range()

        return df

    def generate_signal(self, df: pd.DataFrame) -> Signal:
        df = df.dropna(subset=["rsi", "ema_fast", "ema_slow", "atr"])
        if len(df) < 2:
            return self._no_signal()

        curr = df.iloc[-1]
        prev = df.iloc[-2]

        entry = curr["close"]
        atr = curr["atr"]
        if atr == 0 or pd.isna(atr):
            return self._no_signal()

        sl_distance = atr * self._sl_atr_mult
        sl_pips = self._price_distance_to_pips(sl_distance)

        ema_bullish_cross = prev["ema_fast"] <= prev["ema_slow"] and curr["ema_fast"] > curr["ema_slow"]
        ema_bearish_cross = prev["ema_fast"] >= prev["ema_slow"] and curr["ema_fast"] < curr["ema_slow"]
        rsi_oversold_recovery = prev["rsi"] < self._rsi_os and curr["rsi"] >= self._rsi_os
        rsi_overbought_reversal = prev["rsi"] > self._rsi_ob and curr["rsi"] <= self._rsi_ob

        # Relaxed filter: EMA cross + RSI not in extreme opposite zone
        if ema_bullish_cross and curr["rsi"] < self._rsi_ob:
            notes = f"EMA cross ↑ RSI={curr['rsi']:.1f}"
            return self._make_signal("BUY", entry, sl_pips, notes)

        if ema_bearish_cross and curr["rsi"] > self._rsi_os:
            notes = f"EMA cross ↓ RSI={curr['rsi']:.1f}"
            return self._make_signal("SELL", entry, sl_pips, notes)

        return self._no_signal()

    def _price_distance_to_pips(self, distance: float) -> float:
        if self.symbol == "XAUUSD":
            return distance / 0.01
        return distance / 0.0001
