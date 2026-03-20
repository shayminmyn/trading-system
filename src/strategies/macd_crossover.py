"""
MACD Crossover Strategy.

Signal rules:
  BUY  — MACD line crosses ABOVE signal line AND histogram turns positive
  SELL — MACD line crosses BELOW signal line AND histogram turns negative

Additional filter: previous bar's histogram must be on opposite side
to avoid catching mid-move entries.
"""

from __future__ import annotations

import pandas as pd
import ta as ta_lib
from ta.trend import MACD
from ta.volatility import AverageTrueRange

from .base_strategy import BaseStrategy, Signal
from ..utils.logger import get_logger

logger = get_logger("macd_crossover")


class MACDCrossoverStrategy(BaseStrategy):
    """
    Parameters (via `parameters` dict):
      fast_period              : int   default 12
      slow_period              : int   default 26
      signal_period            : int   default 9
      min_histogram_threshold  : float default 0.0
      sl_atr_multiplier        : float default 1.5  (SL = ATR × multiplier)
      atr_period               : int   default 14
    """

    def __init__(self, symbol: str, timeframe: str, parameters: dict | None = None) -> None:
        super().__init__(symbol, timeframe, parameters)
        p = self.parameters
        self._fast: int = p.get("fast_period", 12)
        self._slow: int = p.get("slow_period", 26)
        self._signal: int = p.get("signal_period", 9)
        self._hist_threshold: float = p.get("min_histogram_threshold", 0.0)
        self._sl_atr_mult: float = p.get("sl_atr_multiplier", 1.5)
        self._atr_period: int = p.get("atr_period", 14)
        self._min_bars = self._slow + self._signal + 10

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        # MACD — uses `ta` library (pure Python, Python 3.14 compatible)
        macd_ind = MACD(
            close=df["close"],
            window_slow=self._slow,
            window_fast=self._fast,
            window_sign=self._signal,
        )
        df["macd"] = macd_ind.macd()
        df["macd_signal"] = macd_ind.macd_signal()
        df["macd_hist"] = macd_ind.macd_diff()

        # ATR for dynamic Stop Loss
        atr_ind = AverageTrueRange(
            high=df["high"],
            low=df["low"],
            close=df["close"],
            window=self._atr_period,
        )
        df["atr"] = atr_ind.average_true_range()

        return df

    def generate_signal(self, df: pd.DataFrame) -> Signal:
        df = df.dropna(subset=["macd", "macd_signal", "macd_hist", "atr"])
        if len(df) < 2:
            return self._no_signal()

        curr = df.iloc[-1]
        prev = df.iloc[-2]

        hist_curr = curr["macd_hist"]
        hist_prev = prev["macd_hist"]
        entry = curr["close"]
        atr = curr["atr"]

        if atr == 0 or pd.isna(atr):
            return self._no_signal()

        sl_price_distance = atr * self._sl_atr_mult

        # BUY: histogram crosses from negative to positive
        if (
            hist_prev < 0
            and hist_curr > self._hist_threshold
            and curr["macd"] > curr["macd_signal"]
        ):
            sl_pips = self._price_distance_to_pips(sl_price_distance)
            return self._make_signal(
                action="BUY",
                entry=entry,
                sl_pips=sl_pips,
                notes=f"MACD histogram crossover ↑ (hist={hist_curr:.6f})",
            )

        # SELL: histogram crosses from positive to negative
        if (
            hist_prev > 0
            and hist_curr < -self._hist_threshold
            and curr["macd"] < curr["macd_signal"]
        ):
            sl_pips = self._price_distance_to_pips(sl_price_distance)
            return self._make_signal(
                action="SELL",
                entry=entry,
                sl_pips=sl_pips,
                notes=f"MACD histogram crossover ↓ (hist={hist_curr:.6f})",
            )

        return self._no_signal()

    def _price_distance_to_pips(self, distance: float) -> float:
        """Convert raw price distance to pips.
        XAUUSD/XAGUSD : 1 pip = $0.10  (1 USD move = 10 pips)
        JPY pairs     : 1 pip = 0.01
        Standard Forex: 1 pip = 0.0001
        """
        sym = self.symbol.upper()
        if sym in ("XAUUSD", "XAGUSD"):
            return distance / 0.10
        if "JPY" in sym:
            return distance / 0.01
        return distance / 0.0001
