"""
Abstract base class for all trading strategies.

Design principles:
  - Subclasses override calculate_indicators() and generate_signal()
  - update_data() feeds new bars from DataManager callbacks
  - Thread-safe: each strategy instance owns its own DataFrame
    and is called from a single stream thread, so no lock needed
    for the data itself. The signal queue is thread-safe.
"""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

import pandas as pd

from ..utils.logger import get_logger

logger = get_logger("base_strategy")

SignalAction = Literal["BUY", "SELL", "NONE"]


@dataclass
class Signal:
    """Raw signal produced by a strategy before risk calculation."""

    action: SignalAction
    symbol: str
    timeframe: str
    strategy_name: str
    entry: float
    sl_pips: float                    # Stop-loss distance in pips
    timestamp: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    notes: str = ""

    def is_actionable(self) -> bool:
        return self.action in ("BUY", "SELL") and self.sl_pips > 0

    def __str__(self) -> str:
        return (
            f"Signal({self.action} {self.symbol}/{self.timeframe} "
            f"entry={self.entry:.5f} sl_pips={self.sl_pips:.1f} "
            f"strategy={self.strategy_name})"
        )


class BaseStrategy(ABC):
    """
    Abstract base for all trading strategies.

    Subclass must implement:
      - calculate_indicators(df) → pd.DataFrame   (add indicator columns)
      - generate_signal(df)     → Signal           (return signal from last row)
    """

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        parameters: dict | None = None,
    ) -> None:
        self.symbol = symbol
        self.timeframe = timeframe
        self.parameters: dict = parameters or {}
        self._data: pd.DataFrame = pd.DataFrame()
        self._min_bars: int = 50          # subclass may override

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def update_data(self, df: pd.DataFrame) -> None:
        """Receive fresh OHLCV DataFrame from DataManager."""
        self._data = df.copy()

    def on_new_bar(
        self,
        symbol: str,
        timeframe: str,
        df: pd.DataFrame,
    ) -> Signal | None:
        """
        Entry point called by DataManager on each new bar.
        Runs calculate_indicators then generate_signal.
        Returns None if not enough data or signal is NONE.
        """
        if len(df) < self._min_bars:
            return None

        self.update_data(df)
        try:
            enriched = self.calculate_indicators(self._data.copy())
            signal = self.generate_signal(enriched)
        except Exception:
            logger.exception("%s failed on %s %s", self.name, symbol, timeframe)
            return None

        if signal.is_actionable():
            logger.info("%s | %s", self.name, signal)
        return signal

    # ── Abstract methods ─────────────────────────────────────────────────────

    @abstractmethod
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add technical indicator columns to `df` and return it.
        Do NOT modify the original DataFrame.
        """

    @abstractmethod
    def generate_signal(self, df: pd.DataFrame) -> Signal:
        """
        Examine the last row(s) of `df` (with indicators) and return a Signal.
        Always return a Signal object — use action="NONE" for no signal.
        """

    # ── Helpers for subclasses ────────────────────────────────────────────────

    def _no_signal(self) -> Signal:
        return Signal(
            action="NONE",
            symbol=self.symbol,
            timeframe=self.timeframe,
            strategy_name=self.name,
            entry=0.0,
            sl_pips=0.0,
        )

    def _make_signal(
        self,
        action: SignalAction,
        entry: float,
        sl_pips: float,
        notes: str = "",
    ) -> Signal:
        return Signal(
            action=action,
            symbol=self.symbol,
            timeframe=self.timeframe,
            strategy_name=self.name,
            entry=entry,
            sl_pips=sl_pips,
            notes=notes,
        )
