"""
Mock data source for development on macOS/Linux (no MT5 available).

Generates realistic synthetic OHLCV data using a random walk model
with volatility calibrated to typical Forex/Gold behavior.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Callable

import numpy as np
import pandas as pd

from ..utils.logger import get_logger

logger = get_logger("mock_source")

# Approximate daily volatility per symbol (in price units)
_VOLATILITY = {
    "XAUUSD": 15.0,    # ~$15/day
    "EURUSD": 0.0060,  # ~60 pips/day
    "GBPUSD": 0.0080,
    "USDJPY": 0.60,
}

_BASE_PRICE = {
    "XAUUSD": 2150.0,
    "EURUSD": 1.0850,
    "GBPUSD": 1.2700,
    "USDJPY": 151.50,
}

_TIMEFRAME_SECONDS = {
    "M1": 60, "M5": 300, "M15": 900, "M30": 1800,
    "H1": 3600, "H4": 14400, "D1": 86400,
}


def _bars_per_day(timeframe: str) -> int:
    tf_sec = _TIMEFRAME_SECONDS.get(timeframe, 3600)
    return max(1, 86400 // tf_sec)


_FIXED_EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)


def generate_ohlcv(
    symbol: str,
    timeframe: str,
    n_bars: int,
    end_time: datetime | None = None,
    seed: int | None = None,
) -> pd.DataFrame:
    """
    Generate synthetic OHLCV bars for `symbol` ending at `end_time`.
    When `seed` is provided and `end_time` is None, uses a fixed reference
    epoch so results are fully reproducible across calls.
    Suitable for strategy backtesting and development without MT5.
    """
    rng = np.random.default_rng(seed)
    tf_sec = _TIMEFRAME_SECONDS.get(timeframe, 3600)
    daily_vol = _VOLATILITY.get(symbol, 0.005)
    bar_vol = daily_vol / np.sqrt(_bars_per_day(timeframe))
    base = _BASE_PRICE.get(symbol, 1.0)

    if end_time is None:
        # Use fixed epoch when seed is set for reproducibility; live time otherwise
        end_time = _FIXED_EPOCH if seed is not None else datetime.now(tz=timezone.utc)

    timestamps = [
        end_time - timedelta(seconds=tf_sec * (n_bars - 1 - i))
        for i in range(n_bars)
    ]

    closes = [base]
    for _ in range(n_bars - 1):
        closes.append(closes[-1] * (1 + rng.normal(0, bar_vol / base)))

    rows = []
    for i, (ts, close) in enumerate(zip(timestamps, closes)):
        spread = bar_vol * 0.5
        high = close + abs(rng.normal(0, spread))
        low = close - abs(rng.normal(0, spread))
        open_ = closes[i - 1] if i > 0 else close
        volume = int(rng.integers(100, 2000))
        rows.append({
            "timestamp": ts,
            "open": round(open_, 5),
            "high": round(max(open_, close, high), 5),
            "low": round(min(open_, close, low), 5),
            "close": round(close, 5),
            "volume": volume,
        })

    return pd.DataFrame(rows)


class MockDataStreamer:
    """
    Simulates a realtime bar stream by emitting synthetic candles
    at accelerated speed (no real waiting). Calls `on_new_bar` callback
    per symbol/timeframe. Thread-safe for no-GIL parallel execution.
    """

    def __init__(
        self,
        symbols: list[str],
        timeframes: list[str],
        on_new_bar: Callable[[str, str, pd.Series], None],
        speed_multiplier: float = 1.0,
    ) -> None:
        self._symbols = symbols
        self._timeframes = timeframes
        self._on_new_bar = on_new_bar
        self._speed = speed_multiplier
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        """Start one thread per (symbol, timeframe) pair — truly parallel with no-GIL."""
        for symbol in self._symbols:
            for tf in self._timeframes:
                t = threading.Thread(
                    target=self._stream_loop,
                    args=(symbol, tf),
                    name=f"mock-stream-{symbol}-{tf}",
                    daemon=True,
                )
                t.start()
                self._threads.append(t)
        logger.info(
            "MockDataStreamer started %d stream threads", len(self._threads)
        )

    def stop(self) -> None:
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=5)
        logger.info("MockDataStreamer stopped")

    def _stream_loop(self, symbol: str, timeframe: str) -> None:
        tf_sec = _TIMEFRAME_SECONDS.get(timeframe, 3600)
        sleep_sec = max(0.1, tf_sec / self._speed)
        bar_index = 0

        while not self._stop_event.is_set():
            bar = generate_ohlcv(symbol, timeframe, n_bars=1, seed=bar_index)[
                [
                    "timestamp", "open", "high", "low", "close", "volume",
                ]
            ].iloc[0]
            try:
                self._on_new_bar(symbol, timeframe, bar)
            except Exception:
                logger.exception("Error in on_new_bar callback %s %s", symbol, timeframe)
            bar_index += 1
            self._stop_event.wait(timeout=sleep_sec)
