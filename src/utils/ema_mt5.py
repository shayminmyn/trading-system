"""
EMA aligned with MetaTrader 5 `iMA(..., MODE_EMA)` and typical chart platforms.

Differences vs `pandas.Series.ewm(span=period).mean()` / `ta.EMAIndicator`:
  - MT5 seeds the first EMA with **SMA(period)** of the first `period` closes
    (bar index `period - 1`, 0-based).
  - Subsequent bars: EMA[i] = Close[i] * k + EMA[i-1] * (1 - k),
    with k = 2 / (period + 1).

The `ta` / naive `ewm` often initializes from the first close (or a shorter
warm-up), so levels diverge from MT5 especially on the left side of the series.

References: MQL5 MA_METHOD MODE_EMA; TradingView `ema()` documentation.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def ema_mt5(series: pd.Series | np.ndarray, period: int) -> pd.Series:
    """
    Exponential moving average on `series` (price column: close, high, low, …).

    Parameters
    ----------
    series : pd.Series or 1-D array
    period : int >= 1

    Returns
    -------
    pd.Series (same index as input when Series) with NaN for indices 0..period-2
    when period > 1.
    """
    if period < 1:
        raise ValueError("period must be >= 1")

    idx = getattr(series, "index", None)
    arr = np.asarray(series, dtype=float)
    n = len(arr)
    out = np.full(n, np.nan, dtype=float)

    if n == 0:
        return pd.Series(out, index=idx)

    if period == 1:
        out[:] = arr
        return pd.Series(out, index=idx)

    if n < period:
        return pd.Series(out, index=idx)

    k = 2.0 / (period + 1.0)
    one_m_k = 1.0 - k

    out[period - 1] = float(np.nanmean(arr[:period]))
    for i in range(period, n):
        out[i] = arr[i] * k + out[i - 1] * one_m_k

    return pd.Series(out, index=idx)
