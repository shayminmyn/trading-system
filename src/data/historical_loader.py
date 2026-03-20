"""
Historical data loader.

Supports:
  - CSV files downloaded from HistData.com or Dukascopy
  - Automatic resampling between timeframes
  - Caching in memory to avoid re-reading large files
"""

from __future__ import annotations

import threading
from pathlib import Path

import pandas as pd

from ..utils.logger import get_logger

logger = get_logger("historical_loader")

_RESAMPLE_RULES = {
    "M1": "1min", "M5": "5min", "M15": "15min", "M30": "30min",
    "H1": "1h", "H4": "4h", "D1": "1D",
}

# HistData.com CSV column order (no header)
_HISTDATA_COLS = ["timestamp", "open", "high", "low", "close", "volume"]
_HISTDATA_DTYPES = {"open": float, "high": float, "low": float, "close": float, "volume": float}


class HistoricalLoader:
    """
    Thread-safe historical CSV loader with in-memory cache.
    Multiple threads (e.g. parallel strategy backtest workers) can call
    load() concurrently without re-reading the file.
    """

    def __init__(self, data_dir: str = "data/historical") -> None:
        self._dir = Path(data_dir)
        self._cache: dict[str, pd.DataFrame] = {}
        self._lock = threading.Lock()

    def load(
        self,
        symbol: str,
        timeframe: str = "H1",
        csv_path: str | None = None,
    ) -> pd.DataFrame:
        """
        Load OHLCV data for symbol/timeframe.

        Priority:
          1. Exact-match CSV for (symbol, timeframe)
          2. M1 base CSV resampled to `timeframe`
          3. Raise FileNotFoundError
        """
        cache_key = f"{symbol}_{timeframe}"
        with self._lock:
            if cache_key in self._cache:
                return self._cache[cache_key].copy()

        df = self._load_uncached(symbol, timeframe, csv_path)
        with self._lock:
            self._cache[cache_key] = df
        return df.copy()

    def _load_uncached(
        self, symbol: str, timeframe: str, csv_path: str | None
    ) -> pd.DataFrame:
        # Explicit path override
        if csv_path:
            return self._read_csv(Path(csv_path))

        # Look for exact timeframe file
        for candidate in [
            self._dir / f"{symbol}_{timeframe}.csv",
            self._dir / f"{symbol}_{timeframe.lower()}.csv",
            self._dir / symbol / f"{timeframe}.csv",
        ]:
            if candidate.exists():
                return self._read_csv(candidate)

        # Fall back: load M1 and resample
        for m1_candidate in [
            self._dir / f"{symbol}_M1.csv",
            self._dir / f"{symbol}_m1.csv",
        ]:
            if m1_candidate.exists():
                logger.info(
                    "No %s %s file found; resampling from M1: %s",
                    symbol, timeframe, m1_candidate,
                )
                df_m1 = self._read_csv(m1_candidate)
                return self._resample(df_m1, timeframe)

        raise FileNotFoundError(
            f"No historical CSV found for {symbol} {timeframe} in {self._dir}. "
            "Download from https://www.histdata.com/ and place in data/historical/."
        )

    def _read_csv(self, path: Path) -> pd.DataFrame:
        """Auto-detect CSV format (HistData, Dukascopy, generic) and parse."""
        logger.debug("Reading CSV: %s", path)
        raw = pd.read_csv(path, header=None, nrows=2)

        # Try to detect header
        first_val = str(raw.iloc[0, 0]).strip().upper()
        has_header = first_val in ("DATE", "TIMESTAMP", "TIME", "DATETIME", "OPEN")

        df = pd.read_csv(
            path,
            header=0 if has_header else None,
            parse_dates=[0],
            dtype_backend="numpy_nullable",
        )
        df.columns = [c.strip().lower() for c in df.columns]

        # Normalise timestamp column name
        for col_name in ("date", "datetime", "time", "timestamp"):
            if col_name in df.columns:
                df = df.rename(columns={col_name: "timestamp"})
                break

        # HistData format: date + time in separate columns
        if "timestamp" not in df.columns and len(df.columns) >= 6:
            df.columns = _HISTDATA_COLS
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

        required = ["open", "high", "low", "close"]
        for col in required:
            if col not in df.columns:
                raise ValueError(f"CSV missing required column '{col}': {path}")

        df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
        df = df.sort_values("timestamp").reset_index(drop=True)

        if "volume" not in df.columns:
            df["volume"] = 0

        return df[["timestamp", "open", "high", "low", "close", "volume"]].astype(
            {"open": float, "high": float, "low": float, "close": float, "volume": float}
        )

    def _resample(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        rule = _RESAMPLE_RULES.get(timeframe)
        if rule is None:
            raise ValueError(f"Cannot resample to unknown timeframe: {timeframe}")

        df = df.set_index("timestamp")
        resampled = df.resample(rule).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }).dropna()
        resampled = resampled.reset_index()
        logger.info("Resampled %d M1 bars → %d %s bars", len(df), len(resampled), timeframe)
        return resampled

    def clear_cache(self) -> None:
        with self._lock:
            self._cache.clear()
