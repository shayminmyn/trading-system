"""
Historical data loader.

Supports:
  - Our standard format: timestamp,open,high,low,close,volume  (ISO8601 UTC)
  - MetaTrader 4/5 export: DATE,TIME,OPEN,HIGH,LOW,CLOSE,[TICKVOL],[VOL],[SPREAD]
    (date and time as separate columns, dates in YYYY.MM.DD or YYYY-MM-DD)
  - HistData.com / Dukascopy: timestamp,open,high,low,close,volume
  - Automatic resampling from M1 base file when exact TF file is missing
  - Thread-safe in-memory cache
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
        if csv_path:
            return self._read_csv(Path(csv_path))

        for candidate in [
            self._dir / f"{symbol}_{timeframe}.csv",
            self._dir / f"{symbol}_{timeframe.lower()}.csv",
            self._dir / symbol / f"{timeframe}.csv",
        ]:
            if candidate.exists():
                return self._read_csv(candidate)

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
            "Run scripts/import_mt_csv.py or scripts/download_historical.py."
        )

    # ── format detection ─────────────────────────────────────────────────────

    @staticmethod
    def _sniff(path: Path) -> dict:
        """
        Read the first two non-empty lines and return sniff info:
          ncols_header  – number of comma-separated tokens in line 0
          ncols_data    – number of comma-separated tokens in line 1
          has_header    – True when line 0 looks like a header row
          mt_date_sep   – True when data date uses '.' separator (MT4 style)
        """
        lines: list[str] = []
        with path.open(encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                line = raw.strip()
                if line:
                    lines.append(line)
                if len(lines) == 2:
                    break

        if len(lines) < 2:
            return {"ncols_header": 0, "ncols_data": 0,
                    "has_header": False, "mt_date_sep": False}

        h_tokens = lines[0].split(",")
        d_tokens = lines[1].split(",")
        first_h  = h_tokens[0].strip().lstrip("<").rstrip(">").upper()
        has_hdr  = first_h in (
            "DATE", "TIMESTAMP", "TIME", "DATETIME",
            "OPEN", "TICKER", "SYM", "SYMBOL",
        )
        # MT4 uses dots in dates: 2022.04.08
        mt_sep = "." in d_tokens[0] and d_tokens[0].replace(".", "").isdigit()
        return {
            "ncols_header": len(h_tokens),
            "ncols_data":   len(d_tokens),
            "has_header":   has_hdr,
            "mt_date_sep":  mt_sep,
        }

    def _read_csv(self, path: Path) -> pd.DataFrame:
        """
        Auto-detect and parse CSV into a clean OHLCV DataFrame with UTC timestamps.

        Handled formats
        ---------------
        1. Standard  : timestamp,open,high,low,close,volume  (ISO8601 with/without tz)
        2. MT4/MT5   : [<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<TICKVOL>,<SPREAD>]
                       date can be YYYY.MM.DD or YYYY-MM-DD; time HH:MM or HH:MM:SS
                       header row is optional and may use <ANGLE> brackets
        3. HistData  : no header, 6 cols, YYYYMMDD HHMMSS in col-0
        """
        logger.debug("Reading CSV: %s", path)
        sniff = self._sniff(path)
        n_hdr  = sniff["ncols_header"]
        n_data = sniff["ncols_data"]

        # ── MetaTrader format: date + time in two separate columns ────────────
        # MT4/MT5 exports always have DATE and TIME as distinct columns.
        # We recognise this when:
        #   (a) data has ≥7 columns (date, time, o, h, l, c, [vol, ...])
        #   (b) header col-count ≠ data col-count  (mismatched because header
        #       was our 6-col standard header but data is MT4 8-col)
        is_mt_format = (
            n_data >= 7
            or (sniff["has_header"] and n_hdr != n_data and n_data >= 6)
            or sniff["mt_date_sep"]
        )

        if is_mt_format:
            return self._read_mt_csv(path, sniff)

        # ── Standard / Dukascopy / HistData ──────────────────────────────────
        return self._read_standard_csv(path, sniff)

    def _read_mt_csv(self, path: Path, sniff: dict) -> pd.DataFrame:
        """Parse MetaTrader 4/5 CSV export."""
        n_data = sniff["ncols_data"]

        # Assign positional column names; ignore extra trailing columns
        # MT4: date, time, open, high, low, close, tickvol[, vol, spread]
        pos_names = ["date", "time", "open", "high", "low", "close",
                     "tickvol", "vol", "spread", "extra"]

        df = pd.read_csv(
            path,
            header=0 if sniff["has_header"] else None,
            names=pos_names[:n_data],
            usecols=list(range(min(n_data, 7))),   # keep date..tickvol
            dtype=str,
        )
        df.columns = pos_names[:min(n_data, 7)]

        # Combine date + time into a single timestamp string
        date_col = df["date"].str.strip().str.replace(".", "-", regex=False)
        time_col = df["time"].str.strip()
        ts_str = date_col + " " + time_col

        df["timestamp"] = pd.to_datetime(ts_str, format="mixed", utc=True,
                                         errors="coerce")

        # Volume: prefer tickvol; fall back to 0
        vol_col = "tickvol" if "tickvol" in df.columns else None
        df["volume"] = pd.to_numeric(df[vol_col], errors="coerce").fillna(0) \
            if vol_col else 0

        for col in ("open", "high", "low", "close"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
        df = df.sort_values("timestamp").reset_index(drop=True)
        logger.info("Loaded MT4/MT5 CSV %s: %d bars", path.name, len(df))
        return df[["timestamp", "open", "high", "low", "close", "volume"]].astype(
            {"open": float, "high": float, "low": float,
             "close": float, "volume": float}
        )

    def _read_standard_csv(self, path: Path, sniff: dict) -> pd.DataFrame:
        """Parse standard/Dukascopy/HistData CSV."""
        df = pd.read_csv(
            path,
            header=0 if sniff["has_header"] else None,
            dtype_backend="numpy_nullable",
        )
        df.columns = [str(c).strip().lower() for c in df.columns]

        # Normalise timestamp column name
        for col_name in ("date", "datetime", "time", "timestamp"):
            if col_name in df.columns:
                df = df.rename(columns={col_name: "timestamp"})
                break

        if "timestamp" not in df.columns:
            # No header: assume HistData 6-col layout
            df.columns = ["timestamp", "open", "high", "low", "close", "volume",
                          *[f"_x{i}" for i in range(max(0, len(df.columns) - 6))]]

        df["timestamp"] = pd.to_datetime(
            df["timestamp"], format="ISO8601", utc=True, errors="coerce"
        )

        required = ["open", "high", "low", "close"]
        for col in required:
            if col not in df.columns:
                raise ValueError(f"CSV missing required column '{col}': {path}")

        df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
        df = df.sort_values("timestamp").reset_index(drop=True)

        if "volume" not in df.columns:
            df["volume"] = 0

        return df[["timestamp", "open", "high", "low", "close", "volume"]].astype(
            {"open": float, "high": float, "low": float,
             "close": float, "volume": float}
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
