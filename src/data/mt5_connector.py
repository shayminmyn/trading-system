"""
MetaTrader5 connector — wraps the MT5 Python library.

MT5 is Windows-only. On macOS/Linux this module falls back gracefully
and raises MT5UnavailableError. Use MockDataSource for development.

Thread-safety note:
  MT5 library functions are NOT thread-safe internally. All MT5 calls
  must be made from the SAME thread that called mt5.initialize().
  We enforce this via threading.local() and a dedicated thread per symbol.
"""

from __future__ import annotations

import platform
import threading
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from ..utils.logger import get_logger

logger = get_logger("mt5_connector")

_IS_WINDOWS = platform.system() == "Windows"
_mt5_local = threading.local()


class MT5UnavailableError(RuntimeError):
    """Raised when MetaTrader5 library is not available (non-Windows)."""


TIMEFRAME_MAP: dict[str, int] = {}  # populated after import


def _import_mt5():
    """Lazy import of MT5 — only works on Windows."""
    global TIMEFRAME_MAP
    if not _IS_WINDOWS:
        raise MT5UnavailableError(
            "MetaTrader5 Python library requires Windows. "
            "On macOS/Linux, set data.fallback_source=mock in config.yaml."
        )
    try:
        import MetaTrader5 as mt5

        TIMEFRAME_MAP = {
            "M1": mt5.TIMEFRAME_M1,
            "M5": mt5.TIMEFRAME_M5,
            "M15": mt5.TIMEFRAME_M15,
            "M30": mt5.TIMEFRAME_M30,
            "H1": mt5.TIMEFRAME_H1,
            "H4": mt5.TIMEFRAME_H4,
            "D1": mt5.TIMEFRAME_D1,
            "W1": mt5.TIMEFRAME_W1,
            "MN1": mt5.TIMEFRAME_MN1,
        }
        return mt5
    except ImportError as exc:
        raise MT5UnavailableError(
            "MetaTrader5 package not installed. "
            "Run: pip install MetaTrader5"
        ) from exc


class MT5Connector:
    """
    Manages a single MT5 connection.

    Important: All methods must be called from the thread that called connect().
    Use DataManager which handles threading correctly.
    """

    def __init__(self, login: int, password: str, server: str, timeout: int = 60_000) -> None:
        self._login = login
        self._password = password
        self._server = server
        self._timeout = timeout
        self._mt5 = None
        self._connected = False
        self._lock = threading.Lock()

    def connect(self) -> bool:
        mt5 = _import_mt5()
        self._mt5 = mt5

        if not mt5.initialize():
            logger.error("MT5 initialize() failed: %s", mt5.last_error())
            return False

        authorized = mt5.login(
            login=self._login,
            password=self._password,
            server=self._server,
            timeout=self._timeout,
        )
        if not authorized:
            logger.error("MT5 login failed: %s", mt5.last_error())
            mt5.shutdown()
            return False

        info = mt5.account_info()
        logger.info(
            "MT5 connected | Server: %s | Login: %d | Balance: %.2f %s",
            info.server,
            info.login,
            info.balance,
            info.currency,
        )
        self._connected = True
        return True

    def disconnect(self) -> None:
        if self._mt5 and self._connected:
            self._mt5.shutdown()
            self._connected = False
            logger.info("MT5 disconnected")

    def is_connected(self) -> bool:
        return self._connected

    def get_ohlcv(self, symbol: str, timeframe: str, n_bars: int) -> pd.DataFrame:
        """Fetch the last n_bars OHLCV candles for symbol/timeframe."""
        if not self._connected:
            raise RuntimeError("MT5 not connected")

        tf = TIMEFRAME_MAP.get(timeframe)
        if tf is None:
            raise ValueError(f"Unknown timeframe: {timeframe}")

        rates = self._mt5.copy_rates_from_pos(symbol, tf, 0, n_bars)
        if rates is None or len(rates) == 0:
            logger.warning("No rates returned for %s %s", symbol, timeframe)
            return pd.DataFrame()

        df = pd.DataFrame(rates)
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        df = df.rename(columns={
            "time": "timestamp", "open": "open", "high": "high",
            "low": "low", "close": "close", "tick_volume": "volume",
        })
        df = df[["timestamp", "open", "high", "low", "close", "volume"]].copy()
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df

    def get_latest_tick(self, symbol: str) -> dict | None:
        """Return the latest tick for symbol."""
        if not self._connected:
            return None
        tick = self._mt5.symbol_info_tick(symbol)
        if tick is None:
            return None
        return {
            "symbol": symbol,
            "bid": tick.bid,
            "ask": tick.ask,
            "last": tick.last,
            "time": datetime.fromtimestamp(tick.time, tz=timezone.utc),
        }

    def get_symbol_info(self, symbol: str) -> dict | None:
        """Return symbol metadata (pip value, contract size, etc.)."""
        if not self._connected:
            return None
        info = self._mt5.symbol_info(symbol)
        if info is None:
            return None
        return {
            "symbol": symbol,
            "digits": info.digits,
            "point": info.point,           # 1 pip in price units
            "trade_contract_size": info.trade_contract_size,
            "currency_profit": info.currency_profit,
            "description": info.description,
        }
