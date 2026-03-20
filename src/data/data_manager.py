"""
DataManager — central hub for market data.

Responsibilities:
  - Connect to MT5 (Windows) or mock source (macOS/Linux dev)
  - Maintain a rolling OHLCV DataFrame per (symbol, timeframe)
  - Notify registered strategy callbacks when a new bar closes
  - Each (symbol, timeframe) pair runs in its own dedicated thread.
    With Python 3.13t/3.14t (no-GIL) these threads are truly parallel.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Callable

import pandas as pd

from ..utils.logger import get_logger
from .historical_loader import HistoricalLoader

logger = get_logger("data_manager")

# Callback signature: (symbol: str, timeframe: str, df: pd.DataFrame) -> None
BarCallback = Callable[[str, str, pd.DataFrame], None]

_TIMEFRAME_SECONDS = {
    "M1": 60, "M5": 300, "M15": 900, "M30": 1800,
    "H1": 3600, "H4": 14400, "D1": 86400,
}


class DataManager:
    """
    Thread-safe market data manager.

    Usage:
        dm = DataManager(config)
        dm.register_callback("XAUUSD", "H1", my_strategy.on_new_bar)
        dm.start()
        ...
        dm.stop()
    """

    def __init__(self, config: dict) -> None:
        self._config = config
        data_cfg = config.get("data", {})
        self._warmup_bars: int = data_cfg.get("warmup_bars", 200)
        self._poll_interval: float = data_cfg.get("poll_interval_seconds", 1.0)
        # When using mock (no MT5), optional shorter wait between bars for quick Telegram/tests.
        # Example: 0.05 → ~20 bars/sec. If unset, uses poll_interval_seconds.
        mp = data_cfg.get("mock_poll_interval_seconds")
        self._mock_poll_interval: float | None = float(mp) if mp is not None else None
        # INFO log every N poll iterations (0 = no INFO heartbeat; use DEBUG for each poll).
        self._poll_log_every_n: int = max(0, int(data_cfg.get("poll_log_every_n", 20)))
        self._fallback: str = data_cfg.get("fallback_source", "mock")
        self._hist_dir: str = data_cfg.get("historical_dir", "data/historical")
        # Mock + CSV replay: last N bars from data/historical, seed first M bars, stream the rest.
        self._mock_replay_enabled: bool = bool(data_cfg.get("mock_replay_from_historical", True))
        self._mock_replay_max_bars: int = max(1, int(data_cfg.get("mock_replay_max_bars", 2000)))
        self._mock_replay_seed_bars: int = max(1, int(data_cfg.get("mock_replay_seed_bars", 200)))

        # DataFrame store: key = (symbol, timeframe)
        self._data: dict[tuple[str, str], pd.DataFrame] = {}
        self._data_lock = threading.Lock()

        # Callbacks: key = (symbol, timeframe) → list of callables
        self._callbacks: dict[tuple[str, str], list[BarCallback]] = defaultdict(list)

        self._threads: list[threading.Thread] = []
        self._stop_event = threading.Event()
        self._connector = None
        self._historical_loader = HistoricalLoader(self._hist_dir)
        # Remaining OHLCV rows to append in mock replay mode (per stream thread).
        self._replay_remaining: dict[tuple[str, str], pd.DataFrame] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    def register_callback(
        self, symbol: str, timeframe: str, callback: BarCallback
    ) -> None:
        """Register a function to call on every new closed bar."""
        key = (symbol, timeframe)
        self._callbacks[key].append(callback)
        logger.debug("Registered callback for %s %s: %s", symbol, timeframe, callback.__qualname__)

    def get_data(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """Return current OHLCV DataFrame (thread-safe copy)."""
        with self._data_lock:
            df = self._data.get((symbol, timeframe), pd.DataFrame())
            return df.copy()

    def start(self) -> None:
        """Connect data source and start streaming threads."""
        logger.info("DataManager starting (source=%s)…", self._fallback)
        self._connector = self._build_connector()

        trading_pairs = self._config.get("trading_pairs", [])
        for pair in trading_pairs:
            symbol = pair["symbol"]
            for tf in pair.get("timeframes", []):
                self._init_data(symbol, tf)
                t = threading.Thread(
                    target=self._stream_loop,
                    args=(symbol, tf),
                    name=f"data-{symbol}-{tf}",
                    daemon=True,
                )
                t.start()
                self._threads.append(t)

        logger.info("DataManager started %d stream threads", len(self._threads))

    def stop(self) -> None:
        """Signal all threads to stop and wait for them."""
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=10)
        if self._connector and hasattr(self._connector, "disconnect"):
            self._connector.disconnect()
        logger.info("DataManager stopped")

    def load_historical(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """Load historical bars for backtesting (no streaming)."""
        return self._historical_loader.load(symbol, timeframe)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _build_connector(self):
        """Return MT5Connector or MockDataStreamer based on config/platform."""
        import platform as _platform

        mt5_cfg = self._config.get("mt5", {})
        on_windows = _platform.system() == "Windows"
        has_mt5_creds = bool(mt5_cfg.get("login") and mt5_cfg.get("password"))

        if on_windows and has_mt5_creds and self._fallback != "mock":
            try:
                from .mt5_connector import MT5Connector
                conn = MT5Connector(
                    login=int(mt5_cfg["login"]),
                    password=str(mt5_cfg["password"]),
                    server=mt5_cfg.get("server", ""),
                    timeout=mt5_cfg.get("timeout", 60_000),
                )
                if conn.connect():
                    logger.info("Using MT5 as data source")
                    return conn
                logger.warning("MT5 connection failed, falling back to mock")
            except Exception as exc:
                logger.warning("MT5 unavailable (%s), using mock source", exc)

        logger.info("Using MockDataSource (development mode)")
        from .mock_source import MockDataStreamer
        return None  # Mock data is generated inline in _stream_loop

    def _init_data(self, symbol: str, timeframe: str) -> None:
        """Seed the rolling DataFrame with warmup bars."""
        key = (symbol, timeframe)
        try:
            if self._connector and hasattr(self._connector, "get_ohlcv"):
                df = self._connector.get_ohlcv(symbol, timeframe, self._warmup_bars)
            elif self._connector is None and self._mock_replay_enabled:
                # Mock: replay real CSV — newest window, seed bars, then stream remainder.
                try:
                    raw = self._historical_loader.load(symbol, timeframe)
                    if "timestamp" not in raw.columns:
                        raise ValueError("historical data missing timestamp column")
                    raw = raw.sort_values("timestamp").reset_index(drop=True)
                    win = min(len(raw), self._mock_replay_max_bars)
                    chunk = raw.tail(win).reset_index(drop=True)
                    n = len(chunk)
                    seed_n = min(self._mock_replay_seed_bars, n)
                    df = chunk.iloc[:seed_n].copy()
                    rem = chunk.iloc[seed_n:].reset_index(drop=True)
                    self._replay_remaining[key] = rem
                    logger.info(
                        "Mock replay %s %s: window=%d (newest), seed=%d, queued=%d bars to stream",
                        symbol,
                        timeframe,
                        n,
                        len(df),
                        len(rem),
                    )
                except FileNotFoundError as fnf:
                    logger.warning(
                        "Mock replay: %s — falling back to synthetic warmup (%d bars)",
                        fnf,
                        self._warmup_bars,
                    )
                    from .mock_source import generate_ohlcv

                    df = generate_ohlcv(symbol, timeframe, n_bars=self._warmup_bars)
                except Exception as exc:
                    logger.warning(
                        "Mock replay failed (%s); synthetic warmup (%d bars)",
                        exc,
                        self._warmup_bars,
                    )
                    from .mock_source import generate_ohlcv

                    df = generate_ohlcv(symbol, timeframe, n_bars=self._warmup_bars)
            else:
                from .mock_source import generate_ohlcv
                df = generate_ohlcv(symbol, timeframe, n_bars=self._warmup_bars)
            with self._data_lock:
                self._data[key] = df
            logger.info(
                "Loaded %d warmup bars for %s %s", len(df), symbol, timeframe
            )
        except Exception as exc:
            logger.error("Failed to load warmup bars for %s %s: %s", symbol, timeframe, exc)
            with self._data_lock:
                self._data[key] = pd.DataFrame(
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                )

    def _stream_loop(self, symbol: str, timeframe: str) -> None:
        """
        Per-(symbol, timeframe) streaming thread.
        Polls every `_poll_interval` seconds, detects new bar closes,
        and fires registered callbacks.
        With no-GIL Python these loops run truly in parallel.
        """
        tf_sec = _TIMEFRAME_SECONDS.get(timeframe, 3600)
        last_bar_time: datetime | None = None
        bar_counter = 0
        is_mock = self._connector is None
        wait_sec = (
            self._mock_poll_interval
            if is_mock and self._mock_poll_interval is not None
            else self._poll_interval
        )

        replay_left = len(
            self._replay_remaining.get((symbol, timeframe), pd.DataFrame())
        )
        logger.info(
            "Stream loop %s %s: poll wait=%.3fs (mock=%s replay_queued=%d)",
            symbol,
            timeframe,
            wait_sec,
            is_mock,
            replay_left,
        )

        poll_num = 0
        while not self._stop_event.is_set():
            try:
                new_bar = self._fetch_latest_bar(symbol, timeframe, bar_counter)
                bar_counter += 1
                poll_num += 1
                appended = False
                if new_bar is not None:
                    bar_ts = new_bar["timestamp"]
                    if last_bar_time is None or bar_ts > last_bar_time:
                        self._append_bar(symbol, timeframe, new_bar)
                        last_bar_time = bar_ts
                        appended = True
                        df_snapshot = self.get_data(symbol, timeframe)
                        self._fire_callbacks(symbol, timeframe, df_snapshot)

                logger.debug(
                    "poll %s %s #%d fetched=%s appended=%s ts=%s",
                    symbol,
                    timeframe,
                    poll_num,
                    new_bar is not None,
                    appended,
                    new_bar["timestamp"] if new_bar is not None else None,
                )
                if (
                    self._poll_log_every_n > 0
                    and poll_num % self._poll_log_every_n == 0
                ):
                    logger.info(
                        "poll heartbeat %s %s #%d wait=%.3fs fetched=%s appended=%s",
                        symbol,
                        timeframe,
                        poll_num,
                        wait_sec,
                        new_bar is not None,
                        appended,
                    )
            except Exception:
                logger.exception("Error in stream loop %s %s", symbol, timeframe)

            self._stop_event.wait(timeout=max(0.0, wait_sec))

        logger.debug("Stream loop stopped: %s %s", symbol, timeframe)

    def _fetch_latest_bar(
        self, symbol: str, timeframe: str, bar_index: int
    ) -> pd.Series | None:
        """Get one new bar — from MT5, mock CSV replay, or synthetic mock."""
        if self._connector and hasattr(self._connector, "get_ohlcv"):
            df = self._connector.get_ohlcv(symbol, timeframe, 1)
            return df.iloc[-1] if not df.empty else None

        key = (symbol, timeframe)
        if key in self._replay_remaining:
            rem = self._replay_remaining[key]
            if rem is not None and len(rem) > 0:
                row = rem.iloc[0]
                self._replay_remaining[key] = rem.iloc[1:].reset_index(drop=True)
                if len(self._replay_remaining[key]) == 0:
                    logger.info(
                        "Mock replay finished for %s %s (all queued bars streamed)",
                        symbol,
                        timeframe,
                    )
                return row
            # Replay finished — no more bars until restart
            return None

        from .mock_source import generate_ohlcv

        # Synthetic mock: monotonic bar time so each poll produces a new closed bar.
        end_time = datetime.now(timezone.utc) + timedelta(microseconds=bar_index)
        df = generate_ohlcv(
            symbol, timeframe, n_bars=1, seed=bar_index, end_time=end_time
        )
        return df.iloc[0]

    def _append_bar(self, symbol: str, timeframe: str, bar: pd.Series) -> None:
        key = (symbol, timeframe)
        with self._data_lock:
            df = self._data.get(key, pd.DataFrame())
            new_row = pd.DataFrame([bar])
            df = pd.concat([df, new_row], ignore_index=True)
            # Keep rolling window to avoid unbounded memory growth
            df = df.tail(5000).reset_index(drop=True)
            self._data[key] = df

    def _fire_callbacks(
        self, symbol: str, timeframe: str, df: pd.DataFrame
    ) -> None:
        key = (symbol, timeframe)
        for cb in self._callbacks.get(key, []):
            try:
                cb(symbol, timeframe, df)
            except Exception:
                logger.exception(
                    "Callback error %s for %s %s", cb.__qualname__, symbol, timeframe
                )
