"""
DataManager — central hub for market data.

Responsibilities:
  - Connect to MT5 (Windows) or mock source (macOS/Linux dev)
  - Maintain a rolling OHLCV DataFrame per (symbol, timeframe)
  - Notify registered strategy callbacks when a new bar closes
  - Each (symbol, timeframe) pair runs in its own dedicated thread.
    With Python 3.13t/3.14t (no-GIL) these threads are truly parallel.

MT5 live (``data.fallback_source != mock``):
  - Mỗi ``poll_interval_seconds`` (vd. 1s) gọi API **2 nến** mới nhất của đúng
    ``timeframe`` (M5/H1/…) — **không** gom tick theo giây; nến do broker/MT5 định nghĩa.
  - Trong 2 nến: nến cũ hơn = đã **đóng** (OHLC chốt), nến mới hơn = đang chạy.
  - Khi có nến đóng mới (so timestamp open) → cập nhật buffer rồi chạy strategy.
  - Buffer có giới hạn ``buffer_max_bars``; phần cũ có thể append ra CSV (``buffer_spill_*``) để tránh OOM.
"""

from __future__ import annotations

import re
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
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

        # Rolling buffer: giữ tối đa N nến trong RAM; phần vượt ghi ra đĩa (append CSV) nếu bật spill.
        self._buffer_max_bars: int = max(1, int(data_cfg.get("buffer_max_bars", 5000)))
        self._buffer_spill_enabled: bool = bool(data_cfg.get("buffer_spill_enabled", True))
        spill_dir = data_cfg.get("buffer_spill_dir", "data/live_buffer")
        self._buffer_spill_dir: Path = Path(str(spill_dir))
        self._spill_write_lock = threading.Lock()
        # Đếm số nến đã append vào RAM per (symbol, timeframe) — dùng để log mỗi 20 nến.
        self._buffer_bar_count: dict[tuple[str, str], int] = defaultdict(int)
        self._bar_log_every: int = max(1, int(data_cfg.get("bar_log_every_n", 20)))

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
        # MT5: timestamp của nến đã đóng mới nhất đã xử lý (tránh gọi strategy trùng).
        self._mt5_last_closed_ts: dict[tuple[str, str], pd.Timestamp | None] = {}

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

    def _log_warmup_tail(self, symbol: str, timeframe: str, df: pd.DataFrame, n: int = 5) -> None:
        """Log N nến cuối sau khi warmup — hiện thị OHLC để xác nhận dữ liệu đúng."""
        if df.empty:
            logger.info("Warmup tail %s %s: (empty)", symbol, timeframe)
            return
        tail = df.tail(n)
        lines = [f"Warmup tail {symbol} {timeframe} — last {min(n, len(df))}/{len(df)} bars:"]
        for _, row in tail.iterrows():
            ts = row.get("timestamp", "?")
            o = float(row.get("open", float("nan")))
            h = float(row.get("high", float("nan")))
            l = float(row.get("low", float("nan")))
            c = float(row.get("close", float("nan")))
            lines.append(f"  {ts}  O={o:.5g}  H={h:.5g}  L={l:.5g}  C={c:.5g}")
        logger.info("\n".join(lines))

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
            if self._connector and hasattr(self._connector, "get_ohlcv"):
                self._log_warmup_tail(symbol, timeframe, df)
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
        key = (symbol, timeframe)
        tf_sec = _TIMEFRAME_SECONDS.get(timeframe, 3600)
        last_bar_time: datetime | None = None
        bar_counter = 0
        is_mock = self._connector is None
        is_mt5 = self._connector is not None and hasattr(
            self._connector, "get_ohlcv"
        )
        wait_sec = (
            self._mock_poll_interval
            if is_mock and self._mock_poll_interval is not None
            else self._poll_interval
        )

        # MT5: poll ~mỗi giây, mỗi lần lấy 2 nến từ API; chỉ gọi strategy khi có nến *đóng* mới.
        if is_mt5:
            self._stream_loop_mt5(symbol, timeframe, key, wait_sec, tf_sec)
            return

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

    def _mt5_init_closed_anchor(self, key: tuple[str, str]) -> None:
        """
        Neo thời điểm nến đã đóng mới nhất sau warmup để lần poll đầu không bắn strategy trùng.
        Warmup từ MT5: hàng cuối thường là nến đang chạy → nến đóng gần nhất = iloc[-2].
        """
        if key in self._mt5_last_closed_ts:
            return
        with self._data_lock:
            df0 = self._data.get(key, pd.DataFrame())
        if len(df0) >= 2:
            self._mt5_last_closed_ts[key] = pd.Timestamp(df0.iloc[-2]["timestamp"])
        elif len(df0) == 1:
            self._mt5_last_closed_ts[key] = pd.Timestamp(df0.iloc[-1]["timestamp"])
        else:
            self._mt5_last_closed_ts[key] = None

    def _mt5_poll_new_closed_bar(
        self, symbol: str, timeframe: str, key: tuple[str, str]
    ) -> pd.Series | None:
        """
        Mỗi lần poll: copy_rates 2 nến — nến cũ hơn trong 2 nến mới nhất = nến đã đóng (OHLC chốt).
        Chỉ trả về Series khi có nến đóng *mới* so với _mt5_last_closed_ts.
        """
        df2 = self._connector.get_ohlcv(symbol, timeframe, 2)
        if df2 is None or df2.empty:
            return None
        # sort ascending trong get_ohlcv: iloc[-2] = nến đã đóng, iloc[-1] = nến đang hình thành
        if len(df2) >= 2:
            completed = df2.iloc[-2]
        else:
            completed = df2.iloc[-1]
        ct = pd.Timestamp(completed["timestamp"])
        prev = self._mt5_last_closed_ts.get(key)

        if prev is None:
            self._mt5_last_closed_ts[key] = ct
            logger.info(
                "MT5 %s %s: neo nến đóng baseline ts=%s (chưa gọi strategy)",
                symbol,
                timeframe,
                ct,
            )
            return None

        if ct > prev:
            self._mt5_last_closed_ts[key] = ct
            return completed
        return None

    def _mt5_merge_closed_bar(self, key: tuple[str, str], completed: pd.Series) -> None:
        """Gộp nến đóng vào buffer: cùng timestamp → thay hàng cuối (chốt OHLC); mới hơn → append."""
        ct = pd.Timestamp(completed["timestamp"])
        to_spill: pd.DataFrame | None = None
        new_count: int = 0
        buf_len: int = 0
        with self._data_lock:
            df = self._data.get(key, pd.DataFrame())
            if df.empty:
                self._data[key] = pd.DataFrame([completed])
                self._buffer_bar_count[key] += 1
                new_count = self._buffer_bar_count[key]
                buf_len = 1
            else:
                last_ts = pd.Timestamp(df.iloc[-1]["timestamp"])
                if last_ts == ct:
                    df = pd.concat(
                        [df.iloc[:-1], pd.DataFrame([completed])], ignore_index=True
                    )
                elif last_ts < ct:
                    df = pd.concat([df, pd.DataFrame([completed])], ignore_index=True)
                    self._buffer_bar_count[key] += 1
                    new_count = self._buffer_bar_count[key]
                else:
                    logger.warning(
                        "MT5 merge: bỏ qua — completed ts %s <= last row %s",
                        ct,
                        last_ts,
                    )
                    return
                df, to_spill = self._trim_buffer_spill(key, df)
                self._data[key] = df
                buf_len = len(df)

        if new_count:
            self._maybe_log_bar_appended(key, new_count, completed, buf_len, to_spill)
        if to_spill is not None:
            self._append_buffer_to_spill_file(key, to_spill)

    def _trim_buffer_spill(
        self, key: tuple[str, str], df: pd.DataFrame
    ) -> tuple[pd.DataFrame, "pd.DataFrame | None"]:
        """
        Giữ tối đa ``_buffer_max_bars`` hàng trong RAM.
        Trả về ``(trimmed_df, rows_to_spill)`` — gọi trong _data_lock;
        caller phải ghi spill ra đĩa *ngoài* lock để không chặn reader.
        """
        max_b = self._buffer_max_bars
        if len(df) <= max_b:
            return df, None
        n_drop = len(df) - max_b
        head = df.iloc[:n_drop].copy()
        rest = df.iloc[n_drop:].reset_index(drop=True)
        return rest, (head if self._buffer_spill_enabled else None)

    def _spill_csv_path(self, key: tuple[str, str]) -> Path:
        sym = re.sub(r"[^\w.\-]+", "_", str(key[0]))
        tf = re.sub(r"[^\w.\-]+", "_", str(key[1]))
        return self._buffer_spill_dir / f"{sym}_{tf}_buffer.csv"

    def _append_buffer_to_spill_file(
        self, key: tuple[str, str], rows: pd.DataFrame
    ) -> None:
        """
        Append OHLCV rows ra file CSV.
        Gọi *ngoài* _data_lock để tránh chặn reader khi I/O chậm.
        write_header được kiểm tra bên trong _spill_write_lock — tránh race.
        """
        path = self._spill_csv_path(key)
        try:
            self._buffer_spill_dir.mkdir(parents=True, exist_ok=True)
            with self._spill_write_lock:
                write_header = not path.exists()
                rows.to_csv(path, mode="a", header=write_header, index=False)
            logger.debug(
                "Buffer spill %s %s: +%d rows → %s (RAM cap=%d)",
                key[0],
                key[1],
                len(rows),
                path,
                self._buffer_max_bars,
            )
        except OSError as exc:
            logger.error(
                "Buffer spill failed (%s): %s — data kept in memory only",
                path,
                exc,
            )

    def _stream_loop_mt5(
        self,
        symbol: str,
        timeframe: str,
        key: tuple[str, str],
        wait_sec: float,
        tf_sec: int,
    ) -> None:
        """
        Luồng MT5: poll mỗi `wait_sec` (mặc định 1s), mỗi lần gọi API lấy 2 nến;
        khi nến đã đóng mới (theo open time) → cập nhật DataFrame + callback strategy.
        """
        self._mt5_init_closed_anchor(key)
        bar_counter = 0
        poll_num = 0
        logger.info(
            "Stream loop MT5 %s %s: poll=%.3fs (~%.0fs/nến %s) — chờ nến *đóng* rồi mới tính strategy",
            symbol,
            timeframe,
            wait_sec,
            tf_sec,
            timeframe,
        )
        while not self._stop_event.is_set():
            try:
                new_closed = self._mt5_poll_new_closed_bar(symbol, timeframe, key)
                bar_counter += 1
                poll_num += 1
                appended = new_closed is not None
                if new_closed is not None:
                    self._mt5_merge_closed_bar(key, new_closed)
                    df_snapshot = self.get_data(symbol, timeframe)
                    self._fire_callbacks(symbol, timeframe, df_snapshot)

                logger.debug(
                    "MT5 poll %s %s #%d new_closed=%s ts=%s",
                    symbol,
                    timeframe,
                    poll_num,
                    appended,
                    new_closed["timestamp"] if new_closed is not None else None,
                )
                if (
                    self._poll_log_every_n > 0
                    and poll_num % self._poll_log_every_n == 0
                ):
                    logger.info(
                        "MT5 poll heartbeat %s %s #%d wait=%.3fs new_closed=%s",
                        symbol,
                        timeframe,
                        poll_num,
                        wait_sec,
                        appended,
                    )
            except Exception:
                logger.exception("Error in MT5 stream loop %s %s", symbol, timeframe)

            self._stop_event.wait(timeout=max(0.0, wait_sec))

        logger.debug("MT5 stream loop stopped: %s %s", symbol, timeframe)

    def _fetch_latest_bar(
        self, symbol: str, timeframe: str, bar_index: int
    ) -> pd.Series | None:
        """Get one new bar — from MT5, mock CSV replay, or synthetic mock."""
        if self._connector and hasattr(self._connector, "get_ohlcv"):
            # MT5 không dùng nhánh này — luồng thật nằm ở _stream_loop_mt5
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

    def _maybe_log_bar_appended(
        self,
        key: tuple[str, str],
        count: int,
        bar: pd.Series,
        buf_len: int,
        to_spill: "pd.DataFrame | None",
    ) -> None:
        """Log INFO mỗi bar_log_every_n nến mới, bắt đầu từ nến đầu tiên (count=1)."""
        every = self._bar_log_every
        if count != 1 and (count - 1) % every != 0:
            return
        ts = bar.get("timestamp", "?")
        o = float(bar.get("open", float("nan")))
        h = float(bar.get("high", float("nan")))
        l = float(bar.get("low", float("nan")))
        c = float(bar.get("close", float("nan")))
        spill_info = f" spill+{len(to_spill)}" if to_spill is not None else ""
        logger.info(
            "Bar #%d %s %s  ts=%s  O=%.5g H=%.5g L=%.5g C=%.5g  ram=%d/%d%s",
            count,
            key[0],
            key[1],
            ts,
            o, h, l, c,
            buf_len,
            self._buffer_max_bars,
            spill_info,
        )

    def _append_bar(self, symbol: str, timeframe: str, bar: pd.Series) -> None:
        key = (symbol, timeframe)
        to_spill: pd.DataFrame | None = None
        new_count: int = 0
        with self._data_lock:
            df = self._data.get(key, pd.DataFrame())
            df = pd.concat([df, pd.DataFrame([bar])], ignore_index=True)
            df, to_spill = self._trim_buffer_spill(key, df)
            self._data[key] = df
            self._buffer_bar_count[key] += 1
            new_count = self._buffer_bar_count[key]

        self._maybe_log_bar_appended(key, new_count, bar, len(df), to_spill)
        if to_spill is not None:
            self._append_buffer_to_spill_file(key, to_spill)

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
