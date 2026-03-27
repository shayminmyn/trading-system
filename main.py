"""
Trading System — Realtime Entry Point

Connects all modules and runs the live signal pipeline:
  DataManager → Strategies → RiskManager → TelegramNotifier

Run:
  python main.py                          # standard Python
  PYTHON_GIL=0 python main.py            # Python 3.13t/3.14t — no GIL
  python3.14t main.py                     # free-threaded Python build

Each (symbol × timeframe × strategy) combination runs in its own thread.
With no-GIL, all threads execute truly in parallel across CPU cores.
"""

from __future__ import annotations

import html
import math
import signal
import sys
import threading
from collections import defaultdict
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from src.utils import get_logger, ConfigLoader, print_runtime_info, is_gil_enabled, get_optimal_workers
from src.utils.ema_mt5 import ema_mt5
from src.utils.paper_exit import paper_bar_exit
from src.data import DataManager
from src.strategies import  SonicRStrategy, SonicRM15Strategy, SonicRM5Strategy, TrendLine3Strategy
from src.risk import RiskManager
from src.notifier import TelegramNotifier
from src.execution import MT5OrderExecutor, OrderResult
from src.state import create_paper_store, PaperStateStore, create_daily_stats_store, DailyStatsStore

logger = get_logger("main", log_file="logs/trading.log")


# ── Daily trade outcome tracking ──────────────────────────────────────────────

@dataclass
class _TradeOutcome:
    """Aggregated trade outcomes for one (strategy, symbol, timeframe) bucket."""
    tp: int = 0
    sl: int = 0
    expired: int = 0

    @property
    def total(self) -> int:
        return self.tp + self.sl + self.expired

    @property
    def winrate(self) -> float:
        closed = self.tp + self.sl   # expired không tính vào winrate
        return (self.tp / closed * 100) if closed > 0 else 0.0


def _pip_size_of(symbol: str) -> float:
    """1 pip in price units — matches RiskManager and sonicr convention."""
    sym = symbol.upper()
    if sym in ("XAUUSD", "XAUUSDM", "XAGUSD"):
        return 0.10
    if "JPY" in sym:
        return 0.01
    return 0.0001


_STRATEGY_REGISTRY = {
    "SonicR": SonicRStrategy,
    "SonicRM15": SonicRM15Strategy,
    "SonicRM5": SonicRM5Strategy,
    "TrendLine3": TrendLine3Strategy,
}


def build_strategies(config: dict) -> dict[tuple[str, str], list]:
    """
    Instantiate all configured strategies per (symbol, timeframe).

    Per-strategy timeframe restriction: if a strategy's config contains an
    ``allowed_timeframes`` list, the strategy is only created for timeframes
    that appear in that list.  An empty / missing list means "all timeframes".

    Example config.yaml::

        strategies:
          SonicR:
            allowed_timeframes: ["H1", "H4"]
          SonicRM15:
            allowed_timeframes: ["M15"]
          SonicRM5:
            allowed_timeframes: ["M5"]
    """
    strategies: dict[tuple[str, str], list] = {}
    strategy_params = config.get("strategies", {})
    session_filters = config.get("session_filters", {})

    for pair in config.get("trading_pairs", []):
        symbol = pair["symbol"]
        for tf in pair.get("timeframes", []):
            key = (symbol, tf)
            strategies[key] = []
            for strat_name in pair.get("strategies", list(_STRATEGY_REGISTRY.keys())):
                cls = _STRATEGY_REGISTRY.get(strat_name)
                if cls is None:
                    logger.warning("Unknown strategy: %s — skipping", strat_name)
                    continue
                params = dict(strategy_params.get(strat_name, {}))

                # Respect per-strategy timeframe / symbol whitelists (pop so
                # they are not forwarded to the strategy constructor).
                allowed_tfs: list = params.pop("allowed_timeframes", [])
                if allowed_tfs and tf not in allowed_tfs:
                    logger.debug(
                        "Strategy %s skipped for %s/%s "
                        "(allowed_timeframes=%s)",
                        strat_name, symbol, tf, allowed_tfs,
                    )
                    continue

                allowed_syms: list = params.pop("allowed_symbols", [])
                if allowed_syms and symbol.upper() not in [s.upper() for s in allowed_syms]:
                    logger.debug(
                        "Strategy %s skipped for %s/%s "
                        "(allowed_symbols=%s)",
                        strat_name, symbol, tf, allowed_syms,
                    )
                    continue

                params["session_filters"] = session_filters
                strategies[key].append(cls(symbol=symbol, timeframe=tf, parameters=params))
                logger.info("Loaded strategy: %s for %s/%s", strat_name, symbol, tf)
    return strategies


def main() -> None:
    print_runtime_info()

    # ── Config ────────────────────────────────────────────────────────────────
    cfg = ConfigLoader.load("config.yaml")
    log_cfg = cfg.get("logging", {})
    logger_main = get_logger(
        "main",
        level=log_cfg.get("level", "INFO"),
        log_file=log_cfg.get("log_file", "logs/trading.log"),
    )

    workers = get_optimal_workers(
        cfg.get("concurrency", {}).get("strategy_workers", 4)
    )
    logger_main.info("Strategy workers: %d (no-GIL=%s)", workers, not is_gil_enabled())

    # ── Modules ───────────────────────────────────────────────────────────────
    risk_manager = RiskManager(cfg.raw)
    notifier = TelegramNotifier(cfg.raw)
    data_manager = DataManager(cfg.raw)

    # ── Strategies ────────────────────────────────────────────────────────────
    strategy_map = build_strategies(cfg.raw)

    # ThreadPoolExecutor for parallel strategy evaluation per new bar
    # With no-GIL this is truly CPU-parallel
    executor = ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="strategy-eval",
    )

    # Shared with signal handlers + optional "stop after first Telegram signal"
    stop_event = threading.Event()
    tg_cfg = cfg.raw.get("telegram", {}) or {}
    stop_after_first_signal: bool = bool(tg_cfg.get("stop_after_first_signal", False))
    paper_track_tp_sl: bool = bool(tg_cfg.get("paper_track_tp_sl", False))
    notify_on_tp_hit: bool = bool(tg_cfg.get("notify_on_tp_hit", True))
    notify_on_sl_hit: bool = bool(tg_cfg.get("notify_on_sl_hit", False))
    notify_on_limit_fill: bool = bool(tg_cfg.get("notify_on_limit_fill", True))
    notify_on_limit_expire: bool = bool(tg_cfg.get("notify_on_limit_expire", True))
    stop_after_tp_hit: bool = bool(tg_cfg.get("stop_after_tp_hit", False))
    stop_after_sl_hit: bool = bool(tg_cfg.get("stop_after_sl_hit", False))

    _first_sig_lock = threading.Lock()
    _first_sig_sent = False
    paper_lock = threading.Lock()

    # ── Signal dedup (safety net) ─────────────────────────────────────────────
    # Loại bỏ duplicate order_id trong vòng 60 giây để tránh gửi 2 lần do
    # callback bị fire đôi khi có race condition hoặc config lỗi.
    _seen_order_ids: dict[str, float] = {}   # order_id → time.monotonic()
    _seen_order_ids_lock = threading.Lock()
    _DEDUP_WINDOW_SEC = 60.0

    def _is_duplicate_order_id(oid: str) -> bool:
        import time
        now = time.monotonic()
        with _seen_order_ids_lock:
            # Dọn các entry cũ
            expired = [k for k, t in _seen_order_ids.items() if now - t > _DEDUP_WINDOW_SEC]
            for k in expired:
                del _seen_order_ids[k]
            if oid in _seen_order_ids:
                return True
            _seen_order_ids[oid] = now
            return False
    # Keyed by (symbol, timeframe) so each pair tracks independently.
    # Backend: RedisStateStore (if configured) or InMemoryStateStore (default).
    paper_store: PaperStateStore = create_paper_store(cfg.raw)

    # ── Daily stats ──────────────────────────────────────────────────────────
    # Persistent khi Redis enabled, fallback in-memory khi không có Redis.
    _daily_stats_store: DailyStatsStore = create_daily_stats_store(cfg.raw)

    def _record_outcome(strategy: str, symbol: str, timeframe: str, outcome: str) -> None:
        """Thread-safe; outcome = 'tp' | 'sl' | 'expired'."""
        _daily_stats_store.increment(strategy, symbol, timeframe, outcome)

    def _reset_daily_stats() -> None:
        _daily_stats_store.reset()

    data_cfg = cfg.raw.get("data", {}) or {}
    strat_log_every = max(0, int(data_cfg.get("strategy_eval_log_every_n", 20)))
    _strat_feed_count: dict[tuple[str, str], int] = defaultdict(int)

    def _tf_minutes(tf: str) -> int:
        """Trả về số phút của một nến theo timeframe."""
        tf = tf.upper()
        _map = {"M1": 1, "M5": 5, "M15": 15, "M30": 30,
                "H1": 60, "H4": 240, "D1": 1440, "W1": 10080}
        return _map.get(tf, 1)

    def _emit_open_tp_sl(
        symbol: str, timeframe: str, st: dict, h: float, l: float, ts_str: str
    ) -> None:
        """
        Kiểm tra và xử lý quản lý vị thế OPEN mỗi nến M1 (hoặc TF gốc):
          1. Break-even  — dời SL về entry khi lời ≥ be_at_r × SL distance
          2. Partial close — đóng partial_ratio khi lời ≥ partial_at_r × SL distance
          3. TP / SL exit — đóng toàn bộ vị thế
        """
        key    = (symbol, timeframe, st.get("order_id", ""))
        is_buy = bool(st["is_buy"])
        entry  = float(st["entry"])

        # ── Dùng original_sl để tính trigger prices (ổn định, không bị thay đổi sau BE) ──
        orig_sl    = float(st.get("original_sl", st["sl"]))
        sl_dist    = abs(entry - orig_sl)   # khoảng cách SL gốc
        pip_size   = float(st.get("pip_size", _pip_size_of(symbol)))
        ticket     = int(st.get("mt5_ticket", 0))
        oid        = st.get("order_id", "")
        oid_line   = f"\n🆔 <code>{oid}</code>" if oid else ""

        # ────────────────────────────────────────────────────────────────────────
        # 1. BREAK-EVEN CHECK
        # ────────────────────────────────────────────────────────────────────────
        be_at_r      = float(st.get("be_at_r", 0.0))
        be_triggered = bool(st.get("be_triggered", False))

        if be_at_r > 0 and not be_triggered and sl_dist > 0:
            be_trigger_price = (entry + be_at_r * sl_dist) if is_buy else (entry - be_at_r * sl_dist)
            be_hit = (is_buy and h >= be_trigger_price) or (not is_buy and l <= be_trigger_price)

            if be_hit:
                new_sl = entry   # SL = entry (break-even)
                with paper_lock:
                    current = paper_store.get(key)
                    if current is None:
                        return
                    updated = {**current, "sl": new_sl, "be_triggered": True}
                    paper_store.set(key, updated)
                    st = updated

                logger_main.info(
                    "paper_track: BE activated %s %s entry=%.5f new_sl=%.5f order_id=%s",
                    symbol, timeframe, entry, new_sl, oid,
                )
                # Notify BE
                direction = "📈 BUY" if is_buy else "📉 SELL"
                notifier.send_text(
                    f"🔁 <b>Break-even activated (paper)</b>\n"
                    f"🔹 <b>{symbol}</b> / {timeframe}  {direction}\n"
                    f"💰 Entry <code>{entry:.5f}</code>  "
                    f"📍 SL dời → <code>{new_sl:.5f}</code>\n"
                    f"🎯 Trigger <code>{be_trigger_price:.5f}</code> (R≥{be_at_r})\n"
                    f"🤖 {st.get('strategy', '')}"
                    f"{oid_line}\n"
                    f"🕐 <i>{ts_str}</i>"
                )
                # MT5: dời SL về entry
                if ticket > 0:
                    mt5_executor.modify_sl_async(ticket, symbol, new_sl, order_id=oid)

        # ────────────────────────────────────────────────────────────────────────
        # 2. PARTIAL CLOSE CHECK
        # ────────────────────────────────────────────────────────────────────────
        partial_at_r      = float(st.get("partial_at_r", 0.0))
        partial_triggered = bool(st.get("partial_triggered", False))

        if partial_at_r > 0 and not partial_triggered and sl_dist > 0:
            partial_trigger_price = (
                (entry + partial_at_r * sl_dist) if is_buy
                else (entry - partial_at_r * sl_dist)
            )
            partial_hit = (is_buy and h >= partial_trigger_price) or (not is_buy and l <= partial_trigger_price)

            if partial_hit:
                ratio         = float(st.get("partial_ratio", 0.5))
                full_volume   = float(st.get("volume", 0.0))
                close_vol     = round(full_volume * ratio, 2)
                remain_vol    = round(full_volume - close_vol, 2)
                trail_pips    = float(st.get("partial_trail_pips", 0.0))
                current_sl    = float(st["sl"])

                # Dời SL sau partial: lock-in trail_pips pips lời
                if trail_pips > 0 and pip_size > 0:
                    if is_buy:
                        # Dời SL lên tối thiểu entry + trail_pips (lock in pips)
                        locked_sl = entry + trail_pips * pip_size
                        new_sl_partial = max(current_sl, locked_sl)
                    else:
                        locked_sl = entry - trail_pips * pip_size
                        new_sl_partial = min(current_sl, locked_sl)
                else:
                    new_sl_partial = current_sl   # không thay đổi

                with paper_lock:
                    current = paper_store.get(key)
                    if current is None:
                        return
                    updated = {
                        **current,
                        "partial_triggered": True,
                        "volume":           remain_vol,
                        "sl":               new_sl_partial,
                    }
                    paper_store.set(key, updated)
                    st = updated

                logger_main.info(
                    "paper_track: partial close %s %s close=%.2flot remain=%.2flot "
                    "new_sl=%.5f order_id=%s",
                    symbol, timeframe, close_vol, remain_vol, new_sl_partial, oid,
                )
                # Notify partial close
                direction = "📈 BUY" if is_buy else "📉 SELL"
                sl_change = (
                    f" → <code>{new_sl_partial:.5f}</code>" if new_sl_partial != current_sl else ""
                )
                notifier.send_text(
                    f"✂️ <b>Partial close (paper)</b>\n"
                    f"🔹 <b>{symbol}</b> / {timeframe}  {direction}\n"
                    f"💰 Entry <code>{entry:.5f}</code>  "
                    f"🎯 Trigger <code>{partial_trigger_price:.5f}</code> (R≥{partial_at_r})\n"
                    f"📦 Đóng <code>{close_vol:.2f}</code>lot  còn <code>{remain_vol:.2f}</code>lot\n"
                    f"🛑 SL <code>{current_sl:.5f}</code>{sl_change}\n"
                    f"🤖 {st.get('strategy', '')}"
                    f"{oid_line}\n"
                    f"🕐 <i>{ts_str}</i>"
                )
                # MT5: đóng partial + cập nhật SL
                if ticket > 0:
                    mt5_executor.close_partial_async(ticket, symbol, close_vol, is_buy, oid)
                    if new_sl_partial != current_sl:
                        tp_price = float(st.get("tp", 0.0))
                        mt5_executor.modify_sl_async(ticket, symbol, new_sl_partial,
                                                     new_tp=tp_price, order_id=oid)

        # ────────────────────────────────────────────────────────────────────────
        # 3. TP / SL EXIT CHECK (dùng SL hiện tại — đã cập nhật qua BE/partial)
        # ────────────────────────────────────────────────────────────────────────
        # Re-fetch state in case it was updated above
        with paper_lock:
            current_st = paper_store.get(key)
        if current_st is None:
            return

        current_sl = float(current_st["sl"])
        current_tp = float(current_st["tp"])
        outcome = paper_bar_exit(is_buy, h, l, current_sl, current_tp)
        if outcome is None:
            return

        with paper_lock:
            if paper_store.get(key) is None:
                return
            paper_store.set(key, None)

        _record_outcome(st.get("strategy", ""), symbol, timeframe, outcome.lower())
        if outcome == "TP":
            if notify_on_tp_hit:
                notifier.send_text(
                    "✅ <b>TP hit (paper)</b>\n"
                    f"🔹 <b>{current_st['symbol']}</b> / {current_st['timeframe']}\n"
                    f"🤖 {current_st['strategy']}\n"
                    f"💰 Entry <code>{current_st['entry']:.5f}</code> →  "
                    f"<b>TP</b> <code>{current_tp:.5f}</code>\n"
                    f"🛑 SL ref: <code>{current_sl:.5f}</code>"
                    f"{oid_line}\n"
                    f"🕐 <i>{ts_str}</i>"
                )
            logger_main.info(
                "paper_track: TP hit %s %s order_id=%s → %s",
                symbol, timeframe, oid, "stop" if stop_after_tp_hit else "continue",
            )
            if stop_after_tp_hit:
                stop_event.set()
        else:  # SL
            if notify_on_sl_hit:
                notifier.send_text(
                    "🛑 <b>SL hit (paper)</b>\n"
                    f"🔹 <b>{current_st['symbol']}</b> / {current_st['timeframe']}\n"
                    f"🤖 {current_st['strategy']}\n"
                    f"💰 Entry <code>{current_st['entry']:.5f}</code> →  "
                    f"<b>SL</b> <code>{current_sl:.5f}</code>"
                    f"{oid_line}\n"
                    f"🕐 <i>{ts_str}</i>"
                )
            logger_main.info("paper_track: SL hit %s %s order_id=%s", symbol, timeframe, oid)
            if stop_after_sl_hit:
                stop_event.set()

    def _check_paper_exit_m1(symbol: str, df_m1) -> None:
        """
        Callback M1 — xử lý TẤT CẢ paper states của symbol này mỗi phút:
          - OPEN:    kiểm tra TP/SL trên nến M1 (nhanh hơn đợi nến TF gốc)
          - PENDING: kiểm tra fill M1 + đếm ngược minutes_remaining cho expiry
        """
        if not paper_track_tp_sl:
            return
        if df_m1 is None or len(df_m1) < 1:
            return
        row = df_m1.iloc[-1]
        h = float(row["high"])
        l = float(row["low"])
        o = float(row.get("open", row["close"]))
        ts_str = str(row.get("timestamp", "—"))

        with paper_lock:
            relevant = [(k, dict(v)) for k, v in paper_store.items()
                        if k[0] == symbol]

        for key, st in relevant:
            status = st.get("status", "OPEN")

            if status == "OPEN":
                _emit_open_tp_sl(key[0], key[1], st, h, l, ts_str)

            elif status == "PENDING":
                lim      = float(st["limit_price"])
                is_buy   = st["is_buy"]
                sl_level = float(st.get("sl_level", 0.0))
                rr_ratio = float(st.get("rr_ratio", 2.0))
                tp_level = float(st.get("tp", 0.0))

                # ── Bỏ qua nến M1 ngay sau khi đặt lệnh (nến tạo signal) ────────
                if st.get("skip_next_bar"):
                    with paper_lock:
                        if paper_store.get(key) is not None:
                            paper_store.set(key, {**st, "skip_next_bar": False})
                    continue

                # ── Huỷ nếu giá đã vượt TP mà chưa fill ─────────────────────────
                # BUY LIMIT : chờ giá xuống fill; nếu giá vọt LÊN qua TP → huỷ
                # SELL LIMIT: chờ giá lên fill;  nếu giá rơi XUỐNG qua TP → huỷ
                tp_invalidated = tp_level > 0 and (
                    (    is_buy and h >= tp_level and l > lim) or
                    (not is_buy and l <= tp_level and h < lim)
                )
                if tp_invalidated:
                    with paper_lock:
                        paper_store.set(key, None)
                    oid    = st.get("order_id", "")
                    ticket = int(st.get("mt5_ticket", 0))
                    logger_main.info(
                        "paper_track(M1): PENDING cancelled — price passed TP "
                        "%s %s lim=%.5f tp=%.5f order_id=%s",
                        key[0], key[1], lim, tp_level, oid,
                    )
                    if ticket > 0:
                        mt5_executor.cancel_order_async(ticket, oid)
                    _record_outcome(st.get("strategy", ""), st["symbol"], st["timeframe"], "expired")
                    if notify_on_limit_expire:
                        direction = "📈 BUY" if is_buy else "📉 SELL"
                        oid_line = f"\n🆔 <code>{oid}</code>" if oid else ""
                        ticket_line = (
                            f"\n🎫 ticket=<code>{ticket}</code> — đang huỷ trên MT5..."
                            if ticket > 0 else ""
                        )
                        notifier.send_text(
                            f"🚫 <b>Limit Invalidated (paper)</b>\n"
                            f"🔹 <b>{st['symbol']}</b> / {st['timeframe']}  {direction}\n"
                            f"📍 Limit <code>{lim:.5f}</code> — giá đã vượt TP "
                            f"<code>{tp_level:.5f}</code> mà chưa fill\n"
                            f"🤖 {st['strategy']}"
                            f"{oid_line}"
                            f"{ticket_line}\n"
                            f"🕐 <i>{ts_str}</i>"
                        )
                    continue

                # ── Fill check on M1 bar ───────────────────────────────────────
                filled, fill_price = False, lim
                if is_buy:
                    if o <= lim:
                        filled, fill_price = True, o
                    elif l <= lim:
                        filled, fill_price = True, lim
                else:
                    if o >= lim:
                        filled, fill_price = True, o
                    elif h >= lim:
                        filled, fill_price = True, lim

                if filled:
                    # Giữ nguyên SL/TP đã tính sẵn từ limit_price (risk manager),
                    # chỉ cập nhật entry = fill_price thực tế.
                    # Tránh tính lại TP từ fill_price (khác limit_price → TP lệch).
                    sl_price = float(st["sl"])
                    tp_price = float(st["tp"])
                    with paper_lock:
                        if paper_store.get(key) is None:
                            continue
                        paper_store.set(key, {
                            "status":            "OPEN",
                            "symbol":            st["symbol"],
                            "timeframe":         st["timeframe"],
                            "is_buy":            is_buy,
                            "entry":             fill_price,
                            "sl":                sl_price,
                            "tp":                tp_price,
                            "original_sl":       st.get("original_sl", sl_price),
                            "volume":            st.get("volume", 0.0),
                            "strategy":          st["strategy"],
                            "notes":             st.get("notes", ""),
                            "order_id":          st.get("order_id", ""),
                            "mt5_ticket":        st.get("mt5_ticket", 0),
                            "be_at_r":           st.get("be_at_r", 0.0),
                            "be_triggered":      False,   # reset on fill
                            "partial_at_r":      st.get("partial_at_r", 0.0),
                            "partial_triggered": False,
                            "partial_ratio":     st.get("partial_ratio", 0.5),
                            "partial_trail_pips": st.get("partial_trail_pips", 5.0),
                        })
                    logger_main.info(
                        "paper_track(M1): limit FILLED %s %s fill=%.5f sl=%.5f tp=%.5f order_id=%s",
                        key[0], key[1], fill_price, sl_price, tp_price, st.get("order_id", ""),
                    )
                    if notify_on_limit_fill:
                        direction = "📈 BUY" if is_buy else "📉 SELL"
                        oid = st.get("order_id", "")
                        oid_line = f"\n🆔 <code>{oid}</code>" if oid else ""
                        notifier.send_text(
                            f"🎯 <b>Limit Filled (paper)</b>\n"
                            f"🔹 <b>{st['symbol']}</b> / {st['timeframe']}  {direction}\n"
                            f"💰 Fill <code>{fill_price:.5f}</code>\n"
                            f"🛑 SL <code>{sl_price:.5f}</code>  "
                            f"✅ TP <code>{tp_price:.5f}</code>\n"
                            f"🤖 {st['strategy']}"
                            f"{oid_line}\n"
                            f"🕐 <i>{ts_str}</i>"
                        )
                    continue

                # Decrement minute countdown
                mins_left = int(st.get("minutes_remaining", 0)) - 1
                if mins_left <= 0:
                    with paper_lock:
                        paper_store.set(key, None)
                    oid        = st.get("order_id", "")
                    ticket     = int(st.get("mt5_ticket", 0))
                    total_mins = int(st.get("minutes_total", int(st.get("bars_remaining", 0))))
                    logger_main.info(
                        "paper_track(M1): limit EXPIRED %s %s lim=%.5f after %dm "
                        "order_id=%s ticket=%d",
                        key[0], key[1], lim, total_mins, oid, ticket,
                    )
                    # Huỷ pending order trên MT5 nếu đã đặt
                    if ticket > 0:
                        mt5_executor.cancel_order_async(ticket, oid)
                        logger_main.info(
                            "paper_track(M1): enqueued MT5 cancel ticket=%d order_id=%s",
                            ticket, oid,
                        )
                    _record_outcome(st.get("strategy", ""), st["symbol"], st["timeframe"], "expired")
                    if notify_on_limit_expire:
                        direction = "📈 BUY" if is_buy else "📉 SELL"
                        oid_line = f"\n🆔 <code>{oid}</code>" if oid else ""
                        ticket_line = (
                            f"\n🎫 ticket=<code>{ticket}</code> — đang huỷ trên MT5..."
                            if ticket > 0 else ""
                        )
                        notifier.send_text(
                            f"⌛ <b>Limit Expired</b>\n"
                            f"🔹 <b>{st['symbol']}</b> / {st['timeframe']}  {direction}\n"
                            f"📍 Limit <code>{lim:.5f}</code> — không fill sau "
                            f"{total_mins} phút\n"
                            f"🤖 {st['strategy']}"
                            f"{oid_line}"
                            f"{ticket_line}\n"
                            f"🕐 <i>{ts_str}</i>"
                        )
                else:
                    with paper_lock:
                        if paper_store.get(key) is not None:
                            paper_store.set(key, {**st, "minutes_remaining": mins_left})

    def _check_paper_exit(symbol: str, timeframe: str, df) -> None:
        """
        Check paper position mỗi khi nến TF chiến lược đóng.

        State machine (per order slot):
          PENDING → check fill (BUY: low <= limit, SELL: high >= limit)
                  → if filled: transition to OPEN
                  → if bars_remaining == 0: EXPIRED → clear state
          OPEN    → TP/SL check được xử lý bởi _check_paper_exit_m1 (M1 callback).
                    Ở đây vẫn kiểm tra phòng hờ nếu M1 stream không khả dụng.
        """
        if not paper_track_tp_sl:
            return
        with paper_lock:
            # Key format: (symbol, timeframe, order_id) — collect all slots for this pair
            slots = [(k, dict(v)) for k, v in paper_store.items()
                     if k[0] == symbol and k[1] == timeframe]
        if not slots:
            return
        # Use the first slot for backward-compatible fallback logic below
        key, st = slots[0]

        if df is None or len(df) < 1:
            return
        row = df.iloc[-1]
        h = float(row["high"])
        l = float(row["low"])
        o = float(row.get("open", row["close"]))
        ts_str = str(row.get("timestamp", "—"))

        status = st.get("status", "OPEN")

        # PENDING fill/expiry và OPEN TP/SL đều được xử lý bởi _check_paper_exit_m1.
        # Hàm này chỉ là fallback khi M1 stream chưa có (môi trường mock/dev).

    def _register_paper(complete, sig=None) -> None:
        """
        Register a new paper position.

        Key = (symbol, timeframe, order_id) — supports multiple concurrent
        positions on the same symbol+timeframe pair.
        Limit orders: stored as PENDING, waiting for fill.
        Market orders: stored as OPEN immediately.
        """
        if not paper_track_tp_sl:
            return
        key = (complete.symbol, complete.timeframe, complete.order_id)
        with paper_lock:
            if paper_store.get(key) is not None:
                return   # exact duplicate order_id — skip
            is_buy   = "BUY"   in complete.action.upper()
            is_limit = "LIMIT" in complete.action.upper()

            if is_limit:
                expiry_bars = int(sig.limit_expiry_bars) if (sig and sig.limit_expiry_bars > 0) else 10
                sl_level    = float(sig.sl_level) if sig else 0.0
                tf_mins     = _tf_minutes(complete.timeframe)
                total_mins  = expiry_bars * tf_mins
                paper_store.set(key, {
                    "status":            "PENDING",
                    "symbol":            complete.symbol,
                    "timeframe":         complete.timeframe,
                    "is_buy":            is_buy,
                    "limit_price":       float(complete.entry),
                    "sl_level":          sl_level,
                    "sl":                float(complete.sl),
                    "tp":                float(complete.tp1),
                    "original_sl":       float(complete.sl),   # anchor cho BE/partial calc
                    "volume":            float(complete.volume),
                    "rr_ratio":          float(complete.rr_ratio),
                    "pip_size":          _pip_size_of(complete.symbol),
                    "bars_remaining":    expiry_bars,
                    "minutes_remaining": total_mins,
                    "minutes_total":     total_mins,
                    "strategy":          complete.strategy_name,
                    "notes":             complete.notes or "",
                    "order_id":          complete.order_id,
                    "mt5_ticket":        0,
                    "skip_next_bar":     True,  # bỏ qua nến M1 đầu tiên sau signal
                    "be_at_r":           float(complete.breakeven_at_r),
                    "be_triggered":      False,
                    "partial_at_r":      float(complete.partial_close_at_r),
                    "partial_triggered": False,
                    "partial_ratio":     float(complete.partial_close_ratio),
                    "partial_trail_pips": float(complete.partial_trail_pips),
                })
                logger_main.info(
                    "paper_track: PENDING %s %s %s limit=%.5f sl=%.5f tp=%.5f expiry=%d bars (%dm)",
                    complete.symbol, complete.timeframe,
                    "BUY LIMIT" if is_buy else "SELL LIMIT",
                    complete.entry, complete.sl, complete.tp1, expiry_bars, total_mins,
                )
            else:
                paper_store.set(key, {
                    "status":            "OPEN",
                    "symbol":            complete.symbol,
                    "timeframe":         complete.timeframe,
                    "is_buy":            is_buy,
                    "entry":             float(complete.entry),
                    "sl":                float(complete.sl),
                    "tp":                float(complete.tp1),
                    "original_sl":       float(complete.sl),   # anchor cho BE/partial calc
                    "volume":            float(complete.volume),
                    "strategy":          complete.strategy_name,
                    "notes":             complete.notes or "",
                    "order_id":          complete.order_id,
                    "mt5_ticket":        0,
                    "be_at_r":           float(complete.breakeven_at_r),
                    "be_triggered":      False,
                    "partial_at_r":      float(complete.partial_close_at_r),
                    "partial_triggered": False,
                    "partial_ratio":     float(complete.partial_close_ratio),
                    "partial_trail_pips": float(complete.partial_trail_pips),
                })
                logger_main.info(
                    "paper_track: OPEN %s %s %s entry=%.5f sl=%.5f tp=%.5f be_at_r=%.1f partial_at_r=%.1f",
                    complete.symbol, complete.timeframe,
                    "BUY" if is_buy else "SELL",
                    complete.entry, complete.sl, complete.tp1,
                    complete.breakeven_at_r, complete.partial_close_at_r,
                )

    def on_new_bar(symbol: str, timeframe: str, df) -> None:
        """
        Callback from DataManager — fires for every new closed bar.
        Submits all strategies for this (symbol, timeframe) to thread pool.
        """
        _check_paper_exit(symbol, timeframe, df)

        key = (symbol, timeframe)
        nrows = len(df) if df is not None else 0
        last_ts = None
        if df is not None and nrows > 0 and "timestamp" in df.columns:
            last_ts = df.iloc[-1]["timestamp"]

        _strat_feed_count[key] += 1
        nfeed = _strat_feed_count[key]
        n_strats = len(strategy_map.get(key, []))

        logger_main.debug(
            "strategy callback %s %s feed#%d df_rows=%d last_ts=%s strategies=%d",
            symbol,
            timeframe,
            nfeed,
            nrows,
            last_ts,
            n_strats,
        )
        if strat_log_every > 0 and nfeed % strat_log_every == 0:
            close_val = ema34_val = ema89_val = None
            if df is not None and nrows >= 89 and "close" in df.columns:
                closes = df["close"].astype(float)
                close_val = float(closes.iloc[-1])
                ema34_s = ema_mt5(closes, 34)
                ema89_s = ema_mt5(closes, 89)
                e34 = float(ema34_s.iloc[-1])
                e89 = float(ema89_s.iloc[-1])
                ema34_val = None if math.isnan(e34) else e34
                ema89_val = None if math.isnan(e89) else e89
            elif df is not None and nrows > 0 and "close" in df.columns:
                close_val = float(df["close"].iloc[-1])
            logger_main.info(
                "strategy data feed %s %s #%d rows=%d last_ts=%s → %d strategy(s) | "
                "close=%s ema34=%s ema89=%s",
                symbol,
                timeframe,
                nfeed,
                nrows,
                last_ts,
                n_strats,
                f"{close_val:.5f}" if close_val is not None else "—",
                f"{ema34_val:.5f}" if ema34_val is not None else "—(< 89 bars)",
                f"{ema89_val:.5f}" if ema89_val is not None else "—(< 89 bars)",
            )

        futures = [
            executor.submit(_evaluate_strategy, strategy, symbol, timeframe, df)
            for strategy in strategy_map.get(key, [])
        ]
        # Bắt buộc chờ strategy xong trong cùng nến: nếu không, nến sau có thể chạy
        # _check_paper_exit trước khi _register_paper kịp chạy → không bao giờ TP hit.
        if futures:
            wait(futures, return_when=ALL_COMPLETED)

    def _evaluate_strategy(strategy, symbol: str, timeframe: str, df) -> None:
        nonlocal _first_sig_sent
        nrows = len(df) if df is not None else 0
        logger_main.debug(
            "strategy exec start %s %s/%s rows=%d thread=%s",
            strategy.name,
            symbol,
            timeframe,
            nrows,
            threading.current_thread().name,
        )
        try:
            sig = strategy.on_new_bar(symbol, timeframe, df)
        except Exception:
            logger_main.exception(
                "strategy exec failed %s %s/%s", strategy.name, symbol, timeframe
            )
            return

        actionable = bool(sig and sig.is_actionable())
        logger_main.debug(
            "strategy exec done %s %s/%s action=%s actionable=%s",
            strategy.name,
            symbol,
            timeframe,
            getattr(sig, "action", None) if sig else None,
            actionable,
        )
        if sig and sig.is_actionable():
            # Per-strategy risk override: strategies.<Name>.risk_per_trade_percent
            strat_cfg = cfg.raw.get("strategies", {}).get(strategy.name, {}) or {}
            strat_risk = strat_cfg.get("risk_per_trade_percent")
            risk_override = float(strat_risk) if strat_risk is not None else None
            complete = risk_manager.build_complete_signal(sig, risk_pct_override=risk_override)
            if complete:
                # Safety-net: bỏ qua nếu cùng order_id đã xử lý trong 60s
                if _is_duplicate_order_id(complete.order_id):
                    logger_main.warning(
                        "DUPLICATE signal ignored: order_id=%s strategy=%s %s/%s",
                        complete.order_id, strategy.name, symbol, timeframe,
                    )
                    return
                notifier.send_signal(complete)
                _register_paper(complete, sig)
                mt5_executor.submit_signal(complete)
                # Khi đang chờ TP paper, không dừng ngay sau tín hiệu đầu
                if stop_after_first_signal and not paper_track_tp_sl:
                    with _first_sig_lock:
                        if not _first_sig_sent:
                            _first_sig_sent = True
                            logger_main.info(
                                "stop_after_first_signal: đã gửi tín hiệu đầu tiên → dừng hệ thống"
                            )
                            stop_event.set()

    # Register callbacks — strategy processing
    for (symbol, tf) in strategy_map:
        data_manager.register_callback(symbol, tf, on_new_bar)

    # Register M1 callback per symbol for fast TP/SL detection on OPEN positions
    if paper_track_tp_sl:
        _m1_symbols: set[str] = {sym for sym, _ in strategy_map}
        for _sym in _m1_symbols:
            _sym_ref = _sym  # capture for closure

            def _make_m1_cb(s: str):
                def _m1_cb(_symbol: str, _tf: str, df_m1) -> None:
                    _check_paper_exit_m1(s, df_m1)
                return _m1_cb

            data_manager.register_callback(_sym, "M1", _make_m1_cb(_sym))
            logger_main.info("paper_track: registered M1 TP/SL callback for %s", _sym)

    # ── Daily summary ─────────────────────────────────────────────────────────
    daily_cfg = cfg.raw.get("daily_summary", {}) or {}
    _daily_summary_enabled: bool = bool(daily_cfg.get("enabled", True))
    _daily_summary_hour_local: int = int(daily_cfg.get("hour_local", 0))    # 0 = midnight (UTC+7)
    _daily_tz_offset_hours: int = int(daily_cfg.get("tz_offset_hours", 7))  # UTC+7

    def _build_daily_summary_text(snapshot: dict) -> str:
        """Build Telegram HTML message từ snapshot _daily_stats."""
        if not snapshot:
            return "📊 <b>Tổng kết ngày</b>\n\n<i>Không có lệnh nào hôm nay.</i>"

        # group by (symbol, timeframe) → list of (strategy, outcome)
        grouped: dict[tuple[str, str], list[tuple[str, _TradeOutcome]]] = defaultdict(list)
        for (strat, sym, tf), outcome in sorted(snapshot.items()):
            grouped[(sym, tf)].append((strat, outcome))

        now_local = datetime.now(timezone.utc) + timedelta(hours=_daily_tz_offset_hours)
        date_str = now_local.strftime("%d/%m/%Y")

        lines = [f"📊 <b>Tổng kết ngày {date_str}</b> (UTC+{_daily_tz_offset_hours})\n"]
        total_tp = total_sl = total_exp = 0

        for (sym, tf), entries in grouped.items():
            lines.append(f"🔹 <b>{html.escape(sym)}</b> / <code>{html.escape(tf)}</code>")
            for strat, o in entries:
                wr = f"{o.winrate:.0f}%" if (o.tp + o.sl) > 0 else "—"
                parts = []
                if o.tp:
                    parts.append(f"✅ TP {o.tp}")
                if o.sl:
                    parts.append(f"🛑 SL {o.sl}")
                if o.expired:
                    parts.append(f"⌛ Exp {o.expired}")
                total_line = f"({o.total} lệnh)" if o.total else "(0 lệnh)"
                lines.append(
                    f"  🤖 {html.escape(strat)}  "
                    + "  ".join(parts)
                    + f"  WR {wr}  {total_line}"
                )
                total_tp += o.tp
                total_sl += o.sl
                total_exp += o.expired
            lines.append("")

        grand_total = total_tp + total_sl + total_exp
        grand_wr = f"{total_tp / (total_tp + total_sl) * 100:.0f}%" if (total_tp + total_sl) > 0 else "—"
        lines.append(
            f"<b>Tổng:</b>  ✅TP {total_tp}  🛑SL {total_sl}  ⌛Exp {total_exp}"
            f"  —  {grand_total} lệnh  WR {grand_wr}"
        )
        return "\n".join(lines)

    def _next_summary_utc() -> datetime:
        """Thời điểm UTC của lần gửi tổng kết tiếp theo (00:00 giờ địa phương = 17:00 UTC nếu UTC+7)."""
        offset = timedelta(hours=_daily_tz_offset_hours)
        now_utc = datetime.now(timezone.utc)
        # Reset hour tính theo giờ địa phương
        target_utc_hour = (_daily_summary_hour_local - _daily_tz_offset_hours) % 24
        next_run = now_utc.replace(
            hour=target_utc_hour, minute=0, second=0, microsecond=0
        )
        if next_run <= now_utc:
            next_run += timedelta(days=1)
        return next_run

    def _daily_summary_loop() -> None:
        while not stop_event.is_set():
            next_run = _next_summary_utc()
            wait_sec = (next_run - datetime.now(timezone.utc)).total_seconds()
            logger_main.info(
                "daily_summary: next send at %s UTC (%.0fs from now)",
                next_run.strftime("%Y-%m-%d %H:%M UTC"), wait_sec,
            )
            # Chờ đến giờ, wake sớm nếu stop_event set
            stop_event.wait(timeout=max(0, wait_sec))
            if stop_event.is_set():
                break
            # Đủ giờ → snapshot + reset + send
            raw_snapshot = _daily_stats_store.get_all()
            snapshot = {
                k: _TradeOutcome(v["tp"], v["sl"], v["expired"])
                for k, v in raw_snapshot.items()
            }
            _reset_daily_stats()
            text = _build_daily_summary_text(snapshot)
            logger_main.info("daily_summary: sending\n%s", text)
            notifier.send_text(text)

    # ── Start ─────────────────────────────────────────────────────────────────
    notifier.start()
    data_manager.start()

    # MT5 Order Executor — phải khởi tạo SAU data_manager.start() để connector đã sẵn sàng
    mt5_executor = MT5OrderExecutor(cfg.raw, data_manager.get_connector())

    def _on_order_result(result: OrderResult) -> None:
        """Callback: log + Telegram sau mỗi lần gửi lệnh MT5."""
        # Liên kết MT5 ticket vào paper_store để có thể huỷ về sau
        if result.success and result.order_type == "LIMIT" and result.order_id:
            with paper_lock:
                for k, slot in paper_store.items():
                    if slot.get("order_id") == result.order_id:
                        updated = {**slot, "mt5_ticket": result.ticket}
                        paper_store.set(k, updated)
                        logger_main.info(
                            "paper_track: MT5 ticket=%d linked order_id=%s",
                            result.ticket, result.order_id,
                        )
                        break

        oid_line = f"\n🆔 <code>{result.order_id}</code>" if result.order_id else ""

        if result.success:
            if result.order_type == "CANCEL":
                already_msg = " (already closed)" in (result.error_msg or "")
                logger_main.info(
                    "MT5 cancel OK: ticket=%d order_id=%s%s",
                    result.ticket, result.order_id,
                    " — order was already gone" if already_msg else "",
                )
                # Gửi xác nhận huỷ thành công nếu order thực sự bị huỷ
                if not already_msg:
                    notifier.send_text(
                        f"🗑 <b>Lệnh chờ đã huỷ (MT5)</b>\n"
                        f"🎫 ticket=<code>{result.ticket}</code>\n"
                        f"🆔 <code>{result.order_id}</code>"
                    )
                return
            logger_main.info("MT5 order placed: %s", result)
            notifier.send_text(
                f"{'✅' if result.order_type == 'MARKET' else '⏳'} "
                f"<b>{'Lệnh đặt thành công' if result.order_type == 'MARKET' else 'Lệnh chờ đặt xong'}"
                f" (MT5)</b>\n"
                f"{'📈' if 'BUY' in result.action else '📉'} "
                f"<b>{result.action}</b>  {result.symbol}\n"
                f"💰 Price <code>{result.price}</code>  "
                f"🛑 SL <code>{result.sl}</code>  "
                f"✅ TP <code>{result.tp}</code>\n"
                f"⚖️ {result.volume:.2f} lot  🎫 ticket=<code>{result.ticket}</code>\n"
                f"🤖 {result.strategy_name}"
                f"{oid_line}"
            )
        else:
            if result.order_type == "CANCEL":
                logger_main.warning(
                    "MT5 cancel FAILED ticket=%d order_id=%s: %s",
                    result.ticket, result.order_id, result.error_msg,
                )
                notifier.send_text(
                    f"⚠️ <b>Không thể huỷ lệnh chờ MT5</b>\n"
                    f"🎫 ticket=<code>{result.ticket}</code>\n"
                    f"🚫 {html.escape(result.error_msg or '')}\n"
                    f"<i>Vui lòng kiểm tra và huỷ thủ công trong MT5</i>"
                )
                return
            logger_main.error("MT5 order FAILED: %s", result)
            extra_hint = ""
            if result.error_code == 10027:
                extra_hint = "\n⚠️ <b>AutoTrading đang TẮT</b> — bật nút AutoTrading trên toolbar MT5"
            notifier.send_text(
                f"❌ <b>Đặt lệnh MT5 thất bại</b>\n"
                f"{'📈' if 'BUY' in result.action else '📉'} "
                f"<b>{result.action}</b>  {result.symbol}\n"
                f"🚫 err={result.error_code}: {result.error_msg}"
                f"{extra_hint}\n"
                f"🤖 {result.strategy_name}"
                f"{oid_line}"
            )

    mt5_executor.add_result_callback(_on_order_result)
    mt5_executor.start()

    if _daily_summary_enabled:
        _summary_thread = threading.Thread(
            target=_daily_summary_loop,
            name="daily-summary",
            daemon=True,
        )
        _summary_thread.start()
        logger_main.info(
            "daily_summary: enabled, fires at %02d:00 UTC+%d every day",
            _daily_summary_hour_local, _daily_tz_offset_hours,
        )

    notifier.send_text(
        "🤖 <b>Trading System Online</b>\n"
        f"Symbols: {[p['symbol'] for p in cfg['trading_pairs']]}\n"
        f"MT5 execution: {'✅ ON' if mt5_executor.is_enabled else '⏸ OFF (paper only)'}\n"
        f"GIL disabled: {not is_gil_enabled()}"
    )
    _flags = []
    if stop_after_first_signal and not paper_track_tp_sl:
        _flags.append("stop_after_first_signal")
    if paper_track_tp_sl:
        _flags.append(
            f"paper_track TP(notify={notify_on_tp_hit},stop={stop_after_tp_hit}) "
            f"SL(notify={notify_on_sl_hit},stop={stop_after_sl_hit})"
        )
    if mt5_executor.is_enabled:
        _flags.append(f"mt5_execution(magic={cfg.raw.get('execution', {}).get('magic_number', 20260101)})")
    logger_main.info(
        "Trading system running. Press Ctrl+C to stop.%s",
        (" [" + ", ".join(_flags) + "]") if _flags else "",
    )

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    def _shutdown(signum, frame):
        logger_main.info("Shutdown signal received (%s)", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    stop_event.wait()

    logger_main.info("Stopping all modules…")
    mt5_executor.stop()
    data_manager.stop()
    executor.shutdown(wait=False)
    notifier.stop()
    logger_main.info("Trading system stopped.")


if __name__ == "__main__":
    main()
