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

import math
import signal
import sys
import threading
from collections import defaultdict
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait

from src.utils import get_logger, ConfigLoader, print_runtime_info, is_gil_enabled, get_optimal_workers
from src.utils.ema_mt5 import ema_mt5
from src.utils.paper_exit import paper_bar_exit
from src.data import DataManager
from src.strategies import MACDCrossoverStrategy, RSI_EMA_Strategy, SonicRStrategy, SonicRFundStrategy, SonicRM15Strategy, SonicRM5Strategy
from src.risk import RiskManager
from src.notifier import TelegramNotifier
from src.execution import MT5OrderExecutor, OrderResult

logger = get_logger("main", log_file="logs/trading.log")


def _pip_size_of(symbol: str) -> float:
    """1 pip in price units — matches RiskManager and sonicr convention."""
    sym = symbol.upper()
    if sym in ("XAUUSD", "XAUUSDM", "XAGUSD"):
        return 0.10
    if "JPY" in sym:
        return 0.01
    return 0.0001


_STRATEGY_REGISTRY = {
    "MACDCrossover": MACDCrossoverStrategy,
    "RSI_EMA": RSI_EMA_Strategy,
    "SonicR": SonicRStrategy,
    "SonicRFund": SonicRFundStrategy,
    "SonicRM15": SonicRM15Strategy,
    "SonicRM5": SonicRM5Strategy,
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
                if allowed_syms and symbol not in allowed_syms:
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
    paper_state: dict | None = None

    data_cfg = cfg.raw.get("data", {}) or {}
    strat_log_every = max(0, int(data_cfg.get("strategy_eval_log_every_n", 20)))
    _strat_feed_count: dict[tuple[str, str], int] = defaultdict(int)

    def _check_paper_exit(symbol: str, timeframe: str, df) -> None:
        """
        Check paper position each new closed bar.

        State machine:
          PENDING → check fill (BUY: low <= limit, SELL: high >= limit)
                  → if filled: transition to OPEN, recalculate SL/TP from fill price
                  → if bars_remaining == 0: EXPIRED → clear state
          OPEN    → check TP/SL hit (same logic as backtest)
        """
        nonlocal paper_state
        if not paper_track_tp_sl:
            return
        with paper_lock:
            if paper_state is None:
                return
            if paper_state["symbol"] != symbol or paper_state["timeframe"] != timeframe:
                return
            st = dict(paper_state)

        if df is None or len(df) < 1:
            return
        row = df.iloc[-1]
        h = float(row["high"])
        l = float(row["low"])
        o = float(row.get("open", row["close"]))
        ts_str = str(row.get("timestamp", "—"))

        status = st.get("status", "OPEN")

        # ── PENDING: check limit fill or expiry ───────────────────────────────
        if status == "PENDING":
            lim      = float(st["limit_price"])
            is_buy   = st["is_buy"]
            sl_level = float(st.get("sl_level", 0.0))
            rr_ratio = float(st.get("rr_ratio", 2.0))
            pip_size = float(st.get("pip_size", 0.10))

            # Fill check — identical to BacktestEngine._find_limit_fill
            filled     = False
            fill_price = lim
            if is_buy:
                if o <= lim:          # gap down through limit → fill at open
                    filled, fill_price = True, o
                elif l <= lim:        # normal fill at our limit price
                    filled, fill_price = True, lim
            else:
                if o >= lim:          # gap up through limit → fill at open
                    filled, fill_price = True, o
                elif h >= lim:
                    filled, fill_price = True, lim

            if filled:
                # Recompute SL / TP from actual fill price (handles gap fills)
                if sl_level > 0:
                    sl_dist = abs(fill_price - sl_level)
                else:
                    sl_dist = abs(fill_price - st["sl"])   # fallback
                tp_dist  = sl_dist * rr_ratio
                sl_price = (fill_price - sl_dist) if is_buy else (fill_price + sl_dist)
                tp_price = (fill_price + tp_dist) if is_buy else (fill_price - tp_dist)

                with paper_lock:
                    if paper_state is None:
                        return
                    paper_state = {
                        "status":    "OPEN",
                        "symbol":    st["symbol"],
                        "timeframe": st["timeframe"],
                        "is_buy":    is_buy,
                        "entry":     fill_price,
                        "sl":        sl_price,
                        "tp":        tp_price,
                        "strategy":  st["strategy"],
                        "notes":     st.get("notes", ""),
                    }
                logger_main.info(
                    "paper_track: limit FILLED %s %s fill=%.5f sl=%.5f tp=%.5f",
                    symbol, timeframe, fill_price, sl_price, tp_price,
                )
                if notify_on_limit_fill:
                    direction = "📈 BUY" if is_buy else "📉 SELL"
                    notifier.send_text(
                        f"🎯 <b>Limit Filled (paper)</b>\n"
                        f"🔹 <b>{st['symbol']}</b> / {st['timeframe']}  {direction}\n"
                        f"💰 Fill <code>{fill_price:.5f}</code>\n"
                        f"🛑 SL <code>{sl_price:.5f}</code>  "
                        f"✅ TP <code>{tp_price:.5f}</code>\n"
                        f"🤖 {st['strategy']}\n"
                        f"🕐 <i>{ts_str}</i>"
                    )
                return

            # Not filled — decrement countdown
            new_remaining = int(st["bars_remaining"]) - 1
            if new_remaining <= 0:
                with paper_lock:
                    if paper_state is not None:
                        paper_state = None
                logger_main.info(
                    "paper_track: limit EXPIRED %s %s lim=%.5f after %d bars",
                    symbol, timeframe, lim, int(st["bars_remaining"]),
                )
                if notify_on_limit_expire:
                    direction = "📈 BUY" if is_buy else "📉 SELL"
                    notifier.send_text(
                        f"❌ <b>Limit Expired (paper)</b>\n"
                        f"🔹 <b>{st['symbol']}</b> / {st['timeframe']}  {direction}\n"
                        f"📍 Limit <code>{lim:.5f}</code> — không fill sau "
                        f"{int(st['bars_remaining'])} nến\n"
                        f"🤖 {st['strategy']}\n"
                        f"🕐 <i>{ts_str}</i>"
                    )
            else:
                with paper_lock:
                    if paper_state is not None:
                        paper_state = {**st, "bars_remaining": new_remaining}
            return

        # ── OPEN: check TP / SL hit ───────────────────────────────────────────
        outcome = paper_bar_exit(st["is_buy"], h, l, st["sl"], st["tp"])
        if outcome is None:
            return

        with paper_lock:
            if paper_state is None:
                return
            if paper_state["symbol"] != symbol or paper_state["timeframe"] != timeframe:
                return
            paper_state = None

        if outcome == "TP":
            if notify_on_tp_hit:
                notifier.send_text(
                    "✅ <b>TP hit (paper / test)</b>\n"
                    f"🔹 <b>{st['symbol']}</b> / {st['timeframe']}\n"
                    f"🤖 {st['strategy']}\n"
                    f"💰 Entry <code>{st['entry']:.5f}</code> →  "
                    f"<b>TP</b> <code>{st['tp']:.5f}</code>\n"
                    f"🛑 SL ref: <code>{st['sl']:.5f}</code>\n"
                    f"🕐 Bar close: <i>{ts_str}</i>"
                )
            logger_main.info(
                "paper_track: TP hit %s %s → %s",
                symbol, timeframe, "stop" if stop_after_tp_hit else "continue",
            )
            if stop_after_tp_hit:
                stop_event.set()
        else:  # SL
            if notify_on_sl_hit:
                notifier.send_text(
                    "🛑 <b>SL hit (paper / test)</b>\n"
                    f"🔹 <b>{st['symbol']}</b> / {st['timeframe']}\n"
                    f"🤖 {st['strategy']}\n"
                    f"💰 Entry <code>{st['entry']:.5f}</code> →  "
                    f"<b>SL</b> <code>{st['sl']:.5f}</code>\n"
                    f"🕐 Bar close: <i>{ts_str}</i>"
                )
            logger_main.info("paper_track: SL hit %s %s", symbol, timeframe)
            if stop_after_sl_hit:
                stop_event.set()

    def _register_paper(complete, sig=None) -> None:
        """
        Register a new paper position.

        Limit orders (action contains 'LIMIT'): stored as PENDING, waiting for fill.
        Market orders: stored as OPEN immediately (entry at signal close + any slippage
        already priced-in by RiskManager).
        """
        nonlocal paper_state
        if not paper_track_tp_sl:
            return
        with paper_lock:
            if paper_state is not None:
                return
            is_buy  = "BUY"   in complete.action.upper()
            is_limit = "LIMIT" in complete.action.upper()

            if is_limit:
                expiry   = int(sig.limit_expiry_bars) if (sig and sig.limit_expiry_bars > 0) else 10
                sl_level = float(sig.sl_level) if sig else 0.0
                paper_state = {
                    "status":        "PENDING",
                    "symbol":        complete.symbol,
                    "timeframe":     complete.timeframe,
                    "is_buy":        is_buy,
                    "limit_price":   float(complete.entry),   # entry == limit_price after risk_manager fix
                    "sl_level":      sl_level,
                    "sl":            float(complete.sl),
                    "tp":            float(complete.tp1),
                    "rr_ratio":      float(complete.rr_ratio),
                    "pip_size":      _pip_size_of(complete.symbol),
                    "bars_remaining": expiry,
                    "strategy":      complete.strategy_name,
                    "notes":         complete.notes or "",
                }
                logger_main.info(
                    "paper_track: PENDING %s %s %s limit=%.5f sl=%.5f tp=%.5f expiry=%d bars",
                    complete.symbol, complete.timeframe,
                    "BUY LIMIT" if is_buy else "SELL LIMIT",
                    complete.entry, complete.sl, complete.tp1, expiry,
                )
            else:
                paper_state = {
                    "status":    "OPEN",
                    "symbol":    complete.symbol,
                    "timeframe": complete.timeframe,
                    "is_buy":    is_buy,
                    "entry":     float(complete.entry),
                    "sl":        float(complete.sl),
                    "tp":        float(complete.tp1),
                    "strategy":  complete.strategy_name,
                    "notes":     complete.notes or "",
                }
                logger_main.info(
                    "paper_track: OPEN %s %s %s entry=%.5f sl=%.5f tp=%.5f",
                    complete.symbol, complete.timeframe,
                    "BUY" if is_buy else "SELL",
                    complete.entry, complete.sl, complete.tp1,
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
            complete = risk_manager.build_complete_signal(sig)
            if complete:
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

    # Register callbacks
    for (symbol, tf) in strategy_map:
        data_manager.register_callback(symbol, tf, on_new_bar)

    # ── Start ─────────────────────────────────────────────────────────────────
    notifier.start()
    data_manager.start()

    # MT5 Order Executor — phải khởi tạo SAU data_manager.start() để connector đã sẵn sàng
    mt5_executor = MT5OrderExecutor(cfg.raw, data_manager.get_connector())

    def _on_order_result(result: OrderResult) -> None:
        """Callback: log + Telegram sau mỗi lần gửi lệnh MT5."""
        if result.success:
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
            )
        else:
            logger_main.error("MT5 order FAILED: %s", result)
            notifier.send_text(
                f"❌ <b>Đặt lệnh MT5 thất bại</b>\n"
                f"{'📈' if 'BUY' in result.action else '📉'} "
                f"<b>{result.action}</b>  {result.symbol}\n"
                f"🚫 err={result.error_code}: {result.error_msg}\n"
                f"🤖 {result.strategy_name}"
            )

    mt5_executor.add_result_callback(_on_order_result)
    mt5_executor.start()

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
