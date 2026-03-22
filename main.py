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

import signal
import sys
import threading
from collections import defaultdict
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait

from src.utils import get_logger, ConfigLoader, print_runtime_info, is_gil_enabled, get_optimal_workers
from src.utils.paper_exit import paper_bar_exit
from src.data import DataManager
from src.strategies import MACDCrossoverStrategy, RSI_EMA_Strategy, SonicRStrategy, SonicRFundStrategy, SonicRM15Strategy, SonicRM5Strategy
from src.risk import RiskManager
from src.notifier import TelegramNotifier

logger = get_logger("main", log_file="logs/trading.log")

_STRATEGY_REGISTRY = {
    "MACDCrossover": MACDCrossoverStrategy,
    "RSI_EMA": RSI_EMA_Strategy,
    "SonicR": SonicRStrategy,
    "SonicRFund": SonicRFundStrategy,
    "SonicRM15": SonicRM15Strategy,
    "SonicRM5": SonicRM5Strategy,
}


def build_strategies(config: dict) -> dict[tuple[str, str], list]:
    """Instantiate all configured strategies per (symbol, timeframe)."""
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
        """If paper position open on this pair, check last bar for TP/SL (same logic as backtest)."""
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
        h, l = float(row["high"]), float(row["low"])
        outcome = paper_bar_exit(st["is_buy"], h, l, st["sl"], st["tp"])
        if outcome is None:
            return

        with paper_lock:
            if paper_state is None:
                return
            if paper_state["symbol"] != symbol or paper_state["timeframe"] != timeframe:
                return
            paper_state = None

        ts_str = str(row.get("timestamp", "—"))
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
                symbol,
                timeframe,
                "stop" if stop_after_tp_hit else "continue",
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

    def _register_paper(complete) -> None:
        nonlocal paper_state
        if not paper_track_tp_sl:
            return
        with paper_lock:
            if paper_state is not None:
                return
            paper_state = {
                "symbol": complete.symbol,
                "timeframe": complete.timeframe,
                "is_buy": "BUY" in complete.action.upper(),
                "entry": float(complete.entry),
                "sl": float(complete.sl),
                "tp": float(complete.tp1),
                "strategy": complete.strategy_name,
            }
        logger_main.info(
            "paper_track: theo dõi lệnh giấy %s %s entry=%.5f sl=%.5f tp=%.5f",
            complete.symbol,
            complete.timeframe,
            complete.entry,
            complete.sl,
            complete.tp1,
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
            logger_main.info(
                "strategy data feed %s %s #%d rows=%d last_ts=%s → submitting %d strategy(s)",
                symbol,
                timeframe,
                nfeed,
                nrows,
                last_ts,
                n_strats,
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
                _register_paper(complete)
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

    notifier.send_text(
        "🤖 <b>Trading System Online</b>\n"
        f"Symbols: {[p['symbol'] for p in cfg['trading_pairs']]}\n"
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
    data_manager.stop()
    executor.shutdown(wait=False)
    notifier.stop()
    logger_main.info("Trading system stopped.")


if __name__ == "__main__":
    main()
