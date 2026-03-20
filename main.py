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
from concurrent.futures import ThreadPoolExecutor

from src.utils import get_logger, ConfigLoader, print_runtime_info, is_gil_enabled, get_optimal_workers
from src.data import DataManager
from src.strategies import MACDCrossoverStrategy, RSI_EMA_Strategy, SonicRStrategy
from src.risk import RiskManager
from src.notifier import TelegramNotifier

logger = get_logger("main", log_file="logs/trading.log")

_STRATEGY_REGISTRY = {
    "MACDCrossover": MACDCrossoverStrategy,
    "RSI_EMA": RSI_EMA_Strategy,
    "SonicR": SonicRStrategy,
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

    def on_new_bar(symbol: str, timeframe: str, df) -> None:
        """
        Callback from DataManager — fires for every new closed bar.
        Submits all strategies for this (symbol, timeframe) to thread pool.
        """
        key = (symbol, timeframe)
        for strategy in strategy_map.get(key, []):
            executor.submit(_evaluate_strategy, strategy, symbol, timeframe, df)

    def _evaluate_strategy(strategy, symbol: str, timeframe: str, df) -> None:
        signal = strategy.on_new_bar(symbol, timeframe, df)
        if signal and signal.is_actionable():
            complete = risk_manager.build_complete_signal(signal)
            if complete:
                notifier.send_signal(complete)

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
    logger_main.info("Trading system running. Press Ctrl+C to stop.")

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    stop_event = threading.Event()

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
