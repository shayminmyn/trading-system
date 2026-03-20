"""
Backtest Entry Point

Run backtests for all configured (symbol × timeframe × strategy) combinations.
Results are saved as HTML + JSON reports in backtest_results/.

Examples:
  python backtest.py                                  # all pairs from config
  python backtest.py --symbol XAUUSD --tf H1          # specific pair
  python backtest.py --strategy MACDCrossover         # specific strategy
  PYTHON_GIL=0 python backtest.py                     # parallel (no-GIL)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.utils import get_logger, ConfigLoader, print_runtime_info, get_optimal_workers
from src.data import DataManager, HistoricalLoader
from src.data.mock_source import generate_ohlcv
from src.strategies import MACDCrossoverStrategy, RSI_EMA_Strategy, SonicRStrategy
from src.backtest import BacktestEngine, ReportGenerator

logger = get_logger("backtest", log_file="logs/trading.log")

_STRATEGY_REGISTRY = {
    # "MACDCrossover": MACDCrossoverStrategy,
    # "RSI_EMA": RSI_EMA_Strategy,
    "SonicR": SonicRStrategy,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trading System Backtest Runner")
    parser.add_argument("--symbol", default=None, help="Filter by symbol (e.g. XAUUSD)")
    parser.add_argument("--tf", "--timeframe", default=None, help="Filter by timeframe (e.g. H1)")
    parser.add_argument("--strategy", default=None, help="Filter by strategy name")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--mock-bars", type=int, default=2000,
                        help="Number of synthetic bars to generate when no CSV found")
    return parser.parse_args()


def load_data(symbol: str, timeframe: str, cfg: dict, mock_bars: int):
    """Load historical data — real CSV if available, synthetic otherwise."""
    loader = HistoricalLoader(cfg.get("data", {}).get("historical_dir", "data/historical"))
    try:
        df = loader.load(symbol, timeframe)
        logger.info("Loaded real data: %s/%s — %d bars", symbol, timeframe, len(df))
        return df
    except FileNotFoundError:
        logger.warning(
            "No CSV found for %s/%s — using %d synthetic bars for demo",
            symbol, timeframe, mock_bars,
        )
        return generate_ohlcv(symbol, timeframe, n_bars=mock_bars, seed=42)


def main() -> None:
    print_runtime_info()
    args = parse_args()

    cfg = ConfigLoader.load(args.config)
    bt_cfg = cfg.get("backtest", {})
    rm_cfg = cfg.get("risk_management", {})
    strat_params = cfg.get("strategies", {})

    engine = BacktestEngine(cfg.raw)
    reporter = ReportGenerator(bt_cfg.get("output_dir", "backtest_results"))

    # ── Build job list ────────────────────────────────────────────────────────
    jobs = []
    for pair in cfg["trading_pairs"]:
        symbol = pair["symbol"]
        if args.symbol and symbol != args.symbol:
            continue
        for tf in pair.get("timeframes", []):
            if args.tf and tf != args.tf:
                continue
            
            df = load_data(symbol, tf, cfg.raw, args.mock_bars)
            df = df[df["timestamp"] > "2026-01-01"]
            if df.empty or len(df) < 100:
                logger.warning("Not enough data for %s/%s — skipping", symbol, tf)
                continue

            for strat_name in pair.get("strategies", list(_STRATEGY_REGISTRY.keys())):
                if args.strategy and strat_name != args.strategy:
                    continue
                cls = _STRATEGY_REGISTRY.get(strat_name)
                if cls is None:
                    continue
                params = strat_params.get(strat_name, {})
                strategy = cls(symbol=symbol, timeframe=tf, parameters=params)
                jobs.append({
                    "strategy": strategy,
                    "df": df,
                    "risk_pct": float(rm_cfg.get("risk_per_trade_percent", 1.5)),
                    "rr_ratio": float(rm_cfg.get("default_rr_ratio", 2.0)),
                })

    if not jobs:
        logger.error("No backtest jobs matched the given filters.")
        sys.exit(1)

    logger.info("Running %d backtest job(s) with %d workers…", len(jobs), get_optimal_workers())

    # ── Run in parallel ───────────────────────────────────────────────────────
    if len(jobs) == 1:
        results = [engine.run(**jobs[0])]
    else:
        results = engine.run_parallel(jobs)

    # ── Reports ───────────────────────────────────────────────────────────────
    for result in results:
        print(result.summary())
        reporter.generate(result)

    if len(results) > 1:
        comparison_path = reporter.generate_multi(results)
        logger.info("Comparison report: %s", comparison_path)

    logger.info("All backtest reports saved to: %s", bt_cfg.get("output_dir", "backtest_results"))


if __name__ == "__main__":
    main()
