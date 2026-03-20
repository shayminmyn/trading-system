"""
Backtest Engine — vectorized, single-pass backtester.

Pip / lot math (all instruments)
---------------------------------
  XAUUSD / XAGUSD : 1 pip = $0.10  (user rule: 1 USD move = 10 pips)
                    1 standard lot = 100 oz  → pip_value = 100 × $0.10 = $10 / lot
  JPY pairs       : 1 pip = 0.01   → pip_value ≈ $9.09 / lot (100,000 × 0.01 / ~110)
  Standard Forex  : 1 pip = 0.0001 → pip_value = $10 / lot

Lot size formula
-----------------
  lot = (account_balance × risk_pct / 100) / (sl_pips × pip_value_per_lot)
  clamped to [0.01, 10.0]

P&L formula (USD)
------------------
  pnl = pips × pip_value_per_lot × lot_size
  pips > 0 → profit, pips < 0 → loss

Limit-order support
--------------------
  signal.limit_expiry_bars > 0 → limit order at signal.entry (pac_mid / EMA34).
  The engine scans forward up to limit_expiry_bars for a touch:
    BUY  → bar.low  ≤ limit_price → filled at limit_price (no slippage)
    SELL → bar.high ≥ limit_price → filled at limit_price (no slippage)
  If the limit is never touched → signal is discarded (MISS, not counted).
"""

from __future__ import annotations

import concurrent.futures
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from ..utils.logger import get_logger
from ..utils.gil_info import get_optimal_workers
from ..utils.tz_utils import fmt_ts

if TYPE_CHECKING:
    from ..strategies.base_strategy import BaseStrategy

logger = get_logger("backtest_engine")


# ── Pip / lot constants ───────────────────────────────────────────────────────

def _pip_size(symbol: str) -> float:
    """Price distance of 1 pip for the given symbol."""
    s = symbol.upper()
    if s in ("XAUUSD", "XAGUSD"):
        return 0.10       # $0.10 per pip (1 USD move = 10 pips)
    if "JPY" in s:
        return 0.01
    return 0.0001         # Standard Forex


def _pip_value(symbol: str) -> float:
    """USD value of 1 pip for 1 standard lot."""
    s = symbol.upper()
    if s in ("XAUUSD", "XAGUSD"):
        return 10.0       # 100 oz × $0.10
    if "JPY" in s:
        return 9.09       # approximate, USD account
    return 10.0           # 100,000 units × $0.0001


def _calc_lot(balance: float, risk_pct: float, sl_pips: float, symbol: str) -> float:
    """Position size in lots using percentage-risk model."""
    risk_usd = balance * risk_pct / 100.0
    pv = _pip_value(symbol)
    if sl_pips <= 0 or pv <= 0:
        return 0.01
    raw = risk_usd / (sl_pips * pv)
    return max(0.01, min(10.0, round(raw, 2)))


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class BacktestResult:
    """Complete results from one backtest run."""

    symbol:           str
    timeframe:        str
    strategy_name:    str
    initial_capital:  float
    final_equity:     float
    total_trades:     int
    winning_trades:   int
    losing_trades:    int
    winrate:          float          # 0–100 %
    total_return_pct: float
    max_drawdown_pct: float
    sharpe_ratio:     float
    profit_factor:    float
    avg_win_pips:     float
    avg_loss_pips:    float
    gross_profit:     float = 0.0
    gross_loss:       float = 0.0
    equity_curve:     list[float]   = field(default_factory=list, repr=False)
    trades:           list[dict]    = field(default_factory=list, repr=False)

    def summary(self) -> str:
        """Console-friendly summary with last-5 trades in UTC+7."""
        wins   = [t for t in self.trades if t.get("result") == "TP"]
        losses = [t for t in self.trades if t.get("result") == "SL"]
        misses = [t for t in self.trades if t.get("result") == "MISS"]

        limit_n  = sum(1 for t in self.trades if t.get("order_type") == "LIMIT")
        market_n = sum(1 for t in self.trades if t.get("order_type") == "MARKET")
        avg_vol  = (sum(t.get("volume", 0) for t in self.trades) / len(self.trades)
                    if self.trades else 0.0)

        is_gold  = self.symbol.upper() in ("XAUUSD", "XAGUSD")
        fp = (lambda p: f"{p:.2f}") if is_gold else (lambda p: f"{p:.5f}")

        lines = [
            f"\n{'═'*72}",
            f"  Backtest  : {self.strategy_name} | {self.symbol} {self.timeframe}",
            f"{'─'*72}",
            f"  Capital   : ${self.initial_capital:,.2f}  →  ${self.final_equity:,.2f}"
            f"  ({self.total_return_pct:+.2f}%)",
            f"  Max DD    : {self.max_drawdown_pct:.2f}%   Sharpe: {self.sharpe_ratio:.3f}"
            f"   PF: {self.profit_factor:.2f}",
            f"  Winrate   : {self.winrate:.1f}%  ({len(wins)}W / {len(losses)}L"
            + (f" / {len(misses)} MISS" if misses else "")
            + f" / {self.total_trades} total)",
            f"  Avg Win   : {self.avg_win_pips:.1f} pips   "
            f"Avg Loss: {self.avg_loss_pips:.1f} pips   Vol: {avg_vol:.2f} lot",
            f"  Gross +   : +${self.gross_profit:,.2f}   "
            f"Gross - : -${self.gross_loss:,.2f}",
            f"  Orders    : {limit_n} LIMIT  {market_n} MARKET",
        ]

        if self.trades:
            lines.append(f"{'─'*72}")
            lines.append("  Last 5 trades (UTC+7):")
            lines.append(
                f"  {'Entry Time':<18}  {'Dir':4}  {'Typ':3}"
                f"  {'Entry':>8}  {'SL':>8}  {'TP':>8}"
                f"  {'Vol':>5}  {'Result':5}  {'Pips':>7}  {'P&L (USD)':>10}"
                f"  {'Balance':>10}  {'Exit Time':<18}"
            )
            for t in self.trades[-5:]:
                pnl  = t.get("pnl_usd", 0.0)
                bal  = t.get("balance_after", 0.0)
                sign = "+" if pnl >= 0 else ""
                otype = t.get("order_type", "MKT")[0]
                pips  = t.get("pips", 0.0)
                psign = "+" if pips >= 0 else ""
                lines.append(
                    f"  {fmt_ts(t['timestamp']):<18}  {t['action']:4}  {otype:3}"
                    f"  {fp(t['entry']):>8}  {fp(t['sl']):>8}  {fp(t['tp']):>8}"
                    f"  {t.get('volume', 0):.2f}L"
                    f"  {t['result']:5}  {psign}{pips:>6.1f}p"
                    f"  {sign}${abs(pnl):>8,.2f}"
                    f"  ${bal:>9,.2f}"
                    f"  {fmt_ts(t.get('exit_timestamp', '')):<18}"
                )
        lines.append(f"{'═'*72}")
        return "\n".join(lines)


# ── Engine ────────────────────────────────────────────────────────────────────

class BacktestEngine:
    """
    Vectorized backtester.

    Thread-safe: each run() call works on its own DataFrame copy.
    """

    def __init__(self, config: dict) -> None:
        bt = config.get("backtest", {})
        self._initial_capital:  float = float(bt.get("initial_cash", 10_000))
        self._commission_pct:   float = float(bt.get("commission_percent", 0.0))
        self._slippage_pips:    float = float(bt.get("slippage_pips", 2))
        self._workers:          int   = get_optimal_workers(
            config.get("concurrency", {}).get("strategy_workers", 4)
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def run(
        self,
        strategy: "BaseStrategy",
        df: pd.DataFrame,
        risk_pct: float = 1.5,
        rr_ratio: float = 2.0,
    ) -> BacktestResult:
        logger.info(
            "Backtest start: %s on %s/%s (%d bars)",
            strategy.name, strategy.symbol, strategy.timeframe, len(df),
        )
        df = df.reset_index(drop=True).copy()
        signals = self._generate_signals(strategy, df)
        trades  = self._simulate_trades(df, signals, risk_pct, rr_ratio, strategy.symbol)
        result  = self._compute_metrics(trades, strategy)
        logger.info("Backtest complete:%s", result.summary())
        return result

    def run_parallel(self, jobs: list[dict]) -> list[BacktestResult]:
        results = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._workers,
            thread_name_prefix="backtest-worker",
        ) as executor:
            futures = {
                executor.submit(
                    self.run,
                    job["strategy"], job["df"],
                    job.get("risk_pct", 1.5), job.get("rr_ratio", 2.0),
                ): job
                for job in jobs
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    results.append(future.result())
                except Exception:
                    j = futures[future]
                    logger.exception(
                        "Backtest job failed: %s %s",
                        j["strategy"].name, j["strategy"].symbol,
                    )
        return results

    # ── Signal generation ─────────────────────────────────────────────────────

    def _generate_signals(
        self, strategy: "BaseStrategy", df: pd.DataFrame
    ) -> list[dict]:
        """Replay bars through strategy to collect raw signals."""
        signals  = []
        min_bars = max(strategy._min_bars, 50)

        for i in range(min_bars, len(df)):
            window = df.iloc[: i + 1]
            sig = strategy.on_new_bar(strategy.symbol, strategy.timeframe, window)
            if sig and sig.is_actionable():
                signals.append({
                    "bar_index":         i,
                    "action":            sig.action,
                    "entry":             sig.entry,     # limit_price when limit order
                    "sl_pips":           sig.sl_pips,
                    "timestamp":         df.iloc[i]["timestamp"],
                    "limit_expiry_bars": getattr(sig, "limit_expiry_bars", 0),
                })

        logger.debug("Generated %d raw signals", len(signals))
        return signals

    # ── Trade simulation ──────────────────────────────────────────────────────

    def _simulate_trades(
        self,
        df: pd.DataFrame,
        signals: list[dict],
        risk_pct: float,
        rr_ratio: float,
        symbol: str,
    ) -> list[dict]:
        """
        Simulate each signal.

        Market order (limit_expiry_bars == 0):
            Fill on the bar AFTER the signal at signal.entry + slippage.

        Limit order (limit_expiry_bars > 0):
            signal.entry is the limit price (pac_mid / EMA34).
            Scan forward up to limit_expiry_bars bars:
              BUY  → bar.low  ≤ limit_price  → filled at limit_price
              SELL → bar.high ≥ limit_price  → filled at limit_price
            No slippage on limit fills.
            If unfilled → MISS (discarded).
        """
        trades: list[dict]       = []
        active_bars: set[int]    = set()
        ps   = _pip_size(symbol)
        pv   = _pip_value(symbol)
        bal  = self._initial_capital  # running balance

        for sig in signals:
            bar_idx = sig["bar_index"]
            if bar_idx in active_bars:
                continue

            action      = sig["action"]
            limit_price = sig["entry"]
            sl_pips     = sig["sl_pips"]
            expiry      = sig.get("limit_expiry_bars", 0)

            if sl_pips <= 0:
                continue

            # ── Determine fill bar and actual entry price ─────────────────────
            if expiry > 0:
                # Limit order: wait for price to touch limit_price
                fill_bar   = None
                scan_end   = min(bar_idx + 1 + expiry, len(df))
                for i in range(bar_idx + 1, scan_end):
                    row = df.iloc[i]
                    if action == "BUY"  and row["low"]  <= limit_price:
                        fill_bar = i; break
                    if action == "SELL" and row["high"] >= limit_price:
                        fill_bar = i; break

                if fill_bar is None:
                    continue  # MISS — limit never filled

                entry      = limit_price   # no slippage on limit fills
                start_bar  = fill_bar      # SL/TP checked from this bar onward
                order_type = "LIMIT"
            else:
                # Market order — fill on next bar with slippage
                slip  = self._slippage_pips * ps
                entry = limit_price + slip if action == "BUY" else limit_price - slip
                fill_bar   = bar_idx
                start_bar  = bar_idx + 1
                order_type = "MARKET"

            # ── Build SL / TP from actual fill entry ──────────────────────────
            sl_dist = sl_pips * ps
            tp_dist = sl_dist * rr_ratio

            if action == "BUY":
                sl_price = entry - sl_dist
                tp_price = entry + tp_dist
            else:
                sl_price = entry + sl_dist
                tp_price = entry - tp_dist

            volume  = _calc_lot(bal, risk_pct, sl_pips, symbol)
            outcome = self._find_outcome(
                df, start_bar, action, entry, sl_price, tp_price, ps
            )

            # ── P&L calculation ───────────────────────────────────────────────
            # pips: positive = profit, negative = loss (measured from entry)
            pnl_usd = round(outcome["pips"] * pv * volume, 2)

            bal_before = round(bal, 2)
            bal       += pnl_usd
            bal_after  = round(bal, 2)

            trades.append({
                "bar_index":      bar_idx,
                "fill_bar":       fill_bar,
                "timestamp":      sig["timestamp"],
                "exit_timestamp": outcome["exit_timestamp"],
                "action":         action,
                "order_type":     order_type,
                "entry":          round(entry, 5),
                "sl":             round(sl_price, 5),
                "tp":             round(tp_price, 5),
                "sl_pips":        round(sl_pips, 2),
                "volume":         volume,
                "result":         outcome["result"],
                "exit_bar":       outcome["exit_bar"],
                "exit_price":     round(outcome["exit_price"], 5),
                "pips":           round(outcome["pips"], 2),
                "pnl_usd":        pnl_usd,
                "balance_before": bal_before,
                "balance_after":  bal_after,
            })
            active_bars.add(bar_idx)

        return trades

    def _find_outcome(
        self,
        df: pd.DataFrame,
        start_bar: int,
        action: str,
        entry: float,
        sl: float,
        tp: float,
        ps: float,
    ) -> dict:
        """
        Walk forward from start_bar to find the first SL or TP hit.

        Pips are signed: positive = profit (TP), negative = loss (SL).
        Calculation: (exit_price - entry) / ps for BUY
                     (entry - exit_price) / ps for SELL
        """
        def _make(result: str, bar: int, exit_px: float) -> dict:
            raw_pips = (exit_px - entry) / ps if action == "BUY" else (entry - exit_px) / ps
            ts = df.iloc[bar]["timestamp"] if bar < len(df) else df.iloc[-1]["timestamp"]
            return {
                "result":          result,
                "exit_bar":        bar,
                "exit_price":      exit_px,
                "pips":            raw_pips,
                "exit_timestamp":  ts,
            }

        for i in range(start_bar, len(df)):
            high = float(df.iloc[i]["high"])
            low  = float(df.iloc[i]["low"])

            if action == "BUY":
                # Check SL first (conservative — bad fill takes priority)
                if low <= sl:
                    return _make("SL", i, sl)
                if high >= tp:
                    return _make("TP", i, tp)
            else:
                if high >= sl:
                    return _make("SL", i, sl)
                if low <= tp:
                    return _make("TP", i, tp)

        # Still open at end of data
        last_close = float(df.iloc[-1]["close"])
        return _make("OPEN", len(df) - 1, last_close)

    # ── Metrics ───────────────────────────────────────────────────────────────

    def _compute_metrics(
        self,
        trades: list[dict],
        strategy: "BaseStrategy",
    ) -> BacktestResult:
        if not trades:
            return BacktestResult(
                symbol=strategy.symbol, timeframe=strategy.timeframe,
                strategy_name=strategy.name,
                initial_capital=self._initial_capital,
                final_equity=self._initial_capital,
                total_trades=0, winning_trades=0, losing_trades=0,
                winrate=0.0, total_return_pct=0.0, max_drawdown_pct=0.0,
                sharpe_ratio=0.0, profit_factor=0.0,
                avg_win_pips=0.0, avg_loss_pips=0.0,
                gross_profit=0.0, gross_loss=0.0,
            )

        # Equity curve is already built trade-by-trade in _simulate_trades
        equity_curve = [self._initial_capital] + [
            t["balance_after"] for t in trades
        ]

        wins   = [t for t in trades if t["result"] == "TP"]
        losses = [t for t in trades if t["result"] == "SL"]

        # Gross profit / loss in USD
        gross_profit = sum(t["pnl_usd"] for t in wins   if t["pnl_usd"] > 0)
        gross_loss   = abs(sum(t["pnl_usd"] for t in losses if t["pnl_usd"] < 0))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        # Pips stats (absolute values for avg)
        win_pips  = [abs(t["pips"]) for t in wins]
        loss_pips = [abs(t["pips"]) for t in losses]

        # Drawdown
        eq_arr = np.array(equity_curve)
        peak   = np.maximum.accumulate(eq_arr)
        dd     = (peak - eq_arr) / (peak + 1e-9) * 100
        max_dd = float(dd.max())

        # Sharpe (trade-level returns)
        returns = np.diff(eq_arr) / (eq_arr[:-1] + 1e-9)
        sharpe  = (
            float(np.mean(returns) / np.std(returns) * np.sqrt(252))
            if np.std(returns) > 0 else 0.0
        )

        final_equity = trades[-1]["balance_after"]

        return BacktestResult(
            symbol=strategy.symbol,
            timeframe=strategy.timeframe,
            strategy_name=strategy.name,
            initial_capital=self._initial_capital,
            final_equity=round(final_equity, 2),
            total_trades=len(trades),
            winning_trades=len(wins),
            losing_trades=len(losses),
            winrate=round(len(wins) / len(trades) * 100, 2) if trades else 0.0,
            total_return_pct=round(
                (final_equity - self._initial_capital) / self._initial_capital * 100, 2
            ),
            max_drawdown_pct=round(max_dd, 2),
            sharpe_ratio=round(sharpe, 3),
            profit_factor=round(profit_factor, 2),
            avg_win_pips=round(float(np.mean(win_pips)),  1) if win_pips  else 0.0,
            avg_loss_pips=round(float(np.mean(loss_pips)), 1) if loss_pips else 0.0,
            gross_profit=round(gross_profit, 2),
            gross_loss=round(gross_loss, 2),
            equity_curve=equity_curve,
            trades=trades,
        )
