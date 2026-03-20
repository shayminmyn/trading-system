"""
Backtest Engine — runs a strategy over historical OHLCV data.

Uses a vectorized approach (pure NumPy/Pandas) as a lightweight alternative
to vectorbt. Supports vectorbt if installed for richer analytics.

With no-GIL Python: multiple (symbol, strategy) combinations can be
backtested in parallel using ThreadPoolExecutor.
"""

from __future__ import annotations

import concurrent.futures
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from ..utils.logger import get_logger
from ..utils.gil_info import get_optimal_workers
from ..utils.session_news_filter import is_friday_week_close_blackout
from ..utils.tz_utils import fmt_ts

if TYPE_CHECKING:
    from ..strategies.base_strategy import BaseStrategy

logger = get_logger("backtest_engine")


@dataclass
class BacktestResult:
    """Results from a single backtest run."""

    symbol: str
    timeframe: str
    strategy_name: str
    initial_capital: float
    final_equity: float
    min_balance: float              # Lowest equity during the backtest
    max_balance: float              # Highest equity during the backtest
    total_trades: int
    winning_trades: int
    losing_trades: int
    winrate: float          # 0–100 %
    total_return_pct: float
    max_drawdown_pct: float
    max_drawdown_daily_pct: float  # worst single-calendar-day DD % (see daily_drawdown_tz)
    sharpe_ratio: float
    profit_factor: float
    avg_win_pips: float
    avg_loss_pips: float
    equity_curve: list[float] = field(default_factory=list, repr=False)
    trades: list[dict] = field(default_factory=list, repr=False)

    def summary(self) -> str:
        active  = [t for t in self.trades if t.get("result") != "EXPIRED"]
        expired = [t for t in self.trades if t.get("result") == "EXPIRED"]

        avg_vol = (
            round(sum(t.get("volume", 0.0) for t in active) / len(active), 2)
            if active else 0.0
        )
        total_profit = sum(t.get("pnl_usd", 0.0) for t in active if t.get("pnl_usd", 0) > 0)
        total_loss   = sum(t.get("pnl_usd", 0.0) for t in active if t.get("pnl_usd", 0) < 0)

        is_gold = self.symbol in ("XAUUSD", "XAGUSD")

        def fp(p: float) -> str:
            return f"{p:.2f}" if is_gold else f"{p:.5f}"

        # Last 5 active trades preview (timestamps in UTC+7)
        preview_lines = ""
        for t in active[-5:]:
            pnl   = t.get("pnl_usd", 0.0)
            bal   = t.get("balance_after", 0.0)
            sign  = "+" if pnl >= 0 else ""
            otype = t.get("order_type", "MKT")[:3]
            ex_ts = t.get("exit_timestamp")
            ex_str = fmt_ts(ex_ts) if ex_ts is not None else "—"
            preview_lines += (
                f"  {fmt_ts(t['timestamp'])}  {t['action']:4s}[{otype}]"
                f"  entry={fp(t['entry'])}  SL={fp(t['sl'])}  TP={fp(t['tp'])}"
                f"  vol={t.get('volume',0):.2f}L"
                f"  → {t['result']:4s} @{ex_str}  P&L={sign}${pnl:.2f}"
                f"  bal=${bal:,.2f}\n"
            )

        expired_line = f"  Expired      : {len(expired)} limit orders (unfilled)\n" if expired else ""

        return (
            f"\n{'═'*70}\n"
            f"  Backtest: {self.strategy_name} | {self.symbol} {self.timeframe}\n"
            f"{'─'*70}\n"
            f"  Capital      : ${self.initial_capital:,.2f} → ${self.final_equity:,.2f}"
            f"  ({self.total_return_pct:+.2f}%)\n"
            f"  Min total $  : ${self.min_balance:,.2f}  |  Max total $ : ${self.max_balance:,.2f}\n"
            f"  Max Drawdown : {self.max_drawdown_pct:.2f}%  "
            f"(daily max: {self.max_drawdown_daily_pct:.2f}%)\n"
            f"  Sharpe Ratio : {self.sharpe_ratio:.3f}\n"
            f"  Profit Factor: {self.profit_factor:.2f}\n"
            f"  Winrate      : {self.winrate:.1f}%  "
            f"({self.winning_trades}W / {self.losing_trades}L / {self.total_trades}T)\n"
            f"  Avg Win      : {self.avg_win_pips:.1f} pips\n"
            f"  Avg Loss     : {self.avg_loss_pips:.1f} pips\n"
            f"  Avg Volume   : {avg_vol:.2f} lot\n"
            f"  Gross Profit : +${total_profit:,.2f}\n"
            f"  Gross Loss   : -${abs(total_loss):,.2f}\n"
            f"{expired_line}"
            + (f"{'─'*70}\n  Last 5 filled trades — timestamps UTC+7 (VN time):\n{preview_lines}"
               if active else "")
            + f"{'═'*70}"
        )


class BacktestEngine:
    """
    Vectorized backtester.

    Generates signals by replaying historical bars through a strategy,
    simulates trades with SL/TP, and computes performance metrics.

    Thread-safe: multiple BacktestEngine.run() calls can execute in parallel
    (each has its own DataFrame copy — no shared mutable state).
    """

    def __init__(self, config: dict) -> None:
        bt = config.get("backtest", {})
        self._initial_capital: float = float(bt.get("initial_cash", 10_000))
        self._commission_pct: float = float(bt.get("commission_percent", 0.0))
        self._slippage_pips: float = float(bt.get("slippage_pips", 2))
        self._workers: int = get_optimal_workers(config.get("concurrency", {}).get("strategy_workers", 4))
        # Same dict as strategies — used for Friday week-close window (UTC) as in session_filters.
        self._session_filters: dict = dict(config.get("session_filters") or {})
        # Cancel unfilled LIMITs when a bar falls in Friday [close−Nh, close) (see
        # friday_avoid_hours_before_week_close + friday_week_close_*_utc).
        self._cancel_limit_friday_close: bool = bool(
            bt.get("cancel_pending_limits_in_friday_close_window", False)
        )
        # True: lot size uses % of current equity (realized PnL only). False: always initial_cash.
        self._risk_on_equity: bool = bool(bt.get("risk_on_equity", True))
        # Calendar day boundary for max intraday drawdown (IANA tz, e.g. Asia/Ho_Chi_Minh).
        self._daily_drawdown_tz: str = str(bt.get("daily_drawdown_tz", "Asia/Ho_Chi_Minh"))

    def run(
        self,
        strategy: "BaseStrategy",
        df: pd.DataFrame,
        risk_pct: float = 1.5,
        rr_ratio: float = 2.0,
    ) -> BacktestResult:
        """
        Run a single strategy backtest on historical data.
        Returns a BacktestResult with all performance metrics.
        """
        logger.info(
            "Backtest start: %s on %s/%s (%d bars)",
            strategy.name, strategy.symbol, strategy.timeframe, len(df),
        )
        df = df.reset_index(drop=True).copy()
        

        signals = self._generate_signals(strategy, df)
        # Per-strategy concurrent-trade limit (default: unconstrained = 999)
        max_concurrent = getattr(strategy, "_max_concurrent_trades", 999)
        trades = self._simulate_trades(df, signals, risk_pct, rr_ratio, strategy.symbol, max_concurrent)
        result = self._compute_metrics(trades, strategy, df)

        logger.info("Backtest complete:%s", result.summary())
        return result

    def run_parallel(
        self,
        jobs: list[dict],
    ) -> list[BacktestResult]:
        """
        Run multiple backtests in parallel (leverages no-GIL truly parallel threads).

        Each job dict: {
            "strategy": BaseStrategy,
            "df": pd.DataFrame,
            "risk_pct": float,
            "rr_ratio": float,
        }
        """
        results = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._workers,
            thread_name_prefix="backtest-worker",
        ) as executor:
            futures = {
                executor.submit(
                    self.run,
                    job["strategy"],
                    job["df"],
                    job.get("risk_pct", 1.5),
                    job.get("rr_ratio", 2.0),
                ): job
                for job in jobs
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    results.append(future.result())
                except Exception:
                    job = futures[future]
                    logger.exception(
                        "Backtest job failed: %s %s",
                        job["strategy"].name,
                        job["strategy"].symbol,
                    )
        return results

    # ── Signal generation ─────────────────────────────────────────────────────

    def _generate_signals(
        self, strategy: "BaseStrategy", df: pd.DataFrame
    ) -> list[dict]:
        """Replay bars through strategy to collect raw signals."""
        signals = []
        min_bars = max(strategy._min_bars, 50)

        for i in range(min_bars, len(df)):
            window = df.iloc[:i + 1]
            signal = strategy.on_new_bar(strategy.symbol, strategy.timeframe, window)
            if signal and signal.is_actionable():
                signals.append({
                    "bar_index":         i,
                    "action":            signal.action,
                    "entry":             signal.entry,
                    "sl_pips":           signal.sl_pips,
                    "sl_level":          signal.sl_level,       # absolute SL price
                    "limit_price":       signal.limit_price,    # 0 = market order
                    "limit_expiry_bars": signal.limit_expiry_bars,
                    "timestamp":         df.iloc[i]["timestamp"],
                })

        logger.debug("Generated %d raw signals", len(signals))
        return signals

    # ── Pip / lot helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _pip_size(symbol: str) -> float:
        """1 pip in price units.
        XAUUSD/XAGUSD : $0.10  (1 USD move = 10 pips)
        JPY pairs     : 0.01
        Standard Forex: 0.0001
        """
        sym = symbol.upper()
        if sym in ("XAUUSD", "XAGUSD"):
            return 0.10
        if "JPY" in sym:
            return 0.01
        return 0.0001

    @staticmethod
    def _pip_value_per_lot(symbol: str) -> float:
        """USD value of 1 pip for 1 standard lot.
        XAUUSD : $0.10 × 100 oz = $10/lot
        Forex  : $0.0001 × 100,000 = $10/lot
        JPY    : ≈ $9.09/lot (approximate, USD account)
        """
        sym = symbol.upper()
        if sym in ("XAUUSD", "XAGUSD"):
            return 10.0
        if "JPY" in sym:
            return 9.09
        return 10.0

    def _equity_before_entry_bar(self, trades: list[dict], entry_bar: int, symbol: str) -> float:
        """
        Account balance before opening at `entry_bar`: initial cash plus realized P&L
        from trades that closed strictly before this bar (no floating P&L).
        """
        eq = float(self._initial_capital)
        pv = self._pip_value_per_lot(symbol)
        for t in trades:
            if t.get("result") == "EXPIRED":
                continue
            if int(t.get("exit_bar", -1)) < int(entry_bar):
                eq += float(t.get("volume", 0.01)) * float(t.get("pips", 0.0)) * pv
        return max(eq, 1e-6)

    def _calc_lot(
        self,
        sl_pips: float,
        symbol: str,
        risk_pct: float,
        *,
        balance: float | None = None,
    ) -> float:
        """Calculate position size in lots using % risk model on balance (equity) or initial cash."""
        cap = float(self._initial_capital)
        if self._risk_on_equity and balance is not None:
            cap = max(float(balance), 1e-6)
        risk_amount = cap * risk_pct / 100.0
        pv = self._pip_value_per_lot(symbol)
        if sl_pips <= 0 or pv <= 0:
            return 0.01
        raw = risk_amount / (sl_pips * pv)
        return max(0.01, min(10.0, round(raw, 2)))

    # ── Trade simulation ──────────────────────────────────────────────────────

    # Default limit-order expiry (bars) when signal does not specify one
    _DEFAULT_LIMIT_EXPIRY = 10

    @staticmethod
    def _timestamp_at_bar(df: pd.DataFrame, bar_index: int):
        """Safe bar index → timestamp for reports (exit / expiry time)."""
        if df.empty:
            return None
        i = int(min(max(0, bar_index), len(df) - 1))
        return df.iloc[i]["timestamp"]

    def _find_limit_fill(
        self,
        df: pd.DataFrame,
        start_bar: int,
        action: str,
        limit_price: float,
        expiry_bars: int,
    ) -> tuple[int, float, int]:
        """
        Scan forward for a limit-order fill.

        Returns (fill_bar_index, actual_fill_price, expiry_bar_index).

        On success: expiry_bar_index is -1 (unused) — fill_bar_index is the fill bar.
        On expiry: fill_bar_index is -1, fill_price = 0.0, expiry_bar_index is the
        bar index where the order stopped (for exit_timestamp / reporting).

        Fill rules:
          BUY  limit: fill when bar low  ≤ limit_price
          SELL limit: fill when bar high ≥ limit_price
          Gap fills: if open already crossed the limit, fill at open
          (conservative: protects against assuming we always get limit price
           when price gaps through it)

        Friday week close:
          If cancel_pending_limits_in_friday_close_window is True, any bar inside
          the same UTC window as session_filters (last N hours before Fri close)
          cancels the pending limit before checking for a fill on that bar.
        """
        if start_bar >= len(df):
            return -1, 0.0, max(0, len(df) - 1)

        end_bar = min(start_bar + expiry_bars, len(df))

        for i in range(start_bar, end_bar):
            if self._cancel_limit_friday_close and is_friday_week_close_blackout(
                df.iloc[i]["timestamp"], self._session_filters
            ):
                return -1, 0.0, i

            row  = df.iloc[i]
            o, h, l = float(row["open"]), float(row["high"]), float(row["low"])

            if action == "BUY":
                if o <= limit_price:
                    # Price gapped down through the limit — fill at open
                    return i, o, -1
                if l <= limit_price:
                    # Normal fill at our limit price
                    return i, limit_price, -1
            else:  # SELL
                if o >= limit_price:
                    # Price gapped up through the limit — fill at open
                    return i, o, -1
                if h >= limit_price:
                    return i, limit_price, -1

        # Expired after scanning window (no fill)
        last_tried = end_bar - 1 if end_bar > start_bar else start_bar - 1
        last_tried = max(0, last_tried)
        return -1, 0.0, last_tried

    def _simulate_trades(
        self,
        df: pd.DataFrame,
        signals: list[dict],
        risk_pct: float,
        rr_ratio: float,
        symbol: str,
        max_concurrent: int = 999,
    ) -> list[dict]:
        """
        Simulate each signal forward to find SL/TP outcome.

        Supports two order modes:
          MARKET  (limit_price == 0): enter at signal close + slippage on
                  the very next bar (existing behaviour).
          LIMIT   (limit_price  > 0): place a pending limit at limit_price.
                  The engine scans forward up to limit_expiry_bars looking for
                  a fill.  If unfilled, the order is marked EXPIRED and skipped
                  from performance metrics.

        max_concurrent controls how many trades/pending-orders may be active
        simultaneously.  Set to 1 for "one trade at a time" strategies.
        A pending limit order occupies a slot until it fills or expires.
        """
        trades    = []
        pip_size  = self._pip_size(symbol)
        slip_dist = self._slippage_pips * pip_size
        # Track signal-bar indices to avoid multiple entries on the same bar
        used_signal_bars: set[int] = set()
        # Each entry is the last bar on which a trade/pending-order is still active.
        # A slot is freed once that bar is strictly in the past.
        active_end_bars: list[int] = []

        for sig in signals:
            sig_bar  = sig["bar_index"]
            if sig_bar in used_signal_bars:
                continue

            # ── Concurrent-trade gate ──────────────────────────────────────────
            # Remove slots for trades that finished before this signal bar
            active_end_bars = [b for b in active_end_bars if b >= sig_bar]
            if len(active_end_bars) >= max_concurrent:
                continue

            action      = sig["action"]
            sl_pips_sig = sig["sl_pips"]       # pips from signal close → SL level
            sl_level    = sig.get("sl_level", 0.0)   # absolute SL price (if provided)
            limit_price = sig.get("limit_price", 0.0)
            expiry      = sig.get("limit_expiry_bars") or self._DEFAULT_LIMIT_EXPIRY

            if sl_pips_sig <= 0:
                continue

            # ── LIMIT order path ──────────────────────────────────────────────
            if limit_price > 0:
                fill_bar, fill_price, limit_expiry_bar = self._find_limit_fill(
                    df, sig_bar + 1, action, limit_price, expiry
                )

                if fill_bar < 0:
                    # Order expired without fill — record but exclude from metrics
                    exp_bar = int(limit_expiry_bar)
                    trades.append({
                        "bar_index":   sig_bar,
                        "timestamp":   sig["timestamp"],
                        "action":      action,
                        "order_type":  "LIMIT",
                        "entry":       round(limit_price, 5),
                        "sl":          0.0,
                        "tp":          0.0,
                        "sl_pips":     sl_pips_sig,
                        "volume":      0.0,
                        "result":      "EXPIRED",
                        "exit_bar":    exp_bar,
                        "exit_price":  0.0,
                        "pips":        0.0,
                        "exit_timestamp": self._timestamp_at_bar(df, exp_bar),
                    })
                    # Expired limit still occupied its slot until expiry / weekend cancel
                    active_end_bars.append(exp_bar)
                    used_signal_bars.add(sig_bar)
                    continue

                entry = fill_price

                # Recalculate sl_pips from the actual fill price using the
                # absolute SL level when available (most accurate).
                if sl_level > 0:
                    sl_dist = abs(entry - sl_level)
                    actual_sl_pips = sl_dist / pip_size
                else:
                    # Fall back: same pip distance as signal (slightly off but safe)
                    actual_sl_pips = sl_pips_sig
                    sl_dist = actual_sl_pips * pip_size

                if actual_sl_pips <= 0:
                    continue

                tp_dist = sl_dist * rr_ratio

                if action == "BUY":
                    sl_price = entry - sl_dist
                    tp_price = entry + tp_dist
                else:
                    sl_price = entry + sl_dist
                    tp_price = entry - tp_dist

                bal = self._equity_before_entry_bar(trades, fill_bar, symbol)
                volume  = self._calc_lot(actual_sl_pips, symbol, risk_pct, balance=bal)
                outcome = self._find_outcome(
                    df, fill_bar + 1, action, sl_price, tp_price, pip_size, entry_price=entry,
                )

                trades.append({
                    "bar_index":   sig_bar,
                    "timestamp":   df.iloc[fill_bar]["timestamp"],
                    "action":      action,
                    "order_type":  "LIMIT",
                    "entry":       round(entry, 5),
                    "sl":          round(sl_price, 5),
                    "tp":          round(tp_price, 5),
                    "sl_pips":     round(actual_sl_pips, 1),
                    "volume":      volume,
                    "result":      outcome["result"],
                    "exit_bar":    outcome["exit_bar"],
                    "exit_price":  outcome["exit_price"],
                    "pips":        outcome["pips"],
                    "exit_timestamp": self._timestamp_at_bar(df, outcome["exit_bar"]),
                })
                active_end_bars.append(outcome["exit_bar"])
                used_signal_bars.add(sig_bar)

            # ── MARKET order path (existing behaviour) ────────────────────────
            else:
                entry   = sig["entry"]
                sl_dist = sl_pips_sig * pip_size
                tp_dist = sl_dist * rr_ratio

                if action == "BUY":
                    entry    += slip_dist
                    sl_price  = entry - sl_dist
                    tp_price  = entry + tp_dist
                else:
                    entry    -= slip_dist
                    sl_price  = entry + sl_dist
                    tp_price  = entry - tp_dist

                entry_bar = sig_bar + 1
                bal = self._equity_before_entry_bar(trades, entry_bar, symbol)
                volume  = self._calc_lot(sl_pips_sig, symbol, risk_pct, balance=bal)
                outcome = self._find_outcome(
                    df, entry_bar, action, sl_price, tp_price, pip_size, entry_price=entry,
                )

                trades.append({
                    "bar_index":   sig_bar,
                    "timestamp":   sig["timestamp"],
                    "action":      action,
                    "order_type":  "MARKET",
                    "entry":       round(entry, 5),
                    "sl":          round(sl_price, 5),
                    "tp":          round(tp_price, 5),
                    "sl_pips":     sl_pips_sig,
                    "volume":      volume,
                    "result":      outcome["result"],
                    "exit_bar":    outcome["exit_bar"],
                    "exit_price":  outcome["exit_price"],
                    "pips":        outcome["pips"],
                    "exit_timestamp": self._timestamp_at_bar(df, outcome["exit_bar"]),
                })
                active_end_bars.append(outcome["exit_bar"])
                used_signal_bars.add(sig_bar)

        return trades

    def _find_outcome(
        self,
        df: pd.DataFrame,
        start_bar: int,
        action: str,
        sl: float,
        tp: float,
        pip_size: float,
        entry_price: float,
    ) -> dict:
        """
        Walk forward from start_bar to find SL or TP hit.

        Pips are always vs the *actual fill price* (entry_price), not the prior
        bar's close — required so LIMIT fills match reported P&L (a limit at
        5054.38 is not the same as that bar's close).
        """
        ep = float(entry_price)

        for i in range(start_bar, len(df)):
            row  = df.iloc[i]
            high = row["high"]
            low  = row["low"]

            if action == "BUY":
                if low <= sl:
                    pips = (sl - ep) / pip_size
                    return {"result": "SL", "exit_bar": i, "exit_price": sl, "pips": pips}
                if high >= tp:
                    pips = (tp - ep) / pip_size
                    return {"result": "TP", "exit_bar": i, "exit_price": tp, "pips": pips}
            else:
                if high >= sl:
                    pips = (ep - sl) / pip_size
                    return {"result": "SL", "exit_bar": i, "exit_price": sl, "pips": pips}
                if low <= tp:
                    pips = (ep - tp) / pip_size
                    return {"result": "TP", "exit_bar": i, "exit_price": tp, "pips": pips}

        # Trade still open at end of data — mark-to-market at last bar
        last_close = float(df.iloc[-1]["close"])
        if action == "BUY":
            pips = (last_close - ep) / pip_size
        else:
            pips = (ep - last_close) / pip_size
        return {"result": "OPEN", "exit_bar": len(df) - 1, "exit_price": last_close, "pips": pips}

    # ── Metrics ───────────────────────────────────────────────────────────────

    @staticmethod
    def _max_drawdown_daily_pct(
        active_trades: list[dict],
        equity_curve: list[float],
        tz_name: str,
    ) -> float:
        """
        Maximum drawdown (%) occurring within a single calendar day in `tz_name`.

        Equity updates at trade exit; each day we take [equity at start of day,
        ...equity after each close that day] and measure peak-to-trough DD within
        that sequence. Return the worst such daily DD across the backtest.
        """
        if not active_trades or len(equity_curve) < 2:
            return 0.0
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            logger.warning("Invalid daily_drawdown_tz %r — using UTC", tz_name)
            tz = ZoneInfo("UTC")

        by_day: dict = defaultdict(list)
        for i, tr in enumerate(active_trades):
            ex = tr.get("exit_timestamp")
            if ex is None:
                continue
            t = pd.Timestamp(ex)
            if t.tzinfo is None:
                t = t.tz_localize("UTC")
            else:
                t = t.tz_convert("UTC")
            local_date = t.tz_convert(tz).date()
            eq_after = equity_curve[i + 1]
            by_day[local_date].append((t, eq_after))

        if not by_day:
            return 0.0

        sorted_days = sorted(by_day.keys())
        prev_eod = float(equity_curve[0])
        overall_max_dd = 0.0

        for d in sorted_days:
            day_events = sorted(by_day[d], key=lambda x: x[0])
            seq = [prev_eod]
            for _, eq in day_events:
                seq.append(float(eq))
            peak = seq[0]
            for eq in seq:
                peak = max(peak, eq)
                if peak > 0:
                    dd = (peak - eq) / peak * 100.0
                    overall_max_dd = max(overall_max_dd, dd)
            prev_eod = seq[-1]

        return round(overall_max_dd, 2)

    def _compute_metrics(
        self,
        trades: list[dict],
        strategy: "BaseStrategy",
        df: pd.DataFrame,
    ) -> BacktestResult:
        if not trades:
            ic = self._initial_capital
            return BacktestResult(
                symbol=strategy.symbol,
                timeframe=strategy.timeframe,
                strategy_name=strategy.name,
                initial_capital=ic,
                final_equity=ic,
                min_balance=ic,
                max_balance=ic,
                total_trades=0,
                winning_trades=0,
                losing_trades=0,
                winrate=0.0,
                total_return_pct=0.0,
                max_drawdown_pct=0.0,
                max_drawdown_daily_pct=0.0,
                sharpe_ratio=0.0,
                profit_factor=0.0,
                avg_win_pips=0.0,
                avg_loss_pips=0.0,
                trades=[],
            )

        # Actual pip value per lot (used for real P&L calculation)
        pip_val = self._pip_value_per_lot(strategy.symbol)

        # Separate expired limit orders — they don't affect equity
        active_trades  = [t for t in trades if t["result"] != "EXPIRED"]
        expired_trades = [t for t in trades if t["result"] == "EXPIRED"]

        equity       = self._initial_capital
        equity_curve = [equity]

        for trade in active_trades:
            vol = trade.get("volume", 0.01)
            # pips field is already signed: positive = profit, negative = loss
            pnl = round(vol * trade["pips"] * pip_val, 2)
            trade["balance_before"] = round(equity, 2)
            equity += pnl
            trade["balance_after"]  = round(equity, 2)
            trade["pnl_usd"]        = pnl
            equity_curve.append(equity)

        # Classify by *realised P&L sign* so TP/SL labels always match $ outcome
        wins   = [t for t in active_trades if t["pnl_usd"] > 0]
        losses = [t for t in active_trades if t["pnl_usd"] < 0]

        equity_arr = np.array(equity_curve)
        peak       = np.maximum.accumulate(equity_arr)
        drawdown   = (peak - equity_arr) / peak * 100
        max_dd     = float(drawdown.max())

        returns = np.diff(equity_arr) / equity_arr[:-1]
        sharpe  = (
            float(np.mean(returns) / np.std(returns) * np.sqrt(252))
            if np.std(returns) > 0 else 0.0
        )

        gross_profit   = sum(t["pnl_usd"] for t in wins)
        gross_loss_abs = sum(abs(t["pnl_usd"]) for t in losses)
        profit_factor  = gross_profit / gross_loss_abs if gross_loss_abs > 0 else float("inf")

        win_pips  = [abs(t["pips"]) for t in wins]
        loss_pips = [abs(t["pips"]) for t in losses]

        min_bal = float(np.min(equity_curve))
        max_bal = float(np.max(equity_curve))

        max_dd_daily = self._max_drawdown_daily_pct(
            active_trades, equity_curve, self._daily_drawdown_tz
        )

        return BacktestResult(
            symbol=strategy.symbol,
            timeframe=strategy.timeframe,
            strategy_name=strategy.name,
            initial_capital=self._initial_capital,
            final_equity=round(equity, 2),
            min_balance=round(min_bal, 2),
            max_balance=round(max_bal, 2),
            total_trades=len(active_trades),
            winning_trades=len(wins),
            losing_trades=len(losses),
            winrate=round(len(wins) / len(active_trades) * 100, 2) if active_trades else 0.0,
            total_return_pct=round(
                (equity - self._initial_capital) / self._initial_capital * 100, 2
            ),
            max_drawdown_pct=round(max_dd, 2),
            max_drawdown_daily_pct=max_dd_daily,
            sharpe_ratio=round(sharpe, 3),
            profit_factor=round(profit_factor, 2),
            avg_win_pips=round(np.mean(win_pips), 1) if win_pips else 0.0,
            avg_loss_pips=round(np.mean(loss_pips), 1) if loss_pips else 0.0,
            equity_curve=equity_curve,
            trades=active_trades + expired_trades,  # expired last for HTML log
        )
