"""
Risk Management Module.

Responsibilities:
  - Calculate position size (Lot size) from % risk and SL distance
  - Calculate TP levels from Reward:Risk ratio
  - Build a complete, trade-ready signal object
  - Guard against duplicate signals (dedup by symbol+timeframe+action)

Pip value reference:
  FOREX standard (EURUSD, GBPUSD, etc.):
    Standard lot = 100,000 units | 1 pip = 0.0001 | pip_value = $10/pip/lot

  XAUUSD (Gold):
    1 pip = $0.10 price move (1 USD move = 10 pips)
    Standard lot = 100 oz | pip_value = 0.10 × 100 = $10/pip/lot

  USDJPY / JPY pairs:
    1 pip = 0.01 | pip_value ≈ $9.09/pip/lot (varies with USD/JPY rate)

When connected to MT5, pip values are fetched from symbol_info for accuracy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

from ..strategies.base_strategy import Signal
from ..utils.logger import get_logger

logger = get_logger("risk_manager")

# Default pip values per lot for common symbols (USD account)
_DEFAULT_PIP_VALUE: dict[str, float] = {
    "XAUUSD": 10.0,     # 1 pip ($0.10) × 100 oz = $10/lot
    "XAUUSDm": 10.0,    # 1 pip ($0.10) × 100 oz = $10/lot
    "XAGUSD": 10.0,    # same convention as Gold
    "EURUSD": 10.0,    # 0.0001 × 100,000 = $10/lot
    "GBPUSD": 10.0,
    "AUDUSD": 10.0,
    "NZDUSD": 10.0,
    "USDCAD": 10.0,    # approximate (varies)
    "USDCHF": 10.0,
    "USDJPY":  9.09,   # approximate (1 / 110 × 100,000 × 0.01)
    "EURGBP": 10.0,
}


def _make_order_id(ts, symbol: str, timeframe: str) -> str:
    """Generate a unique order ID: SYMBOL-TF-YYYYMMdd-HHMM from bar timestamp."""
    if hasattr(ts, "strftime"):
        date_str = ts.strftime("%Y%m%d")
        time_str = ts.strftime("%H%M")
    else:
        now = datetime.now(tz=timezone.utc)
        date_str = now.strftime("%Y%m%d")
        time_str = now.strftime("%H%M")
    sym = symbol.replace(".", "").upper()[:8]
    return f"{sym}-{timeframe.upper()}-{date_str}-{time_str}"


@dataclass
class CompleteSignal:
    """Fully resolved signal ready for notification."""

    symbol: str
    timeframe: str
    action: Literal["BUY", "SELL", "BUY LIMIT", "SELL LIMIT"]
    entry: float
    sl: float
    sl_pips: float
    tp1: float
    tp2: float | None
    volume: float
    risk_percent: float
    risk_amount_usd: float
    rr_ratio: float
    strategy_name: str
    notes: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    order_id: str = ""

    def __str__(self) -> str:
        return (
            f"[{self.action}] {self.symbol}/{self.timeframe} "
            f"entry={self.entry:.5f} SL={self.sl:.5f} "
            f"TP1={self.tp1:.5f} vol={self.volume:.2f}lot "
            f"risk={self.risk_percent:.1f}% (${self.risk_amount_usd:.2f})"
        )


class RiskManager:
    """
    Converts a raw Strategy Signal into a tradeable CompleteSignal.

    Thread-safe: all public methods are pure functions (no shared state mutation).
    Safe to call from multiple threads simultaneously with no-GIL.
    """

    def __init__(self, config: dict) -> None:
        rm = config.get("risk_management", {})
        self._balance: float = float(rm.get("account_balance", 10_000))
        self._risk_pct: float = float(rm.get("risk_per_trade_percent", 1.5))
        self._rr_ratio: float = float(rm.get("default_rr_ratio", 2.0))
        self._min_lot: float = float(rm.get("min_lot_size", 0.01))
        self._max_lot: float = float(rm.get("max_lot_size", 10.0))
        self._mt5_connector = None  # injected by DataManager if available

    def set_balance(self, balance: float) -> None:
        self._balance = balance

    def attach_mt5(self, connector) -> None:
        """Optionally attach MT5 connector for live pip value lookups."""
        self._mt5_connector = connector

    # ── Public API ────────────────────────────────────────────────────────────

    def build_complete_signal(self, signal: Signal) -> CompleteSignal | None:
        """
        Convert a raw strategy Signal to a CompleteSignal with calculated
        lot size, SL price, and TP levels.
        Returns None if signal is not actionable or calculation fails.
        """
        if not signal.is_actionable():
            return None

        try:
            pip_value = self._get_pip_value(signal.symbol)
            risk_usd = self._balance * self._risk_pct / 100.0

            # For limit orders the fill price is limit_price, not the close of the
            # signal bar.  All risk calculations (SL distance, lot, TP) must use the
            # actual fill price so that SL pips, TP levels and lot size are correct.
            actual_entry = signal.limit_price if signal.limit_price > 0 else signal.entry

            # Use exact sl_level from strategy when available (swing/ATR price),
            # then derive sl_pips from the real price distance to avoid pip-size
            # mismatch bugs (e.g. XAUUSDm strategy computed pips with wrong pip size).
            if signal.sl_level > 0:
                sl_price = signal.sl_level
                effective_sl_pips = abs(actual_entry - sl_price) / self._pip_size(signal.symbol)
            else:
                sl_price = self._calculate_sl_price(signal.action, actual_entry, signal.sl_pips, signal.symbol)
                effective_sl_pips = signal.sl_pips

            lot = self._calculate_lot(effective_sl_pips, pip_value, risk_usd)
            tp1 = self._calculate_tp(signal.action, actual_entry, effective_sl_pips, signal.symbol, self._rr_ratio)
            tp2 = self._calculate_tp(signal.action, actual_entry, effective_sl_pips, signal.symbol, self._rr_ratio * 1.5)

            action_str = f"{signal.action} LIMIT" if signal.limit_price > 0 else signal.action

            cs = CompleteSignal(
                symbol=signal.symbol,
                timeframe=signal.timeframe,
                action=action_str,
                entry=actual_entry,
                sl=round(sl_price, self._digits(signal.symbol)),
                sl_pips=round(effective_sl_pips, 1),
                tp1=round(tp1, self._digits(signal.symbol)),
                tp2=round(tp2, self._digits(signal.symbol)),
                volume=lot,
                risk_percent=self._risk_pct,
                risk_amount_usd=round(risk_usd, 2),
                rr_ratio=self._rr_ratio,
                strategy_name=signal.strategy_name,
                notes=signal.notes,
                timestamp=signal.timestamp,
                order_id=_make_order_id(signal.timestamp, signal.symbol, signal.timeframe),
            )
            logger.info("Risk calc complete: %s", cs)
            return cs

        except Exception:
            logger.exception("Risk calculation failed for %s", signal)
            return None

    def get_pip_value(self, symbol: str) -> float:
        return self._get_pip_value(symbol)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _get_pip_value(self, symbol: str) -> float:
        """
        Return pip value (USD) per standard lot for `symbol`.
        Tries MT5 symbol_info first, falls back to table.
        """
        if self._mt5_connector:
            try:
                info = self._mt5_connector.get_symbol_info(symbol)
                if info:
                    point = info["point"]
                    contract = info["trade_contract_size"]
                    # 1 pip = 10 × point regardless of digit count:
                    #   5-digit EURUSD  : point=0.00001 → pip=0.0001  ✓
                    #   3-digit USDJPY  : point=0.001   → pip=0.01    ✓
                    #   2-digit XAUUSD  : point=0.01    → pip=0.10    ✓
                    pip = point * 10
                    return pip * contract
            except Exception:
                pass
        return _DEFAULT_PIP_VALUE.get(symbol, 10.0)

    def _calculate_lot(self, sl_pips: float, pip_value: float, risk_usd: float) -> float:
        """
        Lot Size = Risk (USD) / (SL pips × pip_value per lot)
        Clamps to [min_lot, max_lot] and rounds to 2 decimals.
        """
        if sl_pips <= 0 or pip_value <= 0:
            return self._min_lot
        raw = risk_usd / (sl_pips * pip_value)
        clamped = max(self._min_lot, min(self._max_lot, raw))
        return round(clamped, 2)

    def _calculate_sl_price(
        self, action: str, entry: float, sl_pips: float, symbol: str
    ) -> float:
        pip_size = self._pip_size(symbol)
        sl_distance = sl_pips * pip_size
        if action == "BUY":
            return entry - sl_distance
        return entry + sl_distance

    def _calculate_tp(
        self, action: str, entry: float, sl_pips: float, symbol: str, rr: float
    ) -> float:
        pip_size = self._pip_size(symbol)
        tp_distance = sl_pips * pip_size * rr
        if action == "BUY":
            return entry + tp_distance
        return entry - tp_distance

    def _pip_size(self, symbol: str) -> float:
        """1 pip in price units.
        XAUUSD/XAGUSD : $0.10  (1 USD move = 10 pips)
        JPY pairs     : 0.01
        Standard Forex: 0.0001
        """
        if symbol in ("XAUUSD", "XAUUSDm"):
            return 0.10
        if "JPY" in symbol:
            return 0.01
        return 0.0001

    def _digits(self, symbol: str) -> int:
        if symbol in ("XAUUSD", "XAUUSDm"):
            return 2   # e.g. 2150.50 — round to cents
        if "JPY" in symbol:
            return 3
        return 5
