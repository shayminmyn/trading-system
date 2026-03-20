"""
Hidden Divergence Strategy (Phân kỳ ẩn) — RSI Hidden Divergence + EMA Trend Filter.

Core Concept:
  Hidden Divergence is a contradiction between price action and RSI during an
  established trend. It signals that a pullback has exhausted and the primary
  trend is ready to resume — the opposite of classic (regular) divergence.

Signal Logic:
  A. Hidden Bearish Divergence (SELL) — Downtrend (price below EMA89):
       Price makes Lower High   : High2 < High1
       RSI  makes Higher High   : RSI_High2 > RSI_High1
       → RSI recovered strongly but price couldn't exceed the prior high
         → buyer exhaustion; expect a continuation of the down-move.
       Entry : LIMIT order at High2 (the resistance area / EMA34 zone).
               Fires when RSI rolls over from High2 — no candle confirmation
               needed; the limit fill acts as the confirmation.
       SL    : Above High1 + ATR buffer  (protects the full prior wave structure).

  B. Hidden Bullish Divergence (BUY) — Uptrend (price above EMA89):
       Price makes Higher Low   : Low2 > Low1
       RSI  makes Lower Low     : RSI_Low2 < RSI_Low1
       → RSI dipped deeper but price maintained a higher structure
         → seller exhaustion; expect a continuation of the up-move.
       Entry : LIMIT order at Low2 (the support area / EMA34 zone).
               Fires when RSI hooks up from Low2 — no candle confirmation needed.
       SL    : Below Low1 - ATR buffer  (protects the full prior wave structure).

  This "retest entry" design gives superior fill prices vs. market-order entry
  and naturally avoids false breakouts — if price never retests the swing level,
  the order expires unused.

Limit Entry Modes (limit_entry_mode):
  "swing"   — limit at Low2 / High2 (the swing point itself, per spec default)
  "ema34"   — limit at EMA34 of the signal bar (PAC midline value zone)

Golden Combo Filters (raise win-rate to "sniper" level):
  1. Trend Filter (EMA89)   : BUY only above EMA89 / SELL only below EMA89.
  2. Confluence Zone        : Set require_ema_confluence=true to require the
                              2nd swing to touch EMA34 or EMA89 before signalling.
  3. RSI-50 Confirmation    : BUY  → RSI at Low2  > rsi_buy_floor  (default 40)
                              SELL → RSI at High2 < rsi_sell_ceil  (default 60)

Indicators:
  rsi    : RSI(rsi_period=14)
  ema34  : EMA(ema_fast=34)  — value zone / PAC midline
  ema89  : EMA(ema_slow=89)  — trend anchor
  ema200 : EMA(ema_trend=200) — macro trend filter
  atr    : ATR(atr_period=14)

Parameters (config.yaml → strategies.HiddenDivergence):
  rsi_period            : int    14
  ema_fast              : int    34
  ema_slow              : int    89
  ema_trend             : int    200
  atr_period            : int    14
  swing_lookback        : int    60     Bars scanned for swing-point detection
  swing_strength        : int    3      Bars each side required to confirm a swing
  min_swing_separation_bars : int 10   Min bars between swing1 and swing2 (2 đỉnh/2 đáy)
  max_swing_separation_bars : int 18   Max gap (0 = không giới hạn); ~10–15 như chart
  require_clean_segment_between_swings : bool  True  Giữa 2 đỉnh/đáy không có đỉnh/đáy phụ
  sl_buffer_atr         : float  0.3   ATR padding added to stop-loss (at High1/Low1)
  min_rr                : float  1.5   Minimum estimated reward-to-risk ratio
  # ── Limit-order entry ──────────────────────────────────────────────────────
  limit_entry           : bool   True  Use limit order (recommended)
  limit_entry_mode      : str  "swing" "swing"=at Low2/High2  "ema34"=at EMA34
  limit_expiry_bars     : int    10    Bars to wait before cancelling unfilled order
  # ── RSI-50 filters (Golden Combo) ─────────────────────────────────────────
  rsi_buy_floor         : float  40.0  RSI_Low2 must be above this  (BUY filter)
  rsi_sell_ceil         : float  60.0  RSI_High2 must be below this (SELL filter)
  # ── Entry candle (only relevant when limit_entry=false) ───────────────────
  require_strong_candle : bool   False For market-order mode: require strong candle
  strong_candle_ratio   : float  0.4   Min body/range ratio
  # ── EMA Confluence (set true for max accuracy, fewer trades) ─────────────
  require_ema_confluence: bool   False 2nd swing must be near EMA34 or EMA89
  ema_confluence_atr    : float  1.0   ATR tolerance for confluence check
"""

from __future__ import annotations

import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange

from .base_strategy import BaseStrategy, Signal
from ..utils.logger import get_logger

logger = get_logger("hidden_divergence")


class HiddenDivergenceStrategy(BaseStrategy):
    """
    Hidden Divergence (Phân kỳ ẩn) strategy.

    Detects hidden bullish / bearish RSI divergence within EMA89-confirmed
    trends.  Default entry is a LIMIT order placed at the 2nd swing point
    (Low2 for BUY, High2 for SELL) with SL anchored at the 1st swing point
    (Low1 / High1) — covering the full prior-wave structure.
    """

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        parameters: dict | None = None,
    ) -> None:
        super().__init__(symbol, timeframe, parameters)
        p = self.parameters

        # ── Concurrency limit (read by BacktestEngine) ───────────────────────
        # 1 = one trade/pending-order at a time; no new signal until current
        # trade closes or limit order expires.
        self._max_concurrent_trades: int = p.get("max_concurrent_trades", 1)

        # ── Core indicators ───────────────────────────────────────────────────
        self._rsi_period: int        = p.get("rsi_period", 14)
        self._ema_fast: int          = p.get("ema_fast", 34)
        self._ema_slow: int          = p.get("ema_slow", 89)
        self._ema_trend: int         = p.get("ema_trend", 200)
        self._atr_period: int        = p.get("atr_period", 14)

        # ── Swing detection ───────────────────────────────────────────────────
        self._swing_lookback: int    = p.get("swing_lookback", 60)
        self._swing_strength: int    = p.get("swing_strength", 3)
        # Min bar distance between the two swings (Low1→Low2 / High1→High2)
        self._min_swing_sep: int     = max(0, int(p.get("min_swing_separation_bars", 10)))
        # Max gap (0 = unlimited). Chart-style LH/HL often 10–18 bars apart.
        self._max_swing_sep: int      = max(0, int(p.get("max_swing_separation_bars", 18)))
        # Between the two swings: no interior high above High2 / low below Low2 (no extra peak/trough)
        self._clean_segment: bool     = bool(p.get("require_clean_segment_between_swings", True))
        # Low2/High2 on last bars — off by default so wide patterns (like textbook charts) qualify
        self._swing2_curr_or_prev: bool = bool(p.get("require_swing2_curr_or_prev", False))

        # ── Risk / RR ─────────────────────────────────────────────────────────
        self._sl_buffer_atr: float   = p.get("sl_buffer_atr", 0.3)
        self._min_rr: float          = p.get("min_rr", 1.5)

        # ── Limit-order entry ─────────────────────────────────────────────────
        self._limit_entry: bool      = bool(p.get("limit_entry", True))
        # "swing" → limit at Low2/High2;  "ema34" → limit at EMA34
        self._limit_mode: str        = p.get("limit_entry_mode", "swing")
        self._limit_expiry: int      = p.get("limit_expiry_bars", 10)

        # ── RSI-50 confluence filters ─────────────────────────────────────────
        self._rsi_buy_floor: float   = p.get("rsi_buy_floor", 40.0)
        self._rsi_sell_ceil: float   = p.get("rsi_sell_ceil", 60.0)

        # ── Entry candle strength (market-order mode only) ────────────────────
        self._req_strong_candle: bool = bool(p.get("require_strong_candle", False))
        self._strong_ratio: float     = p.get("strong_candle_ratio", 0.4)

        # ── EMA confluence (optional high-probability filter) ─────────────────
        self._req_ema_conf: bool      = bool(p.get("require_ema_confluence", False))
        self._ema_conf_atr: float     = p.get("ema_confluence_atr", 1.0)

        self._min_bars = (
            max(self._ema_trend, self._swing_lookback)
            + self._swing_strength
            + 10
        )

    # ── Indicators ────────────────────────────────────────────────────────────

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df["rsi"]    = RSIIndicator(close=df["close"], window=self._rsi_period).rsi()
        df["ema34"]  = EMAIndicator(close=df["close"], window=self._ema_fast).ema_indicator()
        df["ema89"]  = EMAIndicator(close=df["close"], window=self._ema_slow).ema_indicator()
        df["ema200"] = EMAIndicator(close=df["close"], window=self._ema_trend).ema_indicator()
        df["atr"]    = AverageTrueRange(
            high=df["high"], low=df["low"], close=df["close"],
            window=self._atr_period,
        ).average_true_range()
        return df

    # ── Signal generation ─────────────────────────────────────────────────────

    def generate_signal(self, df: pd.DataFrame) -> Signal:
        required = ["rsi", "ema34", "ema89", "atr"]
        df = df.dropna(subset=required).reset_index(drop=True)

        min_len = self._swing_lookback + self._swing_strength + 10
        if len(df) < min_len:
            return self._no_signal()

        curr = df.iloc[-1]
        prev = df.iloc[-2]

        if curr["atr"] <= 0 or pd.isna(curr["atr"]):
            return self._no_signal()

        sig = self._check_hidden_bullish(df, curr, prev)
        if sig is not None:
            return sig

        sig = self._check_hidden_bearish(df, curr, prev)
        if sig is not None:
            return sig

        return self._no_signal()

    # ── A: Hidden Bullish Divergence → BUY ───────────────────────────────────

    def _check_hidden_bullish(
        self,
        df: pd.DataFrame,
        curr: pd.Series,
        prev: pd.Series,
    ) -> Signal | None:
        """
        BUY setup: uptrend (close > EMA89).

        Divergence: Higher Low in price (Low2 > Low1), Lower Low in RSI.
        RSI must already be hooking upward from Low2.

        Limit entry at Low2 (or EMA34): waits for price to retest the support
        before filling — better average entry, built-in false-breakout filter.
        SL anchored below Low1 to protect the full prior wave structure.
        """
        if curr["close"] <= curr["ema89"]:
            return None

        low_idxs = self._merge_swing_low_indices(df["low"])
        if len(low_idxs) < 2:
            return None

        idx1, idx2 = low_idxs[-2], low_idxs[-1]
        sep = idx2 - idx1
        if sep < self._min_swing_sep:
            return None
        if self._max_swing_sep > 0 and sep > self._max_swing_sep:
            return None

        n = len(df)
        if self._swing2_curr_or_prev and idx2 not in (n - 1, n - 2):
            return None

        low1  = float(df["low"].iloc[idx1])
        low2  = float(df["low"].iloc[idx2])
        rsi1  = float(df["rsi"].iloc[idx1])
        rsi2  = float(df["rsi"].iloc[idx2])

        # Core: Higher Low in price + Lower Low in RSI
        if not (low2 > low1 and rsi2 < rsi1):
            return None

        # Giữa 2 đáy: không có nến nào tạo đáy thấp hơn Low2 (không đỉnh/đáy phụ chen)
        if self._clean_segment and not self._no_lower_low_between(df, idx1, idx2, low2):
            return None

        # RSI-50 filter: Low2 still in bullish RSI territory
        if rsi2 < self._rsi_buy_floor:
            return None

        # RSI hook: RSI has already turned up from Low2
        if curr["rsi"] <= rsi2:
            return None

        # Market-order mode: optional strong candle gate
        if not self._limit_entry and self._req_strong_candle:
            if not self._is_strong_candle(curr, "BUY"):
                return None

        # EMA confluence: Low2 near EMA34 or EMA89 (optional Golden Combo)
        if self._req_ema_conf and not self._near_ema_zone(df, idx2, low2, touch="low"):
            return None

        atr = float(curr["atr"])

        # SL anchored below Low1 — covers the entire prior wave structure
        sl_level    = low1 - atr * self._sl_buffer_atr

        # Determine entry (limit or market)
        if self._limit_entry:
            limit_price = self._resolve_limit_price_buy(low2, float(curr["ema34"]))
            # Guard: limit must be above sl_level and below current close
            if limit_price <= sl_level or limit_price >= float(curr["close"]):
                return None
            sl_distance = limit_price - sl_level
        else:
            limit_price = 0.0
            sl_distance = float(curr["close"]) - sl_level

        if sl_distance <= 0:
            return None

        # RR check vs. highest high in lookback (next resistance target)
        lb          = max(0, len(df) - self._swing_lookback)
        tp_estimate = float(df["high"].iloc[lb:].max())
        entry_ref   = limit_price if self._limit_entry else float(curr["close"])
        tp_distance = tp_estimate - entry_ref
        rr          = tp_distance / sl_distance if sl_distance > 0 else 0.0
        if rr < self._min_rr:
            logger.debug("HidDiv BUY skipped RR=%.2f < %.1f", rr, self._min_rr)
            return None

        sl_pips = self._price_to_pips(sl_distance)
        if sl_pips <= 0:
            return None

        return self._make_signal(
            "BUY", float(curr["close"]), sl_pips,
            f"HidDiv BUY★ | Low1={low1:.5f} Low2={low2:.5f} "
            f"RSI1={rsi1:.1f} RSI2={rsi2:.1f} "
            f"ema89={curr['ema89']:.5f} lim={limit_price:.5f} RR≈{rr:.1f}",
            limit_price=limit_price,
            limit_expiry_bars=self._limit_expiry,
            sl_level=sl_level,
        )

    # ── B: Hidden Bearish Divergence → SELL ──────────────────────────────────

    def _check_hidden_bearish(
        self,
        df: pd.DataFrame,
        curr: pd.Series,
        prev: pd.Series,
    ) -> Signal | None:
        """
        SELL setup: downtrend (close < EMA89).

        Divergence: Lower High in price (High2 < High1), Higher High in RSI.
        RSI must already be turning down from High2.

        Limit entry at High2 (or EMA34): waits for price to bounce back to
        resistance before filling.
        SL anchored above High1 to protect the full prior wave structure.
        """
        if curr["close"] >= curr["ema89"]:
            return None

        high_idxs = self._merge_swing_high_indices(df["high"])
        if len(high_idxs) < 2:
            return None

        idx1, idx2 = high_idxs[-2], high_idxs[-1]
        sep = idx2 - idx1
        if sep < self._min_swing_sep:
            return None
        if self._max_swing_sep > 0 and sep > self._max_swing_sep:
            return None

        n = len(df)
        if self._swing2_curr_or_prev and idx2 not in (n - 1, n - 2):
            return None

        high1 = float(df["high"].iloc[idx1])
        high2 = float(df["high"].iloc[idx2])
        rsi1  = float(df["rsi"].iloc[idx1])
        rsi2  = float(df["rsi"].iloc[idx2])

        # Core: Lower High in price (High2 < High1) + Higher High in RSI (RSI2 > RSI1)
        if not (high2 < high1 and rsi2 > rsi1):
            return None

        # Giữa 2 đỉnh: không có nến nào có high cao hơn High2 (đúng cấu trúc LH liền kề)
        if self._clean_segment and not self._no_higher_high_between(df, idx1, idx2, high2):
            return None

        # RSI-50 filter: High2 still in bearish RSI territory
        if rsi2 > self._rsi_sell_ceil:
            return None

        # RSI roll-over: RSI has already turned down from High2
        if curr["rsi"] >= rsi2:
            return None

        # Market-order mode: optional strong candle gate
        if not self._limit_entry and self._req_strong_candle:
            if not self._is_strong_candle(curr, "SELL"):
                return None

        # EMA confluence: High2 near EMA34 or EMA89 (optional Golden Combo)
        if self._req_ema_conf and not self._near_ema_zone(df, idx2, high2, touch="high"):
            return None

        atr = float(curr["atr"])

        # SL anchored above High1 — covers the entire prior wave structure
        sl_level    = high1 + atr * self._sl_buffer_atr

        # Determine entry (limit or market)
        if self._limit_entry:
            limit_price = self._resolve_limit_price_sell(high2, float(curr["ema34"]))
            # Guard: limit must be below sl_level and above current close
            if limit_price >= sl_level or limit_price <= float(curr["close"]):
                return None
            sl_distance = sl_level - limit_price
        else:
            limit_price = 0.0
            sl_distance = sl_level - float(curr["close"])

        if sl_distance <= 0:
            return None

        # RR check vs. lowest low in lookback (next support target)
        lb          = max(0, len(df) - self._swing_lookback)
        tp_estimate = float(df["low"].iloc[lb:].min())
        entry_ref   = limit_price if self._limit_entry else float(curr["close"])
        tp_distance = entry_ref - tp_estimate
        rr          = tp_distance / sl_distance if sl_distance > 0 else 0.0
        if rr < self._min_rr:
            logger.debug("HidDiv SELL skipped RR=%.2f < %.1f", rr, self._min_rr)
            return None

        sl_pips = self._price_to_pips(sl_distance)
        if sl_pips <= 0:
            return None

        return self._make_signal(
            "SELL", float(curr["close"]), sl_pips,
            f"HidDiv SELL★ | High1={high1:.5f} High2={high2:.5f} "
            f"RSI1={rsi1:.1f} RSI2={rsi2:.1f} "
            f"ema89={curr['ema89']:.5f} lim={limit_price:.5f} RR≈{rr:.1f}",
            limit_price=limit_price,
            limit_expiry_bars=self._limit_expiry,
            sl_level=sl_level,
        )

    # ── Limit price resolution ────────────────────────────────────────────────

    def _resolve_limit_price_buy(self, low2: float, ema34: float) -> float:
        """
        Return the BUY limit entry price.
        "swing" → Low2 (the confirmed support swing level)
        "ema34" → EMA34 of the signal bar (value zone midline)
        Falls back to whichever is higher (closer to price = safer fill).
        """
        if self._limit_mode == "ema34":
            return ema34
        # Default "swing": Low2 is the primary entry level.
        # If EMA34 is slightly above Low2, prefer EMA34 for a tighter trade.
        return max(low2, ema34) if abs(ema34 - low2) / max(low2, 1e-8) < 0.002 else low2

    def _resolve_limit_price_sell(self, high2: float, ema34: float) -> float:
        """
        Return the SELL limit entry price.
        "swing" → High2 (the confirmed resistance swing level)
        "ema34" → EMA34 of the signal bar (value zone midline)
        Falls back to whichever is lower (closer to price = safer fill).
        """
        if self._limit_mode == "ema34":
            return ema34
        # Default "swing": High2 is the primary entry level.
        return min(high2, ema34) if abs(ema34 - high2) / max(high2, 1e-8) < 0.002 else high2

    # ── Clean segment between the two swings (như chart: không đỉnh/đáy phụ giữa) ─

    def _no_higher_high_between(
        self,
        df: pd.DataFrame,
        idx1: int,
        idx2: int,
        high2: float,
    ) -> bool:
        """
        Bars strictly between idx1 and idx2 (exclusive): no high above High2.
        Ensures the second peak is the highest in that segment (Lower High pair).
        """
        if idx2 <= idx1 + 1:
            return True
        mid = df["high"].iloc[idx1 + 1 : idx2]
        if mid.empty:
            return True
        tol = 1e-9 * max(abs(high2), 1.0)
        return float(mid.max()) <= high2 + tol

    def _no_lower_low_between(
        self,
        df: pd.DataFrame,
        idx1: int,
        idx2: int,
        low2: float,
    ) -> bool:
        """
        Bars strictly between idx1 and idx2: no low below Low2 (Higher Low pair).
        """
        if idx2 <= idx1 + 1:
            return True
        mid = df["low"].iloc[idx1 + 1 : idx2]
        if mid.empty:
            return True
        tol = 1e-9 * max(abs(low2), 1.0)
        return float(mid.min()) >= low2 - tol

    # ── Swing detection ───────────────────────────────────────────────────────

    def _find_swing_lows(self, lows: pd.Series) -> list[int]:
        """
        Return sorted list of confirmed swing low indices within the last
        `swing_lookback` bars.

        A bar at index i is a swing low when every one of the `swing_strength`
        bars immediately before and after it has a strictly higher low.  The
        last `swing_strength` bars are excluded because their right side is
        not yet confirmed.
        """
        n     = len(lows)
        s     = self._swing_strength
        end   = n - s - 1
        start = max(s, n - self._swing_lookback)

        if start > end:
            return []

        arr    = lows.values
        swings = []
        for i in range(start, end + 1):
            v = arr[i]
            if (
                all(arr[j] > v for j in range(i - s, i)) and
                all(arr[j] > v for j in range(i + 1, i + s + 1))
            ):
                swings.append(i)
        return swings

    def _edge_swing_low_indices(self, lows: pd.Series) -> list[int]:
        """
        Swing lows at the right edge: bar n-2 (confirmed by n-1) or n-1
        (left-only), so Low2 can be curr/prev bar.
        """
        n = len(lows)
        arr = lows.values
        s   = self._swing_strength
        out: list[int] = []
        if n < s + 1:
            return out

        # Bar n-2: one higher low to the right at n-1
        i = n - 2
        if i >= s:
            v = arr[i]
            if arr[n - 1] > v and all(arr[j] > v for j in range(i - s, i)):
                out.append(i)

        # Bar n-1: local low vs s bars to the left (no right confirmation yet)
        i = n - 1
        v = arr[i]
        if all(arr[j] > v for j in range(i - s, i)):
            out.append(i)

        return out

    def _edge_swing_high_indices(self, highs: pd.Series) -> list[int]:
        """
        Swing highs at the right edge: bar n-2 or n-1, mirror of lows.
        """
        n = len(highs)
        arr = highs.values
        s   = self._swing_strength
        out: list[int] = []
        if n < s + 1:
            return out

        i = n - 2
        if i >= s:
            v = arr[i]
            if arr[n - 1] < v and all(arr[j] < v for j in range(i - s, i)):
                out.append(i)

        i = n - 1
        v = arr[i]
        if all(arr[j] < v for j in range(i - s, i)):
            out.append(i)

        return out

    def _merge_swing_low_indices(self, lows: pd.Series) -> list[int]:
        base = self._find_swing_lows(lows)
        edge = self._edge_swing_low_indices(lows)
        return sorted(set(base + edge))

    def _merge_swing_high_indices(self, highs: pd.Series) -> list[int]:
        base = self._find_swing_highs(highs)
        edge = self._edge_swing_high_indices(highs)
        return sorted(set(base + edge))

    def _find_swing_highs(self, highs: pd.Series) -> list[int]:
        """
        Return sorted list of confirmed swing high indices within the last
        `swing_lookback` bars.  Mirror of _find_swing_lows.
        """
        n     = len(highs)
        s     = self._swing_strength
        end   = n - s - 1
        start = max(s, n - self._swing_lookback)

        if start > end:
            return []

        arr    = highs.values
        swings = []
        for i in range(start, end + 1):
            v = arr[i]
            if (
                all(arr[j] < v for j in range(i - s, i)) and
                all(arr[j] < v for j in range(i + 1, i + s + 1))
            ):
                swings.append(i)
        return swings

    # ── EMA confluence helper ─────────────────────────────────────────────────

    def _near_ema_zone(
        self,
        df: pd.DataFrame,
        idx: int,
        price: float,
        touch: str,           # "low" or "high"
    ) -> bool:
        """
        Return True when `price` at bar `idx` is within
        ema_confluence_atr × ATR of EMA34 or EMA89.
        """
        atr_val = float(df["atr"].iloc[idx])
        if atr_val <= 0:
            return True
        tol     = atr_val * self._ema_conf_atr
        ema34_v = float(df["ema34"].iloc[idx])
        ema89_v = float(df["ema89"].iloc[idx])
        return abs(price - ema34_v) <= tol or abs(price - ema89_v) <= tol

    # ── Candle strength (market-order mode) ───────────────────────────────────

    def _is_strong_candle(self, bar: pd.Series, direction: str) -> bool:
        """Body > strong_candle_ratio × total range, correct direction."""
        body   = abs(float(bar["close"]) - float(bar["open"]))
        range_ = float(bar["high"]) - float(bar["low"])
        if range_ <= 0:
            return False
        is_dir = (
            (direction == "BUY"  and bar["close"] > bar["open"]) or
            (direction == "SELL" and bar["close"] < bar["open"])
        )
        return is_dir and (body >= range_ * self._strong_ratio)

    # ── Pip conversion ────────────────────────────────────────────────────────

    def _price_to_pips(self, distance: float) -> float:
        """
        XAUUSD/XAGUSD : 1 pip = $0.10
        JPY pairs     : 1 pip = 0.01
        Standard Forex: 1 pip = 0.0001
        """
        sym = self.symbol.upper()
        if sym in ("XAUUSD", "XAGUSD"):
            return distance / 0.10
        if "JPY" in sym:
            return distance / 0.01
        return distance / 0.0001
