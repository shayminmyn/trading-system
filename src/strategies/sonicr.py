"""
SonicR PRO Strategy — PAC Dragon Channel + Value Zone (v3).

Ported from: SonicR PRO Signals Update version III.mq5
             by Dao Viet & Gemini AI

Signal Architecture (3 layers, checked in priority order)
----------------------------------------------------------
Layer 1 — PAC Breakout (buyCond / sellCond in MQ5)
  Price closes OUTSIDE the Dragon Channel (pac_high / pac_low)
  in the direction of EMA200. Strong candle + volume confirm.

Layer 2 — PAC Rejection (buyRej / sellRej in MQ5)
  Bar wick pierces the PAC boundary but CLOSES back on the
  correct side. Signals a "test and reject" from the channel.
  Confirmed by EMA89 position + strong candle + volume.

Layer 3a — Extension/Pullback BUY/SELL (trend path)
  Classic SonicR: price extends far from PAC, pulls back to EMA89
  zone, then re-crosses PAC-mid (EMA34 of close) — wave 5 setup.

Layer 3b — SW Oscillation BUY/SELL (sideway path)
  When EMA34 ≈ EMA89 (no clear trend), price oscillates around
  PAC-mid for 30+ bars without Dow continuation → mean-revert.

Indicators
----------
  pac_mid  (ema34) : EMA(ema_fast=34) of Close  — PAC median
  pac_high         : EMA(ema_fast=34) of High   — PAC upper band
  pac_low          : EMA(ema_fast=34) of Low    — PAC lower band
  ema89            : EMA(ema_slow=89) of Close  — trend anchor
  ema200           : EMA(ema_trend=200) of Close — macro trend filter
  atr              : ATR(atr_period=14)
  vol_ma           : SMA(vol_ma_len=60) of volume
  avg_body         : SMA(avg_body_len=20) of |close−open|

Parameters (config.yaml → strategies.SonicR)
--------------------------------------------
  ema_fast               : int   34     PAC channel / EMA short
  ema_slow               : int   89     Trend anchor EMA
  ema_trend              : int   200    Macro trend filter (EMA200)
  atr_period             : int   14
  atr_mult_far           : float 2.0    Far-value zone multiplier
  sl_buffer_atr          : float 0.3    Extra ATR padding on SL
  min_ema_separation_atr : float 0.5    Anti-sideway: min EMA34–89 spread
  slope_lookback         : int   5      Bars for EMA slope check
  pullback_lookback      : int   30     Window for pullback detection
  extension_lookback     : int   20     Window for extension detection

  ─── PAC signals ──────────────────────────────────────────────────────
  enable_pac_signals     : bool  True   Toggle Layer 1+2 signals
  vol_ma_len             : int   60     Volume MA period (MQ5: InpVolMALen)
  avg_body_len           : int   20     Avg-body period  (MQ5: InpAvgBodyLen)
  vol_ratio_breakout     : float 0.9    Min vol/vol_ma for breakout
  vol_ratio_rejection    : float 0.8    Min vol/vol_ma for rejection
  strong_body_ratio_avg  : float 0.8    Min body / avg_body ratio

  ─── Optimisation filters ─────────────────────────────────────────────
  ema89_touch_atr        : float 0.5    Touch-tolerance for EMA89 zone
  require_ema89_touch    : bool  True   Pullback must reach EMA89 zone
  require_ema89_rejection: bool  True   Entry bar rejected at EMA89
  require_strong_candle  : bool  True   Body > strong_candle_ratio × range
  strong_candle_ratio    : float 0.5    Minimum body / full-range ratio

  ─── Sideways Oscillation signal ──────────────────────────────────────
  enable_sw_signal       : bool  True
  sw_lookback            : int   30     Min bars to confirm sideway
  sw_min_crosses         : int   3      Min EMA34 crossings
  sw_max_range_atr       : float 4.0    Max range in ATR units

  min_rr                 : float 1.0    Minimum reward-to-risk ratio
"""

from __future__ import annotations

import pandas as pd
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange

from .base_strategy import BaseStrategy, Signal
from ..utils.logger import get_logger

logger = get_logger("sonicr")


class SonicRStrategy(BaseStrategy):
    """
    SonicR PRO v3 — PAC Dragon Channel + Value Zone strategy.
    Mirrors SonicR PRO Signals Update version III.mq5 logic.
    """

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        parameters: dict | None = None,
    ) -> None:
        super().__init__(symbol, timeframe, parameters)
        p = self.parameters

        # ── Core EMA / ATR ────────────────────────────────────────────────────
        self._ema_fast: int            = p.get("ema_fast", 34)
        self._ema_slow: int            = p.get("ema_slow", 89)
        self._ema_trend: int           = p.get("ema_trend", 200)
        self._atr_period: int          = p.get("atr_period", 14)

        # ── Pattern detection ─────────────────────────────────────────────────
        self._atr_mult_far: float      = p.get("atr_mult_far", 2.0)
        self._sl_buffer_atr: float     = p.get("sl_buffer_atr", 0.3)
        self._min_sep_atr: float       = p.get("min_ema_separation_atr", 0.5)
        self._slope_lookback: int      = p.get("slope_lookback", 5)
        self._pb_lookback: int         = p.get("pullback_lookback", 30)
        self._ext_lookback: int        = p.get("extension_lookback", 20)

        # ── PAC signal parameters (from MQ5) ──────────────────────────────────
        self._enable_pac: bool         = bool(p.get("enable_pac_signals", True))
        self._vol_ma_len: int          = p.get("vol_ma_len", 60)
        self._avg_body_len: int        = p.get("avg_body_len", 20)
        self._vol_ratio_bo: float      = p.get("vol_ratio_breakout", 0.9)
        self._vol_ratio_rej: float     = p.get("vol_ratio_rejection", 0.8)
        self._strong_body_avg: float   = p.get("strong_body_ratio_avg", 0.8)

        # ── Optimisation filters ──────────────────────────────────────────────
        self._ema89_touch_atr: float   = p.get("ema89_touch_atr", 0.5)
        self._req_ema89_touch: bool    = bool(p.get("require_ema89_touch", True))
        self._req_ema89_rej: bool      = bool(p.get("require_ema89_rejection", True))
        self._req_strong_candle: bool  = bool(p.get("require_strong_candle", True))
        self._strong_ratio: float      = p.get("strong_candle_ratio", 0.5)

        # ── Sideways oscillation ──────────────────────────────────────────────
        self._enable_sw: bool          = bool(p.get("enable_sw_signal", True))
        self._sw_lookback: int         = p.get("sw_lookback", 30)
        self._sw_min_crosses: int      = p.get("sw_min_crosses", 3)
        self._sw_max_range_atr: float  = p.get("sw_max_range_atr", 4.0)

        # ── RR ────────────────────────────────────────────────────────────────
        self._min_rr: float            = p.get("min_rr", 1.0)

        # ── Limit-order entry optimisation ────────────────────────────────────
        # When True: instead of entering at market on signal bar close,
        # place a limit order at pac_mid (EMA34) and wait up to
        # limit_expiry_bars for price to retrace there.
        # This gives a tighter SL (measured from pac_mid, not close) and
        # a better-quality entry in the "value zone".
        self._limit_entry: bool        = bool(p.get("limit_entry", True))
        self._limit_expiry: int        = int(p.get("limit_expiry_bars", 5))

        self._min_bars = max(
            self._ema_trend,
            self._ema_slow + self._pb_lookback + self._ext_lookback,
        ) + 10

    # ── Indicators ────────────────────────────────────────────────────────────

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        # PAC Dragon Channel — EMA(34) applied to High, Low, Close
        df["pac_mid"]  = EMAIndicator(close=df["close"], window=self._ema_fast).ema_indicator()
        df["ema34"]    = df["pac_mid"]          # alias for backward compatibility
        df["pac_high"] = EMAIndicator(close=df["high"],  window=self._ema_fast).ema_indicator()
        df["pac_low"]  = EMAIndicator(close=df["low"],   window=self._ema_fast).ema_indicator()

        # Trend EMAs
        df["ema89"]  = EMAIndicator(close=df["close"], window=self._ema_slow).ema_indicator()
        df["ema200"] = EMAIndicator(close=df["close"], window=self._ema_trend).ema_indicator()

        # Volatility
        df["atr"] = AverageTrueRange(
            high=df["high"], low=df["low"], close=df["close"],
            window=self._atr_period,
        ).average_true_range()

        # Volume MA (tick volume proxy)
        df["vol_ma"] = df["volume"].rolling(window=self._vol_ma_len, min_periods=1).mean()

        # Average candle body
        df["avg_body"] = (
            (df["close"] - df["open"]).abs()
            .rolling(window=self._avg_body_len, min_periods=1)
            .mean()
        )
        return df

    # ── Signal generation ─────────────────────────────────────────────────────

    def generate_signal(self, df: pd.DataFrame) -> Signal:
        required = ["pac_mid", "pac_high", "pac_low", "ema89", "ema200", "atr"]
        df = df.dropna(subset=required).reset_index(drop=True)
        if len(df) < max(self._pb_lookback + self._ext_lookback + 5, self._sw_lookback + 5):
            return self._no_signal()

        curr = df.iloc[-1]
        prev = df.iloc[-2]

        if curr["atr"] <= 0 or pd.isna(curr["atr"]):
            return self._no_signal()

        # ── Layer 1+2: PAC breakout / rejection (from MQ5) ────────────────────
        if self._enable_pac:
            sig = self._check_pac_breakout(curr, prev)
            if sig is not None:
                return sig
            sig = self._check_pac_rejection(curr, prev)
            if sig is not None:
                return sig

        # ── Layer 3: route by trend condition (EMA34 vs EMA89 spread) ─────────
        if self._is_trending(df, curr):
            sig = self._check_buy(df, curr, prev)
            if sig is not None:
                return sig
            sig = self._check_sell(df, curr, prev)
            if sig is not None:
                return sig
        elif self._enable_sw:
            sig = self._check_ema34_oscillation(df, curr, prev)
            if sig is not None:
                return sig

        return self._no_signal()

    # ── Layer 1: PAC Breakout ─────────────────────────────────────────────────

    def _check_pac_breakout(
        self, curr: pd.Series, prev: pd.Series
    ) -> Signal | None:
        """
        MQ5 buyCond / sellCond:
          BUY : prev close ≤ pac_high AND curr close > pac_high
                AND close > ema200 AND vol_ok AND strong_body
          SELL: prev close ≥ pac_low  AND curr close < pac_low
                AND close < ema200 AND vol_ok AND strong_body

        SL: opposite PAC band ± sl_buffer_atr × ATR
        """
        pac_high = curr["pac_high"]
        pac_low  = curr["pac_low"]
        ema200   = curr["ema200"]
        atr      = curr["atr"]

        if not self._is_volume_ok(curr, self._vol_ratio_bo):
            return None
        if not self._is_strong_body_avg(curr):
            return None

        # ── BUY breakout ──────────────────────────────────────────────────────
        if prev["close"] <= prev["pac_high"] and curr["close"] > pac_high:
            if curr["close"] <= ema200:
                return None

            sl_level = float(pac_low) - atr * self._sl_buffer_atr
            return self._finalize_signal(
                "BUY", sl_level, curr,
                f"PAC-Breakout BUY | pac_high={pac_high:.5f} ema200={ema200:.5f}",
            )

        # ── SELL breakout ─────────────────────────────────────────────────────
        if prev["close"] >= prev["pac_low"] and curr["close"] < pac_low:
            if curr["close"] >= ema200:
                return None

            sl_level = float(pac_high) + atr * self._sl_buffer_atr
            return self._finalize_signal(
                "SELL", sl_level, curr,
                f"PAC-Breakout SELL | pac_low={pac_low:.5f} ema200={ema200:.5f}",
            )

        return None

    # ── Layer 2: PAC Rejection ────────────────────────────────────────────────

    def _check_pac_rejection(
        self, curr: pd.Series, prev: pd.Series
    ) -> Signal | None:
        """
        MQ5 buyRej / sellRej — wick pierces PAC but close snaps back.

          BUY rejection  : low < pac_high AND close > pac_high
                           AND close > ema89 AND bullish candle
          SELL rejection : high > pac_low  AND close < pac_low
                           AND close < ema89 AND bearish candle

        SL: bar wick extreme ± sl_buffer_atr × ATR
        """
        pac_high = curr["pac_high"]
        pac_low  = curr["pac_low"]
        ema89    = curr["ema89"]
        atr      = curr["atr"]

        if not self._is_volume_ok(curr, self._vol_ratio_rej):
            return None
        if not self._is_strong_body_avg(curr):
            return None

        # ── BUY rejection ─────────────────────────────────────────────────────
        if curr["low"] < pac_high and curr["close"] > pac_high:
            if not (curr["close"] > ema89 and curr["close"] > curr["open"]):
                return None

            sl_level = float(curr["low"]) - atr * self._sl_buffer_atr
            return self._finalize_signal(
                "BUY", sl_level, curr,
                f"PAC-Rejection BUY | pac_high={pac_high:.5f} ema89={ema89:.5f}",
            )

        # ── SELL rejection ────────────────────────────────────────────────────
        if curr["high"] > pac_low and curr["close"] < pac_low:
            if not (curr["close"] < ema89 and curr["close"] < curr["open"]):
                return None

            sl_level = float(curr["high"]) + atr * self._sl_buffer_atr
            return self._finalize_signal(
                "SELL", sl_level, curr,
                f"PAC-Rejection SELL | pac_low={pac_low:.5f} ema89={ema89:.5f}",
            )

        return None

    # ── Layer 3a: Extension / Pullback BUY ───────────────────────────────────

    def _check_buy(
        self,
        df: pd.DataFrame,
        curr: pd.Series,
        prev: pd.Series,
    ) -> Signal | None:
        """
        BUY: uptrend → price extends above PAC → pulls back to EMA89 zone
             → re-crosses pac_mid (EMA34 of close) upward.
        """
        pac_mid = curr["pac_mid"]
        pac_low = curr["pac_low"]
        ema89   = curr["ema89"]
        atr     = curr["atr"]

        if pac_mid <= ema89:
            return None

        # EMA89 slope upward
        if not self._ema89_sloping_up(df):
            return None

        # Trigger: close crosses above pac_mid
        if not (curr["close"] > pac_mid and prev["close"] <= pac_mid):
            return None

        # Strong bullish candle (optimisation filter)
        if self._req_strong_candle and not self._is_strong_candle(curr, "BUY"):
            return None

        # Entry bar fully above EMA89 (rejected from below EMA89)
        if self._req_ema89_rej and curr["low"] <= ema89:
            return None

        n        = len(df)
        pb_start = max(0, n - self._pb_lookback)
        pb_window = df.iloc[pb_start:]

        ema_touch = pb_window["low"] <= (pb_window["pac_mid"] + atr * 0.5)
        if not ema_touch.any():
            return None

        first_touch_pos = int(ema_touch.values.argmax())
        pullback_slice  = pb_window.iloc[first_touch_pos:]
        pullback_low    = float(pullback_slice["low"].min())

        # Pullback reached EMA89 zone
        if self._req_ema89_touch:
            touched = pullback_slice["low"] <= (
                pullback_slice["ema89"] + self._ema89_touch_atr * atr
            )
            if not touched.any():
                return None

        # No firm close below EMA89
        if (pullback_slice["close"] < pullback_slice["ema89"] - atr * 0.5).any():
            return None

        # Extension before pullback
        ext_end   = pb_start + first_touch_pos
        ext_start = max(0, ext_end - self._ext_lookback)
        ext_slice = df.iloc[ext_start:ext_end]

        if ext_slice.empty:
            return None

        far_zone = ext_slice["high"] > ext_slice["pac_mid"] + self._atr_mult_far * ext_slice["atr"]
        if not far_zone.any():
            return None

        # No new lower-low
        if pullback_low < float(ext_slice["low"].min()) * 0.9990:
            return None

        sl_level    = min(pullback_low, float(ema89)) - atr * self._sl_buffer_atr
        recent_high = float(ext_slice["high"].max())
        return self._finalize_signal(
            "BUY", sl_level, curr,
            f"SonicR BUY★ | pac_mid={pac_mid:.5f} ema89={ema89:.5f} "
            f"PB_low={pullback_low:.5f}",
            rr_ref_price=recent_high,
        )

    # ── Layer 3a: Extension / Pullback SELL ──────────────────────────────────

    def _check_sell(
        self,
        df: pd.DataFrame,
        curr: pd.Series,
        prev: pd.Series,
    ) -> Signal | None:
        """
        SELL: downtrend → price extends below PAC → corrections to EMA89 zone
              → re-crosses pac_mid downward.
        """
        pac_mid = curr["pac_mid"]
        pac_high = curr["pac_high"]
        ema89   = curr["ema89"]
        atr     = curr["atr"]

        if pac_mid >= ema89:
            return None

        if not self._ema89_sloping_down(df):
            return None

        if not (curr["close"] < pac_mid and prev["close"] >= pac_mid):
            return None

        if self._req_strong_candle and not self._is_strong_candle(curr, "SELL"):
            return None

        if self._req_ema89_rej and curr["high"] >= ema89:
            return None

        n          = len(df)
        pb_start   = max(0, n - self._pb_lookback)
        corr_window = df.iloc[pb_start:]

        ema_touch = corr_window["high"] >= (corr_window["pac_mid"] - atr * 0.5)
        if not ema_touch.any():
            return None

        first_touch_pos  = int(ema_touch.values.argmax())
        correction_slice = corr_window.iloc[first_touch_pos:]
        correction_high  = float(correction_slice["high"].max())

        if self._req_ema89_touch:
            touched = correction_slice["high"] >= (
                correction_slice["ema89"] - self._ema89_touch_atr * atr
            )
            if not touched.any():
                return None

        if (correction_slice["close"] > correction_slice["ema89"] + atr * 0.5).any():
            return None

        ext_end   = pb_start + first_touch_pos
        ext_start = max(0, ext_end - self._ext_lookback)
        ext_slice = df.iloc[ext_start:ext_end]

        if ext_slice.empty:
            return None

        far_zone = ext_slice["low"] < ext_slice["pac_mid"] - self._atr_mult_far * ext_slice["atr"]
        if not far_zone.any():
            return None

        if correction_high > float(ext_slice["high"].max()) * 1.0010:
            return None

        sl_level   = max(correction_high, float(ema89)) + atr * self._sl_buffer_atr
        recent_low = float(ext_slice["low"].min())
        return self._finalize_signal(
            "SELL", sl_level, curr,
            f"SonicR SELL★ | pac_mid={pac_mid:.5f} ema89={ema89:.5f} "
            f"COR_high={correction_high:.5f}",
            rr_ref_price=recent_low,
        )

    # ── Layer 3b: SW Oscillation ──────────────────────────────────────────────

    def _check_ema34_oscillation(
        self,
        df: pd.DataFrame,
        curr: pd.Series,
        prev: pd.Series,
    ) -> Signal | None:
        """
        Sideways mean-reversion at PAC-mid.
        Condition: price oscillated around PAC-mid for sw_lookback bars
                   without creating a Dow continuation structure.
        Entry: cross through pac_mid → trade toward opposite range bound.
        """
        n = len(df)
        if n < self._sw_lookback + 5:
            return None

        pac_mid = curr["pac_mid"]
        atr     = curr["atr"]

        sw_window = df.iloc[-(self._sw_lookback + 1):-1].copy()

        if not self._is_sideways_no_dow(sw_window, atr):
            return None

        range_high = float(sw_window["high"].max())
        range_low  = float(sw_window["low"].min())

        # BUY: bounce upward through pac_mid
        if curr["close"] > pac_mid and prev["close"] <= pac_mid:
            if self._req_strong_candle and not self._is_strong_candle(curr, "BUY"):
                return None

            sl_level = range_low - atr * self._sl_buffer_atr
            return self._finalize_signal(
                "BUY", sl_level, curr,
                f"SonicR SW-BUY | pac_mid={pac_mid:.5f} "
                f"Range=[{range_low:.5f}–{range_high:.5f}]",
                rr_ref_price=range_high,
            )

        # SELL: rejection downward through pac_mid
        if curr["close"] < pac_mid and prev["close"] >= pac_mid:
            if self._req_strong_candle and not self._is_strong_candle(curr, "SELL"):
                return None

            sl_level = range_high + atr * self._sl_buffer_atr
            return self._finalize_signal(
                "SELL", sl_level, curr,
                f"SonicR SW-SELL | pac_mid={pac_mid:.5f} "
                f"Range=[{range_low:.5f}–{range_high:.5f}]",
                rr_ref_price=range_low,
            )

        return None

    # ── PAC / volume / body helpers ───────────────────────────────────────────

    def _is_volume_ok(self, bar: pd.Series, min_ratio: float) -> bool:
        """
        Volume filter — mirrors MQ5 volRatio check.
        Returns True when tick_volume / vol_ma >= min_ratio.
        Gracefully passes when vol_ma is unavailable (NaN / 0).
        """
        vol_ma = bar.get("vol_ma", 0.0)
        if pd.isna(vol_ma) or vol_ma <= 0:
            return True          # no volume data → don't block
        vol = bar.get("volume", 0.0)
        if pd.isna(vol) or vol <= 0:
            return True
        return (vol / vol_ma) >= min_ratio

    def _is_strong_body_avg(self, bar: pd.Series) -> bool:
        """
        MQ5 isStrongBody: current body >= strong_body_ratio_avg × avg_body.
        Falls back to True when avg_body is unavailable.
        """
        avg_body = bar.get("avg_body", 0.0)
        if pd.isna(avg_body) or avg_body <= 0:
            return True
        body = abs(float(bar["close"]) - float(bar["open"]))
        return body >= self._strong_body_avg * avg_body

    # ── Optimisation helpers ──────────────────────────────────────────────────

    def _is_trending(self, df: pd.DataFrame, curr: pd.Series) -> bool:
        """Anti-sideway: EMA34–EMA89 spread ≥ min_ema_separation_atr × ATR."""
        spread = abs(curr["pac_mid"] - curr["ema89"])
        return spread >= self._min_sep_atr * curr["atr"]

    def _ema89_sloping_up(self, df: pd.DataFrame) -> bool:
        if len(df) < self._slope_lookback + 1:
            return True
        return float(df["ema89"].iloc[-1]) > float(df["ema89"].iloc[-(self._slope_lookback + 1)])

    def _ema89_sloping_down(self, df: pd.DataFrame) -> bool:
        if len(df) < self._slope_lookback + 1:
            return True
        return float(df["ema89"].iloc[-1]) < float(df["ema89"].iloc[-(self._slope_lookback + 1)])

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

    def _is_sideways_no_dow(self, window: pd.DataFrame, atr: float) -> bool:
        """
        True when:
        1. EMA34 crossed ≥ sw_min_crosses times (oscillation confirmed)
        2. Range ≤ sw_max_range_atr × ATR (tight range)
        3. No Dow continuation (no HH+HL or LH+LL across halves)
        """
        if len(window) < 6:
            return False

        above = window["close"] > window["pac_mid"]
        if int((above != above.shift(1)).sum()) < self._sw_min_crosses:
            return False

        total_range = float(window["high"].max() - window["low"].min())
        if total_range > self._sw_max_range_atr * atr:
            return False

        mid = len(window) // 2
        f, s = window.iloc[:mid], window.iloc[mid:]
        f_h, s_h = float(f["high"].max()), float(s["high"].max())
        f_l, s_l = float(f["low"].min()),  float(s["low"].min())

        tol = 0.001
        if (s_h > f_h * (1 + tol)) and (s_l > f_l * (1 + tol)):
            return False    # bullish Dow
        if (s_h < f_h * (1 - tol)) and (s_l < f_l * (1 - tol)):
            return False    # bearish Dow
        return True

    # ── Pip conversion ────────────────────────────────────────────────────────

    def _price_to_pips(self, distance: float) -> float:
        """
        XAUUSD/XAGUSD : 1 pip = $0.10  (1 USD move = 10 pips)
        JPY pairs     : 1 pip = 0.01
        Standard Forex: 1 pip = 0.0001
        """
        sym = self.symbol.upper()
        if sym in ("XAUUSD", "XAGUSD"):
            return distance / 0.10
        if "JPY" in sym:
            return distance / 0.01
        return distance / 0.0001

    # ── Limit-order entry helper ──────────────────────────────────────────────

    def _finalize_signal(
        self,
        action: str,
        sl_level: float,
        curr: pd.Series,
        notes: str,
        rr_ref_price: float | None = None,
    ) -> Signal | None:
        """
        Build the final Signal using either market or limit-order entry.

        Market mode  (limit_entry=False):
            entry = curr["close"]  (enter on next bar's open via backtest)

        Limit mode   (limit_entry=True):
            entry = curr["pac_mid"]  (EMA34 — the "value zone" limit price)
            The backtester will place a limit order at pac_mid and fill only
            when price retraces there within limit_expiry_bars bars.
            SL is measured from pac_mid → tighter SL, better R:R.

        Parameters
        ----------
        action        : "BUY" or "SELL"
        sl_level      : absolute SL price (same regardless of entry mode)
        curr          : last bar Series (must contain pac_mid, close)
        notes         : signal note string
        rr_ref_price  : optional reference price to check min_rr from entry;
                        pass the TP reference (e.g., recent_high / recent_low)
        """
        pac_mid = float(curr["pac_mid"])
        entry   = pac_mid if self._limit_entry else float(curr["close"])

        if action == "BUY":
            sl_distance = entry - sl_level
        else:
            sl_distance = sl_level - entry

        if sl_distance <= 0:
            return None

        # Optional R:R check from the chosen entry price
        if rr_ref_price is not None:
            if action == "BUY":
                rr = (rr_ref_price - entry) / sl_distance
            else:
                rr = (entry - rr_ref_price) / sl_distance
            if rr < self._min_rr:
                logger.debug(
                    "SonicR %s skipped — RR=%.2f < %.1f (entry=%.5f, sl=%.5f, ref=%.5f)",
                    action, rr, self._min_rr, entry, sl_level, rr_ref_price,
                )
                return None

        sl_pips = self._price_to_pips(sl_distance)
        if sl_pips <= 0:
            return None

        sig = self._make_signal(action, entry, sl_pips, notes)
        if self._limit_entry:
            sig.limit_expiry_bars = self._limit_expiry
        return sig
