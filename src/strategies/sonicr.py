"""
SonicR PRO Strategy — PAC Dragon Channel + Value Zone (v2).

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
  rejection_priority     : bool  False  Check Rejection BEFORE Breakout

  ─── Optimisation filters ─────────────────────────────────────────────
  ema89_touch_atr              : float 0.5    Touch-tolerance for EMA89 zone
  require_ema89_touch          : bool  True   Pullback must reach EMA89 zone
  require_ema89_rejection      : bool  True   Entry bar rejected at EMA89
  require_strong_candle        : bool  True   Body > strong_candle_ratio × range
  strong_candle_ratio          : float 0.5    Minimum body / full-range ratio
  breakout_max_ema200_dist_pips: float 0.0    Max entry→EMA200 pips for Breakout (0=off)
  rejection_extra_sl_atr       : float 0.0    Extra ATR buffer on top of sl_buffer_atr for Rejection SL
  max_sl_pips                  : float 0.0    Hard cap on SL pips for any signal (0=off)

  ─── Trailing Break-even ──────────────────────────────────────────────
  breakeven_at_r         : float 0.0    Move SL to entry when profit ≥ N×SL (0=off)

  ─── Opt-1: EMA Dual Slope Filter ────────────────────────────────────
  require_ema_slope      : bool  False  Cả EMA34 và EMA89 phải có độ dốc rõ ràng
  min_slope_pips_per_bar : float 0.5    Ngưỡng tối thiểu pips/nến (tương đương ~30°)

  ─── Opt-2: Swing-based ATR SL ───────────────────────────────────────
  use_swing_sl           : bool  False  Dùng Swing High/Low + ATR thay vì PAC band
  swing_sl_lookback      : int   10     Số nến nhìn lại để tìm Swing High/Low
  swing_sl_atr_mult      : float 1.5    Khoảng đệm = N × ATR ngoài Swing point

  ─── Opt-3: Three-Bar Confirmation (PAC Breakout only) ───────────────
  require_bo_confirmation: bool  False  Bật quy tắc xác nhận nến Breakout
  bo_marubozu_ratio      : float 0.8    Thân nến ≥ N×range → chấp nhận vào ngay

  ─── Opt-4: ATR-based SL (SL = Entry ± N×ATR) ───────────────────────
  use_atr_sl             : bool  False  Thay thế tất cả SL bằng Entry ± N×ATR
  atr_sl_mult            : float 1.5    SL = Entry ± atr_sl_mult × ATR

  ─── Opt-5: Close-confirmation touch (no wick entry) ─────────────────
  use_close_for_ema_touch: bool  False  Dùng close thay low/high khi kiểm tra EMA touch
                                         Tránh vào lệnh chỉ vì râu nến chạm EMA (M15)

  ─── Opt-6: EMA200 global trend filter ───────────────────────────────
  require_ema200_trend   : bool  False  BUY chỉ khi close > EMA200, SELL ngược lại
                                         Áp dụng cho TẤT CẢ layers (không chỉ Breakout)

  ─── Opt-7: Max SL relative to ATR ──────────────────────────────────
  max_sl_atr_mult        : float 0.0    Bỏ qua lệnh nếu SL > N×ATR (0=tắt)
                                         Ví dụ 5.0 = bỏ lệnh có SL > 5×ATR

  ─── Opt-8: Partial close / TP1 scalp ────────────────────────────────
  partial_close_at_r     : float 0.0    Đóng partial_close_ratio khi lời ≥ N×SL (0=tắt)
  partial_close_ratio    : float 0.5    Tỷ lệ khối lượng đóng (0.5 = 50%)
  partial_trail_pips     : float 5.0    Dời SL thêm N pips sau partial close

  ─── Opt-9: Phiên giao dịch (Time Filter) ────────────────────────────
  allowed_hours_utc      : list  []     Giờ UTC được phép vào lệnh (rỗng = mọi giờ)
                                         Ví dụ [7,8,...,16,19,20,21] = EU+US session

  ─── Opt-10: ADX Trend Strength Filter ───────────────────────────────
  adx_period             : int   14     ADX lookback period
  adx_filter_min         : float 0.0   Bỏ qua khi ADX < N (0=tắt). Khuyến nghị: 25

  ─── Opt-11: Linear Regression Slope Filter (EMA89) ──────────────────
  use_linreg_slope       : bool  False  Dùng hồi quy tuyến tính thay vì first/last slope
  linreg_slope_lookback  : int   10     Số nến cho hồi quy (M5: 20, H1: 8)
  linreg_slope_thresh    : float 0.05   |slope| < thresh (pips/bar) → sideway → bỏ qua

  ─── Opt-12: Dragon Tunnel Zigzag Filter ─────────────────────────────
  dragon_zigzag_filter   : bool  False  Bật bộ lọc zig-zag qua dải EMA34 High/Low
  dragon_zigzag_lookback : int   10     Số nến nhìn lại
  dragon_zigzag_max_crosses: int 3      Nếu giá đổi vùng (above/inside/below) ≥ N lần → sideway

  ─── Opt-13: Absolute EMA Gap Filter ─────────────────────────────────
  min_ema_gap_pips       : float 0.0    Khoảng cách tuyệt đối (pips) tối thiểu giữa EMA34 (pac_mid)
                                         và EMA89. Nếu gap < N pips → EMA đi sát nhau → sideway → skip
                                         Ví dụ: 50 pips cho XAUUSD M5 (0=tắt)

  ─── Opt-14: Minimum SL Width ────────────────────────────────────────
  min_sl_pips            : float 0.0    Chiều rộng SL tối thiểu tính bằng pips. Nếu SL tính được
                                         < N pips, SL sẽ được kéo ra đến N pips (dùng Swing H/L làm
                                         cơ sở nếu nó rộng hơn). Đảm bảo SL đủ "thở" trên M5.
                                         Ví dụ: 150 pips cho XAUUSD M5 (0=tắt)

  ─── Opt-15: Higher Timeframe (HTF) EMA Bias Filter ─────────────────
  htf_ema_filter         : bool  False  Bật bộ lọc xu hướng khung lớn. Chiều tín hiệu M5 phải
                                         khớp với xu hướng H1 (hoặc khung được cấu hình).
                                         BUY  chỉ khi EMA34(H1) > EMA89(H1)
                                         SELL chỉ khi EMA34(H1) < EMA89(H1)
                                         Loại bỏ các lệnh ngược xu hướng cấu trúc lớn hơn.
  htf_resample           : str   "1h"   Tần số resample từ TF hiện tại lên khung lớn
                                         (pandas offset alias: "1h"=H1, "4h"=H4, "1D"=D1)
  htf_ema_fast           : int   34     EMA fast period trên khung lớn (default = EMA34)
  htf_ema_slow           : int   89     EMA slow period trên khung lớn (default = EMA89)
  max_entry_pac_dist_atr : float 0.0    (Layer 3a BUY★/SELL★) Nếu nến trigger đóng xa
                                         pac_mid hơn N×ATR → bỏ qua (entry xấu do bùng nổ).
                                         Ví dụ: 1.5 = bỏ nếu |close − pac_mid| > 1.5×ATR.
                                         (0=tắt)

  htf_require_close_vs_ema: bool False  Nếu True, ngoài trend direction còn kiểm tra thêm:
                                         BUY  chỉ khi close(HTF) > EMA34(HTF)  (giá trên PAC mid H1)
                                         SELL chỉ khi close(HTF) < EMA34(HTF)  (giá dưới PAC mid H1)
                                         Tránh vào BUY khi H1 đang pullback về dưới EMA34.

  ─── Sideways Oscillation signal ──────────────────────────────────────
  enable_sw_signal       : bool  True
  sw_lookback            : int   30     Min bars to confirm sideway
  sw_min_crosses         : int   3      Min EMA34 crossings
  sw_max_range_atr       : float 4.0    Max range in ATR units

  min_rr                 : float 1.0    Minimum reward-to-risk ratio
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from ta.trend import ADXIndicator

from ..utils.ema_mt5 import ema_mt5
from ta.volatility import AverageTrueRange

from .base_strategy import BaseStrategy, Signal
from ..utils.logger import get_logger

logger = get_logger("sonicr")


class SonicRStrategy(BaseStrategy):
    """
    SonicR PRO v2 — PAC Dragon Channel + Value Zone strategy.
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
        # Breakout: skip if entry too far from EMA200 (avoids "đu đỉnh/đáy" on hot trend)
        self._bo_max_ema200_dist_pips: float = float(p.get("breakout_max_ema200_dist_pips", 0.0))
        # Rejection: extra ATR buffer on SL to absorb market noise
        self._rej_extra_sl_atr: float  = float(p.get("rejection_extra_sl_atr", 0.0))
        # Hard cap on SL width for any signal
        self._max_sl_pips: float       = float(p.get("max_sl_pips", 0.0))
        # PAC check order: True = Rejection first, False = Breakout first (default)
        self._rejection_priority: bool = bool(p.get("rejection_priority", False))

        # ── Trailing Break-even ───────────────────────────────────────────────
        self._breakeven_at_r: float    = float(p.get("breakeven_at_r", 0.0))

        # ── Opt-1: EMA Dual Slope Filter ─────────────────────────────────────
        self._require_ema_slope: bool       = bool(p.get("require_ema_slope", False))
        self._min_slope_pips_per_bar: float = float(p.get("min_slope_pips_per_bar", 0.5))

        # ── Opt-2: Swing-based ATR SL ─────────────────────────────────────────
        self._use_swing_sl: bool         = bool(p.get("use_swing_sl", False))
        self._swing_sl_lookback: int     = max(3, int(p.get("swing_sl_lookback", 10)))
        self._swing_sl_atr_mult: float   = float(p.get("swing_sl_atr_mult", 1.5))

        # ── Opt-3: Three-Bar Confirmation ─────────────────────────────────────
        self._require_bo_confirmation: bool = bool(p.get("require_bo_confirmation", False))
        self._bo_marubozu_ratio: float      = float(p.get("bo_marubozu_ratio", 0.8))

        # ── Opt-4: ATR-based SL (Entry ± N×ATR) ──────────────────────────────
        self._use_atr_sl: bool         = bool(p.get("use_atr_sl", False))
        self._atr_sl_mult: float       = float(p.get("atr_sl_mult", 1.5))

        # ── Opt-5: Close-confirmation touch (no wick entry) ───────────────────
        self._use_close_for_ema_touch: bool = bool(p.get("use_close_for_ema_touch", False))

        # ── Opt-6: EMA200 global trend filter ────────────────────────────────
        self._require_ema200_trend: bool = bool(p.get("require_ema200_trend", False))

        # ── Opt-7: Max SL relative to ATR ─────────────────────────────────────
        self._max_sl_atr_mult: float = float(p.get("max_sl_atr_mult", 0.0))

        # ── Opt-8: Partial close / TP1 scalp ─────────────────────────────────
        self._partial_close_at_r: float  = float(p.get("partial_close_at_r", 0.0))
        self._partial_close_ratio: float = float(p.get("partial_close_ratio", 0.5))
        self._partial_trail_pips: float  = float(p.get("partial_trail_pips", 5.0))

        # ── Opt-9: Phiên giao dịch (Time Filter) ─────────────────────────────
        _raw_hours = p.get("allowed_hours_utc", [])
        self._allowed_hours_utc: set[int] = (
            set(int(h) for h in _raw_hours) if _raw_hours else set()
        )

        # ── Opt-10: ADX Filter ────────────────────────────────────────────────
        self._adx_period: int        = max(2, int(p.get("adx_period", 14)))
        self._adx_filter_min: float  = float(p.get("adx_filter_min", 0.0))

        # ── Opt-11: Linear Regression Slope Filter ────────────────────────────
        self._use_linreg_slope: bool       = bool(p.get("use_linreg_slope", False))
        self._linreg_slope_lookback: int   = max(5, int(p.get("linreg_slope_lookback", 10)))
        self._linreg_slope_thresh: float   = float(p.get("linreg_slope_thresh", 0.05))

        # ── Opt-12: Dragon Tunnel Zigzag Filter ───────────────────────────────
        self._dragon_zigzag_filter: bool    = bool(p.get("dragon_zigzag_filter", False))
        self._dragon_zigzag_lookback: int   = max(3, int(p.get("dragon_zigzag_lookback", 10)))
        self._dragon_zigzag_max_crosses: int = max(1, int(p.get("dragon_zigzag_max_crosses", 3)))

        # ── Opt-13: Absolute EMA Gap Filter ───────────────────────────────────
        # Khoảng cách pips tuyệt đối giữa EMA34 (pac_mid) và EMA89. 0 = tắt.
        self._min_ema_gap_pips: float = float(p.get("min_ema_gap_pips", 0.0))

        # ── Opt-14: Minimum SL Width ───────────────────────────────────────────
        # SL tối thiểu tính bằng pips; kéo ra nếu SL tính được hẹp hơn. 0 = tắt.
        self._min_sl_pips: float = float(p.get("min_sl_pips", 0.0))

        # ── Opt-15: Higher Timeframe (HTF) EMA Bias Filter ─────────────────────
        # Resample TF hiện tại → khung lớn hơn, kiểm tra EMA34 vs EMA89.
        # Chỉ BUY khi EMA34(HTF) > EMA89(HTF); chỉ SELL khi ngược lại.
        self._htf_ema_filter: bool = bool(p.get("htf_ema_filter", False))
        self._htf_resample: str    = str(p.get("htf_resample", "1h"))
        self._htf_ema_fast: int    = max(2, int(p.get("htf_ema_fast", 34)))
        self._htf_ema_slow: int    = max(2, int(p.get("htf_ema_slow", 89)))
        # ── Opt-16: Max entry distance from pac_mid (Layer 3a only) ───────────────
        # Nếu nến trigger đóng quá xa pac_mid (bùng nổ mạnh), entry sẽ xấu.
        # Giá trị 0 = tắt; 1.5 = bỏ nếu close xa pac_mid hơn 1.5×ATR.
        self._max_entry_pac_dist_atr: float = float(p.get("max_entry_pac_dist_atr", 0.0))

        # Khi True: ngoài EMA34>EMA89 (H1 trend), còn yêu cầu close(H1) nằm đúng bên EMA34(H1)
        #   BUY  → close(H1) > EMA34(H1)  (giá đang trên PAC mid H1 — vùng sức mạnh)
        #   SELL → close(H1) < EMA34(H1)  (giá đang dưới PAC mid H1)
        self._htf_require_close_vs_ema: bool = bool(p.get("htf_require_close_vs_ema", False))

        # ── Sideways oscillation ──────────────────────────────────────────────
        self._enable_sw: bool          = bool(p.get("enable_sw_signal", True))
        self._sw_lookback: int         = p.get("sw_lookback", 30)
        self._sw_min_crosses: int      = p.get("sw_min_crosses", 3)
        self._sw_max_range_atr: float  = p.get("sw_max_range_atr", 4.0)

        # ── RR ────────────────────────────────────────────────────────────────
        self._min_rr: float            = p.get("min_rr", 1.0)

        # ── Limit-order entry ──────────────────────────────────────────────────
        # When True, signals emit a LIMIT order at EMA34 (pac_mid) instead of
        # entering at market on the next bar open.  The backtest engine waits
        # up to limit_expiry_bars bars for the fill; unfilled orders expire.
        self._limit_entry: bool        = bool(p.get("limit_entry", True))
        self._limit_expiry: int        = p.get("limit_expiry_bars", 10)

        self._min_bars = max(
            self._ema_trend,
            self._ema_slow + self._pb_lookback + self._ext_lookback,
        ) + 10

    # ── Indicators ────────────────────────────────────────────────────────────

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        # PAC Dragon Channel — EMA(34) on High/Low/Close (MT5-compatible EMA)
        df["pac_mid"]  = ema_mt5(df["close"], self._ema_fast)
        df["ema34"]    = df["pac_mid"]          # alias for backward compatibility
        df["pac_high"] = ema_mt5(df["high"], self._ema_fast)
        df["pac_low"]  = ema_mt5(df["low"], self._ema_fast)

        # Trend EMAs
        df["ema89"]  = ema_mt5(df["close"], self._ema_slow)
        df["ema200"] = ema_mt5(df["close"], self._ema_trend)

        # Volatility
        df["atr"] = AverageTrueRange(
            high=df["high"], low=df["low"], close=df["close"],
            window=self._atr_period,
        ).average_true_range()

        # ADX — trend strength (Opt-10)
        adx_ind = ADXIndicator(
            high=df["high"], low=df["low"], close=df["close"],
            window=self._adx_period, fillna=True,
        )
        df["adx"] = adx_ind.adx()

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

    

        # Compare in UTC (CSV bars are tz-aware +00:00; naive cutoff would raise)
        _ts = pd.Timestamp(curr["timestamp"])
        if _ts.tzinfo is None:
            _ts = _ts.tz_localize("UTC")
        else:
            _ts = _ts.tz_convert("UTC")
        if _ts < pd.Timestamp("2026-01-01", tz="UTC"):
            return self._no_signal()

        # if _ts >= pd.Timestamp("2026-03-23", tz="UTC"):
        #     print(df.tail(5))
        #     print("test")

        if curr["atr"] <= 0 or pd.isna(curr["atr"]):
            return self._no_signal()

        # ── Opt-9: Time / Session Filter ───────────────────────────────────────
        if not self._is_trade_hour_ok(curr):
            return self._no_signal()

        # ── Opt-10: ADX Filter (direction-agnostic, gate tất cả layers) ────────
        if not self._adx_ok(curr):
            return self._no_signal()

        # ── Opt-12: Dragon Zigzag Filter (sideway detection, gate tất cả layers)
        if not self._dragon_no_zigzag(df):
            return self._no_signal()

        # ── Opt-13: Absolute EMA Gap Filter (gate tất cả layers) ──────────────
        if not self._ema_gap_ok(curr):
            return self._no_signal()

        # ── Pick candidate signal across all layers ────────────────────────────
        candidate = self._pick_candidate_signal(df, curr, prev)

        # ── Opt-15: Higher Timeframe (HTF) EMA Bias Filter ────────────────────
        # Áp dụng SAU KHI có candidate: lọc chiều ngược xu hướng H1 (hoặc HTF đã cấu hình).
        if candidate is not None and self._htf_ema_filter:
            htf_dir = self._htf_ema_dir(df)
            if htf_dir is not None and candidate.action != htf_dir:
                logger.debug(
                    "SonicR HTF bias: %s signal blocked — H1 EMA34/89 bias is %s",
                    candidate.action, htf_dir,
                )
                return self._no_signal()

        return candidate if candidate is not None else self._no_signal()

    def _pick_candidate_signal(
        self,
        df: pd.DataFrame,
        curr: pd.Series,
        prev: pd.Series,
    ) -> Signal | None:
        """
        Execute all layer checks in priority order; return first signal found.
        Extracted from generate_signal so the HTF bias filter can be applied
        cleanly to the final candidate without touching every _check_* method.
        """
        # ── Layer 1+2: PAC breakout / rejection (from MQ5) ────────────────────
        if self._enable_pac:
            if self._rejection_priority:
                sig = self._check_pac_rejection(curr, prev, df)
                if sig is not None:
                    return sig
                sig = self._check_pac_breakout(curr, prev, df)
                if sig is not None:
                    return sig
            else:
                sig = self._check_pac_breakout(curr, prev, df)
                if sig is not None:
                    return sig
                sig = self._check_pac_rejection(curr, prev, df)
                if sig is not None:
                    return sig
            # Opt-3 retest entry (when main breakout was non-Marubozu)
            if self._require_bo_confirmation:
                sig = self._check_pac_breakout_retest(df, curr, prev)
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

        return None

    # ── Layer 1: PAC Breakout ─────────────────────────────────────────────────

    def _check_pac_breakout(
        self,
        curr: pd.Series,
        prev: pd.Series,
        df: pd.DataFrame | None = None,
    ) -> Signal | None:
        """
        MQ5 buyCond / sellCond. Supports Opt-1 slope, Opt-2 swing SL,
        Opt-3 Marubozu, Opt-4 ATR SL.
        """
        pac_high = curr["pac_high"]
        pac_low  = curr["pac_low"]
        ema200   = curr["ema200"]
        atr      = float(curr["atr"])

        if not self._is_volume_ok(curr, self._vol_ratio_bo):
            return None
        if not self._is_strong_body_avg(curr):
            return None

        # ── BUY breakout ──────────────────────────────────────────────────────
        if prev["close"] <= prev["pac_high"] and curr["close"] > pac_high:
            if curr["close"] <= ema200:
                return None
            # Opt-1
            if df is not None and not self._is_dual_slope_valid(df, curr, "BUY"):
                return None
            if df is not None and not self._linreg_slope_ok(df, "BUY"):
                return None
            # Opt-3
            if self._require_bo_confirmation and not self._is_marubozu(curr, "BUY"):
                logger.debug("SonicR Breakout BUY: not Marubozu → waiting for retest")
                return None

            dist_ema200_pips = self._price_to_pips(abs(float(curr["close"]) - float(ema200)))
            if self._bo_max_ema200_dist_pips > 0 and dist_ema200_pips > self._bo_max_ema200_dist_pips:
                return None

            entry = float(curr["close"])
            # Opt-4 ATR SL; Opt-2 Swing SL; fallback PAC SL
            sl_lvl = self._override_sl_if_atr(entry, atr, "BUY")
            if sl_lvl is None:
                if self._use_swing_sl and df is not None:
                    sl_lvl = self._calc_swing_sl(df, atr, "BUY")
                if not self._use_swing_sl or df is None or sl_lvl is None or sl_lvl >= entry:
                    sl_lvl = float(pac_low) - atr * self._sl_buffer_atr
            # Opt-14: enforce minimum SL width
            sl_lvl = self._enforce_min_sl(sl_lvl, entry, "BUY")

            sl_distance = entry - sl_lvl
            if sl_distance <= 0:
                return None
            if not self._sl_within_atr_cap(sl_distance, atr):
                return None
            sl_pips = self._price_to_pips(sl_distance)
            if sl_pips <= 0:
                return None
            if self._max_sl_pips > 0 and sl_pips > self._max_sl_pips:
                logger.debug("SonicR Breakout BUY skipped: sl_pips=%.0f > max=%.0f", sl_pips, self._max_sl_pips)
                return None

            sl_src = "atr" if self._use_atr_sl else ("swing" if self._use_swing_sl else "pac")
            lim = float(pac_high) if self._limit_entry else 0.0
            return self._make_signal(
                "BUY", entry, sl_pips,
                f"PAC-Breakout BUY | pac_high={pac_high:.5f} ema200={ema200:.5f}"
                f" dist_ema200={dist_ema200_pips:.0f}p sl={sl_pips:.0f}p sl_src={sl_src}",
                limit_price=lim,
                limit_expiry_bars=self._limit_expiry,
                sl_level=sl_lvl,
                breakeven_at_r=self._breakeven_at_r,
                partial_close_at_r=self._partial_close_at_r,
                partial_close_ratio=self._partial_close_ratio,
                partial_trail_pips=self._partial_trail_pips,
            )

        # ── SELL breakout ─────────────────────────────────────────────────────
        if prev["close"] >= prev["pac_low"] and curr["close"] < pac_low:
            if curr["close"] >= ema200:
                return None
            if df is not None and not self._is_dual_slope_valid(df, curr, "SELL"):
                return None
            if df is not None and not self._linreg_slope_ok(df, "SELL"):
                return None
            if self._require_bo_confirmation and not self._is_marubozu(curr, "SELL"):
                logger.debug("SonicR Breakout SELL: not Marubozu → waiting for retest")
                return None

            dist_ema200_pips = self._price_to_pips(abs(float(curr["close"]) - float(ema200)))
            if self._bo_max_ema200_dist_pips > 0 and dist_ema200_pips > self._bo_max_ema200_dist_pips:
                return None

            entry = float(curr["close"])
            sl_lvl = self._override_sl_if_atr(entry, atr, "SELL")
            if sl_lvl is None:
                if self._use_swing_sl and df is not None:
                    sl_lvl = self._calc_swing_sl(df, atr, "SELL")
                if not self._use_swing_sl or df is None or sl_lvl is None or sl_lvl <= entry:
                    sl_lvl = float(pac_high) + atr * self._sl_buffer_atr
            # Opt-14: enforce minimum SL width
            sl_lvl = self._enforce_min_sl(sl_lvl, entry, "SELL")

            sl_distance = sl_lvl - entry
            if sl_distance <= 0:
                return None
            if not self._sl_within_atr_cap(sl_distance, atr):
                return None
            sl_pips = self._price_to_pips(sl_distance)
            if sl_pips <= 0:
                return None
            if self._max_sl_pips > 0 and sl_pips > self._max_sl_pips:
                logger.debug("SonicR Breakout SELL skipped: sl_pips=%.0f > max=%.0f", sl_pips, self._max_sl_pips)
                return None

            sl_src = "atr" if self._use_atr_sl else ("swing" if self._use_swing_sl else "pac")
            lim = float(pac_low) if self._limit_entry else 0.0
            return self._make_signal(
                "SELL", entry, sl_pips,
                f"PAC-Breakout SELL | pac_low={pac_low:.5f} ema200={ema200:.5f}"
                f" dist_ema200={dist_ema200_pips:.0f}p sl={sl_pips:.0f}p sl_src={sl_src}",
                limit_price=lim,
                limit_expiry_bars=self._limit_expiry,
                sl_level=sl_lvl,
                breakeven_at_r=self._breakeven_at_r,
                partial_close_at_r=self._partial_close_at_r,
                partial_close_ratio=self._partial_close_ratio,
                partial_trail_pips=self._partial_trail_pips,
            )

        return None

    # ── Layer 2: PAC Rejection ────────────────────────────────────────────────

    def _check_pac_rejection(
        self,
        curr: pd.Series,
        prev: pd.Series,
        df: pd.DataFrame | None = None,
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

        rej_buf = self._sl_buffer_atr + self._rej_extra_sl_atr

        # ── BUY rejection ─────────────────────────────────────────────────────
        if curr["low"] < pac_high and curr["close"] > pac_high:
            if not (curr["close"] > ema89 and curr["close"] > curr["open"]):
                return None
            if not self._ema200_trend_ok(curr, "BUY"):
                return None
            if df is not None and not self._is_dual_slope_valid(df, curr, "BUY"):
                return None
            if df is not None and not self._linreg_slope_ok(df, "BUY"):
                return None

            entry = float(curr["close"])
            atr_f = float(atr)
            sl_lvl = self._override_sl_if_atr(entry, atr_f, "BUY")
            if sl_lvl is None:
                if self._use_swing_sl and df is not None:
                    sl_lvl = self._calc_swing_sl(df, atr_f, "BUY")
                if not self._use_swing_sl or df is None or sl_lvl is None or sl_lvl >= entry:
                    sl_lvl = float(curr["low"]) - atr_f * rej_buf
            # Opt-14: enforce minimum SL width
            sl_lvl = self._enforce_min_sl(sl_lvl, entry, "BUY")

            sl_distance = entry - sl_lvl
            if sl_distance <= 0:
                return None
            if not self._sl_within_atr_cap(sl_distance, atr_f):
                return None
            sl_pips = self._price_to_pips(sl_distance)
            if sl_pips <= 0:
                return None
            if self._max_sl_pips > 0 and sl_pips > self._max_sl_pips:
                logger.debug("SonicR Rejection BUY skipped: sl_pips=%.0f > max=%.0f", sl_pips, self._max_sl_pips)
                return None

            sl_src = "atr" if self._use_atr_sl else ("swing" if self._use_swing_sl else f"{rej_buf:.2f}×ATR")
            lim = float(pac_high) if self._limit_entry else 0.0
            return self._make_signal(
                "BUY", entry, sl_pips,
                f"PAC-Rejection BUY | pac_high={pac_high:.5f} ema89={ema89:.5f}"
                f" sl={sl_pips:.0f}p sl_src={sl_src}",
                limit_price=lim,
                limit_expiry_bars=self._limit_expiry,
                sl_level=sl_lvl,
                breakeven_at_r=self._breakeven_at_r,
                partial_close_at_r=self._partial_close_at_r,
                partial_close_ratio=self._partial_close_ratio,
                partial_trail_pips=self._partial_trail_pips,
            )

        # ── SELL rejection ────────────────────────────────────────────────────
        if curr["high"] > pac_low and curr["close"] < pac_low:
            if not (curr["close"] < ema89 and curr["close"] < curr["open"]):
                return None
            if not self._ema200_trend_ok(curr, "SELL"):
                return None
            if df is not None and not self._is_dual_slope_valid(df, curr, "SELL"):
                return None
            if df is not None and not self._linreg_slope_ok(df, "SELL"):
                return None

            entry = float(curr["close"])
            atr_f = float(atr)
            sl_lvl = self._override_sl_if_atr(entry, atr_f, "SELL")
            if sl_lvl is None:
                if self._use_swing_sl and df is not None:
                    sl_lvl = self._calc_swing_sl(df, atr_f, "SELL")
                if not self._use_swing_sl or df is None or sl_lvl is None or sl_lvl <= entry:
                    sl_lvl = float(curr["high"]) + atr_f * rej_buf
            # Opt-14: enforce minimum SL width
            sl_lvl = self._enforce_min_sl(sl_lvl, entry, "SELL")

            sl_distance = sl_lvl - entry
            if sl_distance <= 0:
                return None
            if not self._sl_within_atr_cap(sl_distance, atr_f):
                return None
            sl_pips = self._price_to_pips(sl_distance)
            if sl_pips <= 0:
                return None
            if self._max_sl_pips > 0 and sl_pips > self._max_sl_pips:
                logger.debug("SonicR Rejection SELL skipped: sl_pips=%.0f > max=%.0f", sl_pips, self._max_sl_pips)
                return None

            sl_src = "atr" if self._use_atr_sl else ("swing" if self._use_swing_sl else f"{rej_buf:.2f}×ATR")
            lim = float(pac_low) if self._limit_entry else 0.0
            return self._make_signal(
                "SELL", entry, sl_pips,
                f"PAC-Rejection SELL | pac_low={pac_low:.5f} ema89={ema89:.5f}"
                f" sl={sl_pips:.0f}p sl_src={sl_src}",
                limit_price=lim,
                limit_expiry_bars=self._limit_expiry,
                sl_level=sl_lvl,
                breakeven_at_r=self._breakeven_at_r,
                partial_close_at_r=self._partial_close_at_r,
                partial_close_ratio=self._partial_close_ratio,
                partial_trail_pips=self._partial_trail_pips,
            )

        return None

    # ── Layer 1 Retest (Opt-3 when require_bo_confirmation) ──────────────────

    def _check_pac_breakout_retest(
        self,
        df: pd.DataFrame,
        curr: pd.Series,
        prev: pd.Series,
    ) -> Signal | None:
        """
        Retest entry: prev bar was a non-Marubozu breakout; curr retests PAC.
          BUY : df[-3] ≤ pac_high, df[-2] > pac_high, curr low ≤ pac_high AND close > pac_high
          SELL: df[-3] ≥ pac_low,  df[-2] < pac_low,  curr high ≥ pac_low  AND close < pac_low
        """
        if len(df) < 3:
            return None

        pre_prev = df.iloc[-3]
        pac_high = float(curr["pac_high"])
        pac_low  = float(curr["pac_low"])
        ema200   = float(curr["ema200"])
        atr      = float(curr["atr"])
        entry    = float(curr["close"])

        # BUY retest
        if (
            float(pre_prev["close"]) <= float(pre_prev.get("pac_high", pac_high))
            and float(prev["close"])  > float(prev.get("pac_high", pac_high))
            and float(curr["low"])    <= pac_high
            and entry > pac_high
            and entry > ema200
        ):
            if not self._is_dual_slope_valid(df, curr, "BUY"):
                return None
            if not self._linreg_slope_ok(df, "BUY"):
                return None
            if not self._is_volume_ok(curr, self._vol_ratio_bo):
                return None
            dist_ema200_pips = self._price_to_pips(abs(entry - ema200))
            if self._bo_max_ema200_dist_pips > 0 and dist_ema200_pips > self._bo_max_ema200_dist_pips:
                return None

            sl_lvl = self._override_sl_if_atr(entry, atr, "BUY")
            if sl_lvl is None:
                sl_lvl = self._calc_swing_sl(df, atr, "BUY") if self._use_swing_sl else 0.0
                if sl_lvl >= entry or sl_lvl == 0.0:
                    sl_lvl = float(curr["pac_low"]) - atr * self._sl_buffer_atr
            # Opt-14: enforce minimum SL width
            sl_lvl = self._enforce_min_sl(sl_lvl, entry, "BUY")
            sl_d = entry - sl_lvl
            if sl_d <= 0 or not self._sl_within_atr_cap(sl_d, atr):
                return None
            sl_pips = self._price_to_pips(sl_d)
            if sl_pips <= 0 or (self._max_sl_pips > 0 and sl_pips > self._max_sl_pips):
                return None

            lim = pac_high if self._limit_entry else 0.0
            return self._make_signal(
                "BUY", entry, sl_pips,
                f"PAC-Breakout-Retest BUY | pac_high={pac_high:.5f} sl={sl_pips:.0f}p",
                limit_price=lim, limit_expiry_bars=self._limit_expiry,
                sl_level=sl_lvl, breakeven_at_r=self._breakeven_at_r,
                partial_close_at_r=self._partial_close_at_r,
                partial_close_ratio=self._partial_close_ratio,
                partial_trail_pips=self._partial_trail_pips,
            )

        # SELL retest
        if (
            float(pre_prev["close"]) >= float(pre_prev.get("pac_low", pac_low))
            and float(prev["close"])  < float(prev.get("pac_low", pac_low))
            and float(curr["high"])   >= pac_low
            and entry < pac_low
            and entry < ema200
        ):
            if not self._is_dual_slope_valid(df, curr, "SELL"):
                return None
            if not self._linreg_slope_ok(df, "SELL"):
                return None
            if not self._is_volume_ok(curr, self._vol_ratio_bo):
                return None
            dist_ema200_pips = self._price_to_pips(abs(entry - ema200))
            if self._bo_max_ema200_dist_pips > 0 and dist_ema200_pips > self._bo_max_ema200_dist_pips:
                return None

            sl_lvl = self._override_sl_if_atr(entry, atr, "SELL")
            if sl_lvl is None:
                sl_lvl = self._calc_swing_sl(df, atr, "SELL") if self._use_swing_sl else 0.0
                if sl_lvl <= entry or sl_lvl == 0.0:
                    sl_lvl = float(curr["pac_high"]) + atr * self._sl_buffer_atr
            # Opt-14: enforce minimum SL width
            sl_lvl = self._enforce_min_sl(sl_lvl, entry, "SELL")
            sl_d = sl_lvl - entry
            if sl_d <= 0 or not self._sl_within_atr_cap(sl_d, atr):
                return None
            sl_pips = self._price_to_pips(sl_d)
            if sl_pips <= 0 or (self._max_sl_pips > 0 and sl_pips > self._max_sl_pips):
                return None

            lim = pac_low if self._limit_entry else 0.0
            return self._make_signal(
                "SELL", entry, sl_pips,
                f"PAC-Breakout-Retest SELL | pac_low={pac_low:.5f} sl={sl_pips:.0f}p",
                limit_price=lim, limit_expiry_bars=self._limit_expiry,
                sl_level=sl_lvl, breakeven_at_r=self._breakeven_at_r,
                partial_close_at_r=self._partial_close_at_r,
                partial_close_ratio=self._partial_close_ratio,
                partial_trail_pips=self._partial_trail_pips,
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

        # Opt-6: EMA200 global trend filter
        if not self._ema200_trend_ok(curr, "BUY"):
            return None

        # EMA89 slope upward
        if not self._ema89_sloping_up(df):
            return None

        # Trigger: nến hiện tại phải là nến đầu tiên đóng cửa TRÊN pac_mid
        # (fresh cross — tránh vào lệnh muộn khi giá đã chạy xa pac_mid từ trước)
        if not (curr["close"] > pac_mid and prev["close"] <= pac_mid):
            return None

        # Opt-16: kiểm tra entry không quá xa pac_mid (phòng ngừa nến bùng nổ)
        if self._max_entry_pac_dist_atr > 0:
            dist = abs(float(curr["close"]) - float(pac_mid))
            if dist > self._max_entry_pac_dist_atr * float(atr):
                logger.debug(
                    "SonicR BUY★ skipped: entry %.5f too far from pac_mid %.5f "
                    "(%.1f pips > %.1f×ATR)",
                    float(curr["close"]), float(pac_mid),
                    self._price_to_pips(dist),
                    self._max_entry_pac_dist_atr,
                )
                return None

        # Strong bullish candle (optimisation filter)
        if self._req_strong_candle and not self._is_strong_candle(curr, "BUY"):
            return None

        # Entry bar fully above EMA89 (rejected from below EMA89)
        if self._req_ema89_rej and curr["low"] <= ema89:
            return None

        # Opt-1 + Opt-11
        if not self._is_dual_slope_valid(df, curr, "BUY"):
            return None
        if not self._linreg_slope_ok(df, "BUY"):
            return None

        n        = len(df)
        pb_start = max(0, n - self._pb_lookback)
        pb_window = df.iloc[pb_start:]

        # Opt-5: close-confirmation touch (no wick)
        touch_col = "close" if self._use_close_for_ema_touch else "low"
        ema_touch = pb_window[touch_col] <= (pb_window["pac_mid"] + atr * 0.5)
        if not ema_touch.any():
            return None

        first_touch_pos = int(ema_touch.values.argmax())
        pullback_slice  = pb_window.iloc[first_touch_pos:]
        pullback_low    = float(pullback_slice["low"].min())

        # Pullback reached EMA89 zone
        if self._req_ema89_touch:
            touched = pullback_slice[touch_col] <= (
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
        ext_slice = df.iloc[ext_start:n]

        if ext_slice.empty:
            return None

        far_zone = ext_slice["high"] > ext_slice["pac_mid"] + self._atr_mult_far * ext_slice["atr"]
        if not far_zone.any():
            return None

        # No new lower-low
        if pullback_low < float(ext_slice["low"].min()) * 0.9990:
            return None

        entry = float(curr["close"])
        atr_f = float(atr)

        # Opt-4 ATR SL; Opt-2 Swing SL; fallback PAC SL
        sl_lvl = self._override_sl_if_atr(entry, atr_f, "BUY")
        if sl_lvl is None:
            if self._use_swing_sl:
                sl_lvl = self._calc_swing_sl(df, atr_f, "BUY")
            if not self._use_swing_sl or sl_lvl is None or sl_lvl >= entry:
                sl_lvl = min(pullback_low, float(ema89)) - atr_f * self._sl_buffer_atr
        # Opt-14: enforce minimum SL width
        sl_lvl = self._enforce_min_sl(sl_lvl, entry, "BUY")

        sl_distance = entry - sl_lvl
        if sl_distance <= 0:
            return None
        if not self._sl_within_atr_cap(sl_distance, atr_f):
            return None

        recent_high = float(ext_slice["high"].max())
        rr = (recent_high - entry) / sl_distance if sl_distance > 0 else 0.0
        if rr < self._min_rr:
            logger.debug("SonicR BUY skipped RR=%.2f < %.1f", rr, self._min_rr)
            return None

        sl_pips = self._price_to_pips(sl_distance)
        if sl_pips <= 0:
            return None
        if self._max_sl_pips > 0 and sl_pips > self._max_sl_pips:
            logger.debug("SonicR BUY★ skipped: sl_pips=%.0f > max=%.0f", sl_pips, self._max_sl_pips)
            return None

        sl_src = "atr" if self._use_atr_sl else ("swing" if self._use_swing_sl else "pac")
        lim = float(pac_mid) if self._limit_entry else 0.0
        return self._make_signal(
            "BUY", entry, sl_pips,
            f"SonicR BUY★ | pac_mid={pac_mid:.5f} ema89={ema89:.5f} "
            f"PB_low={pullback_low:.5f} RR≈{rr:.1f} sl={sl_pips:.0f}p sl_src={sl_src}",
            limit_price=lim,
            limit_expiry_bars=self._limit_expiry,
            sl_level=sl_lvl,
            breakeven_at_r=self._breakeven_at_r,
            partial_close_at_r=self._partial_close_at_r,
            partial_close_ratio=self._partial_close_ratio,
            partial_trail_pips=self._partial_trail_pips,
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

        # Opt-6: EMA200 global trend filter
        if not self._ema200_trend_ok(curr, "SELL"):
            return None

        if not self._ema89_sloping_down(df):
            return None

        # Trigger: nến hiện tại phải là nến đầu tiên đóng cửa DƯỚI pac_mid
        # (fresh cross — tránh vào lệnh muộn khi giá đã chạy xa pac_mid từ trước)
        if not (curr["close"] < pac_mid and prev["close"] >= pac_mid):
            return None

        # Opt-16: kiểm tra entry không quá xa pac_mid (phòng ngừa nến bùng nổ)
        if self._max_entry_pac_dist_atr > 0:
            dist = abs(float(curr["close"]) - float(pac_mid))
            if dist > self._max_entry_pac_dist_atr * float(atr):
                logger.debug(
                    "SonicR SELL★ skipped: entry %.5f too far from pac_mid %.5f "
                    "(%.1f pips > %.1f×ATR)",
                    float(curr["close"]), float(pac_mid),
                    self._price_to_pips(dist),
                    self._max_entry_pac_dist_atr,
                )
                return None

        if self._req_strong_candle and not self._is_strong_candle(curr, "SELL"):
            return None

        if self._req_ema89_rej and curr["high"] >= ema89:
            return None

        # Opt-1 + Opt-11
        if not self._is_dual_slope_valid(df, curr, "SELL"):
            return None
        if not self._linreg_slope_ok(df, "SELL"):
            return None

        n           = len(df)
        pb_start    = max(0, n - self._pb_lookback)
        corr_window = df.iloc[pb_start:]

        # Opt-5: close-confirmation touch (no wick)
        touch_col = "close" if self._use_close_for_ema_touch else "high"
        ema_touch = corr_window[touch_col] >= (corr_window["pac_mid"] - atr * 0.5)
        if not ema_touch.any():
            return None

        first_touch_pos  = int(ema_touch.values.argmax())
        correction_slice = corr_window.iloc[first_touch_pos:]
        correction_high  = float(correction_slice["high"].max())

        if self._req_ema89_touch:
            touched = correction_slice[touch_col] >= (
                correction_slice["ema89"] - self._ema89_touch_atr * atr
            )
            if not touched.any():
                return None

        if (correction_slice["close"] > correction_slice["ema89"] + atr * 0.5).any():
            return None

        ext_end   = pb_start + first_touch_pos
        ext_start = max(0, ext_end - self._ext_lookback)
        ext_slice = df.iloc[ext_start:n]

        if ext_slice.empty:
            return None

        far_zone = ext_slice["low"] < ext_slice["pac_mid"] - self._atr_mult_far * ext_slice["atr"]
        if not far_zone.any():
            return None

        if correction_high > float(ext_slice["high"].max()) * 1.0010:
            return None

        entry = float(curr["close"])
        atr_f = float(atr)

        sl_lvl = self._override_sl_if_atr(entry, atr_f, "SELL")
        if sl_lvl is None:
            if self._use_swing_sl:
                sl_lvl = self._calc_swing_sl(df, atr_f, "SELL")
            if not self._use_swing_sl or sl_lvl is None or sl_lvl <= entry:
                sl_lvl = max(correction_high, float(ema89)) + atr_f * self._sl_buffer_atr
        # Opt-14: enforce minimum SL width
        sl_lvl = self._enforce_min_sl(sl_lvl, entry, "SELL")

        sl_distance = sl_lvl - entry
        if sl_distance <= 0:
            return None
        if not self._sl_within_atr_cap(sl_distance, atr_f):
            return None

        recent_low = float(ext_slice["low"].min())
        rr = (entry - recent_low) / sl_distance if sl_distance > 0 else 0.0
        if rr < self._min_rr:
            logger.debug("SonicR SELL skipped RR=%.2f < %.1f", rr, self._min_rr)
            return None

        sl_pips = self._price_to_pips(sl_distance)
        if sl_pips <= 0:
            return None
        if self._max_sl_pips > 0 and sl_pips > self._max_sl_pips:
            logger.debug("SonicR SELL★ skipped: sl_pips=%.0f > max=%.0f", sl_pips, self._max_sl_pips)
            return None

        sl_src = "atr" if self._use_atr_sl else ("swing" if self._use_swing_sl else "pac")
        lim = float(pac_mid) if self._limit_entry else 0.0
        return self._make_signal(
            "SELL", entry, sl_pips,
            f"SonicR SELL★ | pac_mid={pac_mid:.5f} ema89={ema89:.5f} "
            f"COR_high={correction_high:.5f} RR≈{rr:.1f} sl={sl_pips:.0f}p sl_src={sl_src}",
            limit_price=lim,
            limit_expiry_bars=self._limit_expiry,
            sl_level=sl_lvl,
            breakeven_at_r=self._breakeven_at_r,
            partial_close_at_r=self._partial_close_at_r,
            partial_close_ratio=self._partial_close_ratio,
            partial_trail_pips=self._partial_trail_pips,
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
            if not self._ema200_trend_ok(curr, "BUY"):
                return None
            if self._req_strong_candle and not self._is_strong_candle(curr, "BUY"):
                return None

            sl_lvl      = range_low - float(atr) * self._sl_buffer_atr
            # Opt-14: enforce minimum SL width
            sl_lvl      = self._enforce_min_sl(sl_lvl, float(curr["close"]), "BUY")
            sl_distance = float(curr["close"]) - sl_lvl
            if sl_distance <= 0:
                return None
            if not self._sl_within_atr_cap(sl_distance, float(atr)):
                return None

            tp_distance = range_high - float(curr["close"])
            rr          = tp_distance / sl_distance if sl_distance > 0 else 0.0
            if rr < self._min_rr:
                return None

            sl_pips = self._price_to_pips(sl_distance)
            if sl_pips <= 0:
                return None

            lim = float(pac_mid) if self._limit_entry else 0.0
            return self._make_signal(
                "BUY", float(curr["close"]), sl_pips,
                f"SonicR SW-BUY | pac_mid={pac_mid:.5f} "
                f"Range=[{range_low:.5f}–{range_high:.5f}] RR≈{rr:.1f}",
                limit_price=lim,
                limit_expiry_bars=self._limit_expiry,
                sl_level=sl_lvl,
                breakeven_at_r=self._breakeven_at_r,
                partial_close_at_r=self._partial_close_at_r,
                partial_close_ratio=self._partial_close_ratio,
                partial_trail_pips=self._partial_trail_pips,
            )

        # SELL: rejection downward through pac_mid
        if curr["close"] < pac_mid and prev["close"] >= pac_mid:
            if not self._ema200_trend_ok(curr, "SELL"):
                return None
            if self._req_strong_candle and not self._is_strong_candle(curr, "SELL"):
                return None

            sl_lvl      = range_high + float(atr) * self._sl_buffer_atr
            # Opt-14: enforce minimum SL width
            sl_lvl      = self._enforce_min_sl(sl_lvl, float(curr["close"]), "SELL")
            sl_distance = sl_lvl - float(curr["close"])
            if sl_distance <= 0:
                return None
            if not self._sl_within_atr_cap(sl_distance, float(atr)):
                return None

            tp_distance = float(curr["close"]) - range_low
            rr          = tp_distance / sl_distance if sl_distance > 0 else 0.0
            if rr < self._min_rr:
                return None

            sl_pips = self._price_to_pips(sl_distance)
            if sl_pips <= 0:
                return None

            lim = float(pac_mid) if self._limit_entry else 0.0
            return self._make_signal(
                "SELL", float(curr["close"]), sl_pips,
                f"SonicR SW-SELL | pac_mid={pac_mid:.5f} "
                f"Range=[{range_low:.5f}–{range_high:.5f}] RR≈{rr:.1f}",
                limit_price=lim,
                limit_expiry_bars=self._limit_expiry,
                sl_level=sl_lvl,
                breakeven_at_r=self._breakeven_at_r,
                partial_close_at_r=self._partial_close_at_r,
                partial_close_ratio=self._partial_close_ratio,
                partial_trail_pips=self._partial_trail_pips,
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

    # ── Opt-1: Dual Slope ─────────────────────────────────────────────────────

    def _is_dual_slope_valid(
        self, df: pd.DataFrame, curr: pd.Series, direction: str
    ) -> bool:
        """
        Kiểm tra EMA34 và EMA89 đều có độ dốc rõ ràng theo hướng tín hiệu.
        slope_pips_per_bar = (ema_now − ema_N_ago) / N / pip_size
        """
        if not self._require_ema_slope:
            return True
        n = self._slope_lookback
        if len(df) < n + 2:
            return True
        prev_n   = df.iloc[-(n + 1)]
        slope34  = (float(curr["pac_mid"]) - float(prev_n["pac_mid"])) / n
        slope89  = (float(curr["ema89"])   - float(prev_n["ema89"]))   / n
        pip_size = self._pip_size()
        s34p     = slope34 / pip_size
        s89p     = slope89 / pip_size
        thresh   = self._min_slope_pips_per_bar
        if direction == "BUY":
            ok = s34p > thresh and s89p > thresh
        else:
            ok = s34p < -thresh and s89p < -thresh
        if not ok:
            logger.debug(
                "SonicR slope %s: EMA34=%.2f EMA89=%.2f pips/bar (need %s%.2f)",
                direction, s34p, s89p,
                ">" if direction == "BUY" else "<",
                thresh if direction == "BUY" else -thresh,
            )
        return ok

    # ── Opt-2: Swing SL ───────────────────────────────────────────────────────

    def _calc_swing_sl(
        self, df: pd.DataFrame, atr: float, direction: str
    ) -> float:
        """SL = Swing Low/High (lookback bars) ± swing_sl_atr_mult × ATR."""
        lb = self._swing_sl_lookback
        if len(df) < lb + 2:
            return 0.0
        window = df.iloc[-(lb + 1):-1]
        buf    = float(atr) * self._swing_sl_atr_mult
        return float(window["low"].min()) - buf if direction == "BUY" \
            else float(window["high"].max()) + buf

    # ── Opt-3: Marubozu ───────────────────────────────────────────────────────

    def _is_marubozu(self, bar: pd.Series, direction: str) -> bool:
        """Thân nến ≥ bo_marubozu_ratio × full range, đúng chiều."""
        full_range = float(bar["high"]) - float(bar["low"])
        if full_range <= 0:
            return False
        body   = abs(float(bar["close"]) - float(bar["open"]))
        is_dir = (
            (direction == "BUY"  and bar["close"] > bar["open"]) or
            (direction == "SELL" and bar["close"] < bar["open"])
        )
        return is_dir and body >= self._bo_marubozu_ratio * full_range

    # ── Opt-4: ATR SL override ────────────────────────────────────────────────

    def _override_sl_if_atr(
        self, entry: float, atr: float, direction: str
    ) -> float | None:
        """
        Khi use_atr_sl=True: SL = entry ± atr_sl_mult × ATR.
        None = tắt (caller dùng SL tính sẵn).
        """
        if not self._use_atr_sl:
            return None
        buf = float(atr) * self._atr_sl_mult
        return entry - buf if direction == "BUY" else entry + buf

    # ── Opt-6: EMA200 global trend filter ────────────────────────────────────

    def _ema200_trend_ok(self, curr: pd.Series, direction: str) -> bool:
        """BUY chỉ khi close > EMA200; SELL ngược lại. Tắt khi require_ema200_trend=False."""
        if not self._require_ema200_trend:
            return True
        ema200 = float(curr.get("ema200", curr["close"]))
        ok = curr["close"] > ema200 if direction == "BUY" else curr["close"] < ema200
        if not ok:
            logger.debug(
                "SonicR %s filtered: close=%.5f %s EMA200=%.5f",
                direction, float(curr["close"]),
                "not >" if direction == "BUY" else "not <",
                ema200,
            )
        return ok

    # ── Opt-7: Max SL cap relative to ATR ────────────────────────────────────

    def _sl_within_atr_cap(self, sl_distance: float, atr: float) -> bool:
        """Trả về False (bỏ lệnh) nếu SL > max_sl_atr_mult × ATR. 0 = tắt."""
        if self._max_sl_atr_mult <= 0:
            return True
        cap = float(atr) * self._max_sl_atr_mult
        ok = sl_distance <= cap
        if not ok:
            logger.debug(
                "SonicR SL=%.5f > cap=%.5f (%.1f×ATR) → skipped",
                sl_distance, cap, self._max_sl_atr_mult,
            )
        return ok

    # ── Opt-13: Absolute EMA Gap Filter ──────────────────────────────────────

    def _ema_gap_ok(self, curr: pd.Series) -> bool:
        """
        Kiểm tra khoảng cách tuyệt đối giữa EMA34 (pac_mid) và EMA89.
        Nếu gap < min_ema_gap_pips → EMA đi sát nhau → sideway → skip.
        0 = tắt.
        """
        if self._min_ema_gap_pips <= 0:
            return True
        gap_pips = self._price_to_pips(
            abs(float(curr["pac_mid"]) - float(curr["ema89"]))
        )
        ok = gap_pips >= self._min_ema_gap_pips
        if not ok:
            logger.debug(
                "SonicR EMA gap %.1f pips < min %.1f → sideway skip",
                gap_pips, self._min_ema_gap_pips,
            )
        return ok

    # ── Opt-14: Minimum SL Width ───────────────────────────────────────────────

    def _enforce_min_sl(self, sl_lvl: float, entry: float, direction: str) -> float:
        """
        Đảm bảo SL rộng ít nhất min_sl_pips. Nếu SL tính được hẹp hơn, kéo
        sl_lvl ra xa entry đến đúng min_sl_pips.
        • BUY : sl_lvl = min(sl_lvl, entry − min_sl_pips × pip_size)
        • SELL: sl_lvl = max(sl_lvl, entry + min_sl_pips × pip_size)
        0 = tắt.
        """
        if self._min_sl_pips <= 0:
            return sl_lvl
        min_dist = self._min_sl_pips * self._pip_size()
        if direction == "BUY":
            return min(sl_lvl, entry - min_dist)
        else:
            return max(sl_lvl, entry + min_dist)

    # ── Opt-9: Time / Session Filter ─────────────────────────────────────────

    def _is_trade_hour_ok(self, curr: pd.Series) -> bool:
        """
        Kiểm tra giờ UTC của nến hiện tại có nằm trong allowed_hours_utc không.
        Set rỗng = cho phép mọi giờ.
        """
        if not self._allowed_hours_utc:
            return True
        try:
            ts_raw = curr.get("timestamp", curr.name)
            ts = pd.Timestamp(ts_raw)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            ok = ts.hour in self._allowed_hours_utc
            if not ok:
                logger.debug("SonicR time filter: hour %d not in allowed set", ts.hour)
            return ok
        except Exception:
            return True  # graceful fallback nếu timestamp không parse được

    # ── Opt-10: ADX Filter ───────────────────────────────────────────────────

    def _adx_ok(self, curr: pd.Series) -> bool:
        """Chỉ vào lệnh khi ADX ≥ adx_filter_min. 0 = tắt."""
        if self._adx_filter_min <= 0:
            return True
        adx = float(curr.get("adx", float("nan")))
        if pd.isna(adx) or adx <= 0:
            return True  # không có dữ liệu → không chặn
        ok = adx >= self._adx_filter_min
        if not ok:
            logger.debug("SonicR ADX=%.1f < min=%.1f → skip", adx, self._adx_filter_min)
        return ok

    # ── Opt-11: Linear Regression Slope Filter ────────────────────────────────

    def _linreg_slope_ok(self, df: pd.DataFrame, direction: str) -> bool:
        """
        Tính hồi quy tuyến tính trên EMA89 trong linreg_slope_lookback nến gần nhất.
        Nếu |slope| < linreg_slope_thresh (pips/bar) → EMA đi ngang → bỏ qua.
        BUY: slope phải > +thresh; SELL: slope phải < -thresh.
        """
        if not self._use_linreg_slope:
            return True
        n = self._linreg_slope_lookback
        if len(df) < n + 2:
            return True
        ema89_vals = df["ema89"].iloc[-n:].values.astype(float)
        try:
            x = np.arange(len(ema89_vals), dtype=float)
            slope_price = float(np.polyfit(x, ema89_vals, 1)[0])
            slope_pips = slope_price / self._pip_size()
            ok = (slope_pips > self._linreg_slope_thresh) if direction == "BUY" \
                else (slope_pips < -self._linreg_slope_thresh)
            if not ok:
                logger.debug(
                    "SonicR linreg EMA89 slope %.3f pips/bar not %s %.3f → skip",
                    slope_pips,
                    f"> {self._linreg_slope_thresh:.3f}" if direction == "BUY"
                    else f"< -{self._linreg_slope_thresh:.3f}",
                    self._linreg_slope_thresh,
                )
            return ok
        except Exception:
            return True

    # ── Opt-12: Dragon Tunnel Zigzag Filter ──────────────────────────────────

    def _dragon_no_zigzag(self, df: pd.DataFrame) -> bool:
        """
        Đếm số lần giá đổi vùng qua dải Dragon (pac_high / pac_low):
          +1 = close > pac_high (trên dải)
          -1 = close < pac_low  (dưới dải)
           0 = trong dải (bị bỏ qua khi đếm)

        Nếu số lần chuyển vùng (±1→∓1) ≥ dragon_zigzag_max_crosses → sideway → skip.
        """
        if not self._dragon_zigzag_filter:
            return True
        n = self._dragon_zigzag_lookback
        if len(df) < n + 2:
            return True
        window = df.iloc[-(n + 1):]

        def _zone(row: pd.Series) -> int:
            if float(row["close"]) > float(row["pac_high"]):
                return 1
            if float(row["close"]) < float(row["pac_low"]):
                return -1
            return 0

        zones = window.apply(_zone, axis=1)
        non_zero = zones[zones != 0]
        if len(non_zero) < 2:
            return True  # giá ở trong dải suốt → không đủ dữ liệu để phán định
        # Mỗi lần giá đổi từ +1 sang -1 (hoặc ngược lại) = 1 lần zig-zag
        transitions = int((non_zero != non_zero.shift()).sum()) - 1
        ok = transitions < self._dragon_zigzag_max_crosses
        if not ok:
            logger.debug(
                "SonicR Dragon zigzag: %d zone-switches in %d bars ≥ %d → sideway skip",
                transitions, n, self._dragon_zigzag_max_crosses,
            )
        return ok

    # ── Opt-15: HTF EMA Bias ──────────────────────────────────────────────────

    def _htf_ema_dir(self, df: pd.DataFrame) -> str | None:
        """
        Resample df lên khung lớn hơn (htf_resample, mặc định "1h" = H1),
        tính EMA34 và EMA89 trên dữ liệu đã resample, và trả về:
          "BUY"  — EMA34(HTF) > EMA89(HTF)  → xu hướng tăng H1
          "SELL" — EMA34(HTF) < EMA89(HTF)  → xu hướng giảm H1
          None   — không đủ dữ liệu hoặc EMA bằng nhau

        Resampling từ M5 → H1 cần ít nhất htf_ema_slow × 12 + 50 nến M5.
        Không thay đổi df gốc.
        """
        min_needed = self._htf_ema_slow * 12 + 50
        if len(df) < min_needed:
            logger.debug(
                "SonicR HTF: not enough bars (%d < %d needed)", len(df), min_needed
            )
            return None
        try:
            raw = df[["timestamp", "open", "high", "low", "close", "volume"]].copy()
            raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True)
            raw = raw.set_index("timestamp")
            htf = raw.resample(self._htf_resample).agg(
                {"open": "first", "high": "max", "low": "min",
                 "close": "last", "volume": "sum"}
            ).dropna(subset=["close"])

            if len(htf) < self._htf_ema_slow + 5:
                return None

            ema_fast = ema_mt5(htf["close"], self._htf_ema_fast)
            ema_slow = ema_mt5(htf["close"], self._htf_ema_slow)

            v_fast = float(ema_fast.iloc[-1])
            v_slow = float(ema_slow.iloc[-1])
            v_close = float(htf["close"].iloc[-1])

            if pd.isna(v_fast) or pd.isna(v_slow):
                return None
            if v_fast == v_slow:
                return None

            direction = "BUY" if v_fast > v_slow else "SELL"

            # Opt-15 extra: close(HTF) phải nằm đúng bên EMA34(HTF)
            if self._htf_require_close_vs_ema:
                price_ok = (
                    (direction == "BUY"  and v_close > v_fast) or
                    (direction == "SELL" and v_close < v_fast)
                )
                if not price_ok:
                    logger.debug(
                        "SonicR HTF close filter: close=%.5f %s EMA%d(HTF)=%.5f → block %s",
                        v_close,
                        "not >" if direction == "BUY" else "not <",
                        self._htf_ema_fast, v_fast, direction,
                    )
                    return None

            logger.debug(
                "SonicR HTF EMA%d/EMA%d (%s): ema=%.5f/%.5f close=%.5f → %s bias",
                self._htf_ema_fast, self._htf_ema_slow,
                self._htf_resample, v_fast, v_slow, v_close, direction,
            )
            return direction
        except Exception:
            logger.debug("SonicR HTF EMA: resample error", exc_info=True)
            return None

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

    def _pip_size(self) -> float:
        sym = self.symbol.upper()
        if sym in ("XAUUSD", "XAUUSDM", "XAGUSD"):
            return 0.10
        if "JPY" in sym:
            return 0.01
        return 0.0001

    def _price_to_pips(self, distance: float) -> float:
        """
        XAUUSD/XAGUSD : 1 pip = $0.10  (1 USD move = 10 pips)
        JPY pairs     : 1 pip = 0.01
        Standard Forex: 1 pip = 0.0001
        """
        return distance / self._pip_size()
