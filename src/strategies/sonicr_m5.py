"""
SonicRM5 Strategy — SonicR tối ưu cho khung M5.

Clone từ SonicRStrategy với các tối ưu bật sẵn:

  Opt-1  EMA Slope Filter       – chỉ trade khi EMA89 dốc rõ ràng theo chiều tín hiệu
  Opt-3  Marubozu Confirmation  – bật bộ lọc fakeout cho PAC-Breakout (M5 rất nhiều bẫy)
                                   Nếu nến breakout không phải Marubozu → đợi retest
  Opt-4  ATR-based SL           – SL = Entry ± 1.5 × ATR_M5 (tự co giãn theo volatility)
  Opt-5  Close-confirmation     – chỉ xác nhận EMA touch khi close nằm trên/dưới EMA
                                   (quy tắc "chốt cửa mới vào nhà" — quan trọng hơn trên M5)
  Opt-8  Partial Close          – chốt 50% tại R:R=1:1 → dời SL về entry + BE
  Opt-13 Absolute EMA Gap       – khoảng cách EMA34/89 phải > 50 pips (loại sideway)
  Opt-14 Minimum SL Width       – SL tối thiểu 150 pips; kéo ra nếu ATR tính hẹp hơn
  Opt-15 HTF EMA Bias (H1)      – chỉ BUY khi EMA34(H1) > EMA89(H1); chỉ SELL ngược lại

Tham số mặc định được hiệu chỉnh cho:
  • XAUUSD M5: H-L trung bình ~50–155 pips/nến, biến động cao
  • Lookback windows: ×12 so với H1, ×3 so với M15
  • LinReg slope: 0.3 pips/bar ≈ 20° nghiêng rõ ràng trên XAUUSD M5
  • Break-even: dời SL về entry khi đạt +1R

Mọi tham số đều có thể ghi đè trong config.yaml dưới section SonicRM5.
"""

from __future__ import annotations

from typing import Any

from src.strategies.sonicr import SonicRStrategy


class SonicRM5Strategy(SonicRStrategy):
    """
    SonicR tối ưu cho M5: slope filter + Marubozu BO filter + ATR SL + close-confirmation.

    Kế thừa toàn bộ logic từ SonicRStrategy; chỉ ghi đè defaults phù hợp M5.
    """

    # Default parameters for M5 XAUUSD — all overridable via config.yaml
    _M5_DEFAULTS: dict[str, Any] = {
        # ── Opt-1: EMA Slope Filter ───────────────────────────────────────────
        "require_ema_slope": True,
        "min_slope_pips_per_bar": 0.1,  # 0.1 pips/bar cho M5 XAUUSD
        "slope_lookback": 30,           # 30 nến M5 ≈ 2.5 giờ

        # ── Opt-3: Marubozu Confirmation (PAC Breakout) ───────────────────────
        "require_bo_confirmation": True,
        "bo_marubozu_ratio": 0.75,

        # ── Opt-4: ATR-based SL ───────────────────────────────────────────────
        "use_atr_sl": True,
        "atr_sl_mult": 1.5,             # SL = Entry ± 1.5 × ATR_M5

        # ── Opt-5: Close-confirmation touch ───────────────────────────────────
        "use_close_for_ema_touch": True,

        # ── Opt-6: EMA200 global trend filter ────────────────────────────────
        # BUY chỉ khi close > EMA200; SELL ngược lại — lọc 60-70% tín hiệu ngược trend
        "require_ema200_trend": True,

        # ── Opt-7: Max SL cap theo ATR ────────────────────────────────────────
        # Bỏ lệnh nếu SL > 5×ATR_M5 (SL quá rộng so với volatility hiện tại)
        "max_sl_atr_mult": 5.0,

        # ── Opt-8: Partial close / TP1 scalp ─────────────────────────────────
        # Chốt 50% tại R:R=1:1 → dời SL về entry (break-even)
        "partial_close_at_r": 1.0,
        "partial_close_ratio": 0.5,
        "partial_trail_pips": 5.0,

        # ── Opt-9: Phiên giao dịch (Time Filter) ─────────────────────────────
        "allowed_hours_utc": [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 19, 20, 21],

        # ── Opt-10: ADX Filter ────────────────────────────────────────────────
        # ADX < 25 → thị trường tích lũy/đi ngang → không vào lệnh
        "adx_period": 14,
        "adx_filter_min": 25.0,

        # ── Opt-11: Linear Regression Slope Filter ────────────────────────────
        # Dùng hồi quy tuyến tính 20 nến M5 (≈1.7 giờ) trên EMA89
        # slope 0.3 pips/bar ≈ 20° trên biểu đồ XAUUSD M5 (trước: 0.05 → quá lỏng)
        "use_linreg_slope": True,
        "linreg_slope_lookback": 20,
        "linreg_slope_thresh": 0.3,     # 0.3 pips/bar ≈ 20° nghiêng rõ ràng

        # ── Opt-12: Dragon Tunnel Zigzag Filter ───────────────────────────────
        # Giá đổi vùng (above/inside/below Dragon band) ≥ 3 lần trong 10 nến → sideway
        "dragon_zigzag_filter": True,
        "dragon_zigzag_lookback": 10,
        "dragon_zigzag_max_crosses": 3,

        # ── Opt-13: Absolute EMA Gap Filter ───────────────────────────────────
        # Khoảng cách EMA34–EMA89 phải > 50 pips; nếu không → EMA đi sát → sideway
        "min_ema_gap_pips": 50.0,

        # ── Opt-14: Minimum SL Width ──────────────────────────────────────────
        # SL tối thiểu 150 pips — đủ "thở" trên M5 XAUUSD (ATR_M5 ≈ 50–155 pips)
        # Nếu ATR SL / Swing SL hẹp hơn, tự động kéo ra đến 150 pips
        "min_sl_pips": 150.0,

        # ── Opt-15: Higher Timeframe (H1) EMA Bias Filter ─────────────────────
        # Resample M5 → H1; BUY chỉ khi EMA34(H1) > EMA89(H1), và ngược lại.
        "htf_ema_filter": True,
        "htf_resample": "1h",           # M5 → H1
        "htf_ema_fast": 34,             # PAC mid H1
        "htf_ema_slow": 89,             # trend anchor H1

        # ── Entry mode ────────────────────────────────────────────────────────
        "limit_entry": False,

        # ── Lookback windows ──────────────────────────────────────────────────
        "pullback_lookback": 120,       # 120 M5 ≈ 10 giờ
        "extension_lookback": 60,       # 60 M5 ≈ 5 giờ

        # ── Core ──────────────────────────────────────────────────────────────
        "sl_buffer_atr": 0.3,
        "min_ema_separation_atr": 0.5,
        "atr_mult_far": 2.0,
        "enable_pac_signals": True,
        "rejection_priority": True,
        "breakout_max_ema200_dist_pips": 0,
        "rejection_extra_sl_atr": 0.0,
        "vol_ratio_breakout": 0.9,
        "vol_ratio_rejection": 0.8,
        "require_ema89_touch": True,
        "require_ema89_rejection": True,
        "require_strong_candle": True,
        "strong_candle_ratio": 0.4,
        "max_sl_pips": 0,
        "min_rr": 1.5,
        "breakeven_at_r": 1.0,          # dời SL về entry khi đạt +1R
        "enable_sw_signal": False,
    }

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        parameters: dict[str, Any] | None = None,
    ) -> None:
        merged = {**self._M5_DEFAULTS, **(parameters or {})}
        super().__init__(symbol, timeframe, parameters)
