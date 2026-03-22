"""
SonicRM15 Strategy — SonicR tối ưu cho khung M15.

Clone từ SonicRStrategy với 3 tối ưu bật sẵn:

  Opt-1  EMA Slope Filter   – chỉ trade khi EMA89 có góc nghiêng rõ ràng
  Opt-4  ATR-based SL       – SL = Entry ± 1.5 × ATR (tự giãn/thu hẹp theo volatility)
  Opt-5  Close-confirmation – dùng close thay vì wick để kiểm tra chạm EMA34/89
                              (quy tắc "chốt cửa mới vào nhà")

Tất cả tham số đều có thể ghi đè trong config.yaml dưới section SonicRM15.
Các tối ưu khác của SonicRStrategy (Opt-2 swing SL, Opt-3 Marubozu) đều sẵn
sàng nhưng mặc định tắt; bật qua config khi cần.
"""

from __future__ import annotations

from typing import Any

from src.strategies.sonicr import SonicRStrategy


class SonicRM15Strategy(SonicRStrategy):
    """
    SonicR tối ưu cho M15: slope filter + ATR SL + close-confirmation touch.

    Kế thừa toàn bộ logic từ SonicRStrategy; chỉ ghi đè giá trị mặc định
    phù hợp với timeframe M15 (và XAUUSD).
    """

    # Default parameters for M15 — all overridable via config.yaml
    _M15_DEFAULTS: dict[str, Any] = {
        # ── Opt-1: EMA Slope ──────────────────────────────────────────────
        "require_ema_slope": True,
        # M15: mỗi nến khoảng $0.5-2; 0.2 pips/bar ≈ góc 30° ở khung này
        "min_slope_pips_per_bar": 0.3,
        # Dùng nhiều nến hơn để tính slope trên M15 (bằng H1 ≈ 4 bars)
        "slope_lookback": 10,

        # ── Opt-4: ATR SL ────────────────────────────────────────────────
        "use_atr_sl": True,
        "atr_sl_mult": 1.5,

        # ── Opt-5: Close-confirmation touch ───────────────────────────────
        "use_close_for_ema_touch": True,

        # ── Entry mode: market order khi close xác nhận ────────────────
        "limit_entry": False,

        # ── M15 lookback windows ──────────────────────────────────────────
        # 4 M15 bars ≈ 1 H1 bar → nhân hệ số ~4 so với H1 defaults
        "pullback_lookback": 60,
        "extension_lookback": 40,

        # ── Rejection priority: RR tốt hơn trên M15 ──────────────────────
        "rejection_priority": True,

        # ── SL cap (pips) ─────────────────────────────────────────────────
        "max_sl_pips": 0.0,  # off by default; bật nếu cần

        # ── Break-even ────────────────────────────────────────────────────
        "breakeven_at_r": 1.0,
    }

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        parameters: dict[str, Any] | None = None,
    ) -> None:
        merged = {**self._M15_DEFAULTS, **(parameters or {})}
        super().__init__(symbol, timeframe, merged)
