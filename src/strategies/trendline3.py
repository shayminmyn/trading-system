"""
Trendline 3-Point Touch Strategy
==================================
Vào lệnh tại lần chạm thứ 3 (hoặc thứ N) của một đường trendline 2 điểm.

  Downtrend SELL: nối 2 swing high P1 > P2 → khi giá chạm lần 3 → SELL
  Uptrend   BUY : nối 2 swing low  P1 < P2 → khi giá chạm lần 3 → BUY

Toán học:
  slope          = (P2.price - P1.price) / (P2_bar_idx - P1_bar_idx)
  tl_at_bar_i    = P1.price + slope × (i - P1_bar_idx)

Parameters
----------
swing_lookback          : int    5      Số nến mỗi bên để xác nhận swing high/low
swing_history_bars      : int  300      Chỉ tìm swing trong N nến gần nhất (0 = không giới hạn)
min_bars_between_peaks  : int   10      Khoảng cách tối thiểu giữa P1 và P2 (nến)
min_bars_after_p2       : int    5      Khoảng cách tối thiểu từ P2 đến P3
touch_tolerance_atr     : float  0.5    Ngưỡng chạm = N × ATR (âm = từ dưới, dương = từ trên)
min_slope_deg           : float  3.0    Góc dốc tối thiểu (độ) — tránh trendline nằm ngang
max_slope_deg           : float 60.0    Góc dốc tối đa (độ) — tránh trendline gần thẳng đứng
require_bearish_candle  : bool   True   P3 phải là nến giảm (SELL) / tăng (BUY)
require_wick_rejection  : bool   False  Bật: đòi hỏi râu nến từ chối trendline
wick_ratio              : float  0.4    Tỷ lệ râu từ chối / phạm vi nến
max_approach_body_ratio : float  0.85   Bỏ qua nếu nến tiếp cận là Marubozu mạnh ngược chiều
sl_buffer_atr           : float  0.3    Buffer ATR thêm vào SL phía ngoài P1
limit_entry             : bool   True   Dùng limit order (SELL LIMIT / BUY LIMIT)
limit_expiry_bars       : int    5      Hết hạn sau N nến nếu không fill
touch_count_min         : int    1      Lần chạm thứ mấy bắt đầu vào lệnh (1=lần 3 total)
touch_count_max         : int    1      Lần chạm thứ mấy dừng vào lệnh (1=chỉ lần 3 total)
enable_sell             : bool   True   Bật tín hiệu SELL (downtrend)
enable_buy              : bool   True   Bật tín hiệu BUY  (uptrend)
atr_period              : int   14      ATR period
min_rr                  : float  2.0    Tỷ lệ R:R tối thiểu để chấp nhận lệnh
max_sl_pips             : float  0.0    Giới hạn SL tối đa (0 = tắt)
breakeven_at_r          : float  0.0    Dời SL về BE khi lời ≥ N×SL (0 = tắt)
partial_close_at_r      : float  0.0    Partial close (0 = tắt)
partial_close_ratio     : float  0.5
partial_trail_pips      : float  5.0
signal_ttl_bars         : int    0      Live concurrent gate TTL (0 = tắt)
max_concurrent_trades   : int    0      Giới hạn lệnh đồng thời (0 = không giới hạn)
"""

from __future__ import annotations

import math
from typing import NamedTuple

import numpy as np
import pandas as pd
from ta.volatility import AverageTrueRange

from .base_strategy import BaseStrategy, Signal
from ..utils.logger import get_logger

logger = get_logger("trendline3")


class _Peak(NamedTuple):
    bar_idx: int
    price: float


class TrendLine3Strategy(BaseStrategy):
    """
    Trendline 3-Point Touch — vào lệnh khi giá chạm đường trendline lần 3.

    Hoạt động cho cả hai chiều:
      • SELL: nối 2 swing high (P1 > P2) → SELL khi chạm lần 3
      • BUY : nối 2 swing low  (P1 < P2) → BUY  khi chạm lần 3
    """

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        parameters: dict | None = None,
    ) -> None:
        super().__init__(symbol, timeframe, parameters)
        p = self.parameters

        self._swing_lookback: int          = max(2, int(p.get("swing_lookback", 5)))
        # Chỉ tìm swing highs/lows trong N nến gần nhất (0 = không giới hạn)
        self._swing_history: int           = max(0, int(p.get("swing_history_bars", 300)))
        self._min_bars_between: int        = max(3, int(p.get("min_bars_between_peaks", 10)))
        self._min_bars_after_p2: int       = max(1, int(p.get("min_bars_after_p2", 5)))
        self._touch_tol_atr: float         = float(p.get("touch_tolerance_atr", 0.5))
        self._min_slope_deg: float         = float(p.get("min_slope_deg", 3.0))
        self._max_slope_deg: float         = float(p.get("max_slope_deg", 60.0))
        self._require_bearish: bool        = bool(p.get("require_bearish_candle", True))
        self._require_wick_rej: bool       = bool(p.get("require_wick_rejection", False))
        self._wick_ratio: float            = float(p.get("wick_ratio", 0.4))
        self._max_approach_body: float     = float(p.get("max_approach_body_ratio", 0.85))
        self._sl_buffer_atr: float         = float(p.get("sl_buffer_atr", 0.3))
        self._limit_entry: bool            = bool(p.get("limit_entry", True))
        self._limit_expiry: int            = int(p.get("limit_expiry_bars", 5))
        self._touch_min: int               = max(1, int(p.get("touch_count_min", 1)))
        self._touch_max: int               = max(1, int(p.get("touch_count_max", 1)))
        self._enable_sell: bool            = bool(p.get("enable_sell", True))
        self._enable_buy: bool             = bool(p.get("enable_buy", True))
        self._atr_period: int              = max(5, int(p.get("atr_period", 14)))
        self._min_rr: float                = float(p.get("min_rr", 2.0))
        self._max_sl_pips: float           = float(p.get("max_sl_pips", 0.0))
        self._breakeven_at_r: float        = float(p.get("breakeven_at_r", 0.0))
        self._partial_close_at_r: float    = float(p.get("partial_close_at_r", 0.0))
        self._partial_close_ratio: float   = float(p.get("partial_close_ratio", 0.5))
        self._partial_trail_pips: float    = float(p.get("partial_trail_pips", 5.0))

        self._min_bars = max(50, self._swing_lookback * 2 + self._min_bars_between + 10)

    # ── BaseStrategy interface ────────────────────────────────────────────────

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Thêm cột ATR. Swing detection thực hiện trong generate_signal."""
        atr_ind = AverageTrueRange(
            df["high"], df["low"], df["close"], window=self._atr_period
        )
        df["atr"] = atr_ind.average_true_range()
        return df

    def generate_signal(self, df: pd.DataFrame) -> Signal:
        if len(df) < self._min_bars:
            return self._no_signal()

        curr = df.iloc[-1]
        atr = float(curr.get("atr", 0))
        if not math.isfinite(atr) or atr <= 0:
            return self._no_signal()

        # ── SELL: downtrend trendline (swing highs) ───────────────────────────
        if self._enable_sell:
            sig = self._check_trendline(df, curr, atr, direction="SELL")
            if sig is not None:
                return sig

        # ── BUY: uptrend trendline (swing lows) ──────────────────────────────
        if self._enable_buy:
            sig = self._check_trendline(df, curr, atr, direction="BUY")
            if sig is not None:
                return sig

        return self._no_signal()

    # ── Core trendline logic ─────────────────────────────────────────────────

    def _check_trendline(
        self, df: pd.DataFrame, curr: pd.Series, atr: float, direction: str
    ) -> Signal | None:
        is_sell = direction == "SELL"
        curr_idx = len(df) - 1

        # 1. Tìm swing highs (SELL) hoặc swing lows (BUY) đã xác nhận
        peaks = self._find_swings(df, is_sell)
        if len(peaks) < 2:
            return None

        # 2. Tìm cặp P1/P2 hợp lệ gần nhất
        p1, p2 = self._find_p1_p2(peaks, curr_idx, is_sell)
        if p1 is None or p2 is None:
            return None

        # 3. P3 phải đủ xa P2
        if curr_idx - p2.bar_idx < self._min_bars_after_p2:
            return None

        # 4. Tính slope và kiểm tra góc dốc
        slope = (p2.price - p1.price) / (p2.bar_idx - p1.bar_idx)
        if not self._slope_angle_ok(slope, atr):
            return None

        # 5. Tính giá trendline tại nến hiện tại
        tl_price = p1.price + slope * (curr_idx - p1.bar_idx)

        # 6. Kiểm tra nến hiện tại có chạm trendline không
        tolerance = atr * self._touch_tol_atr
        if is_sell:
            # High phải chạm trendline từ dưới lên (test kháng cự)
            if curr["high"] < tl_price - tolerance:
                return None
            # Close phải nằm DƯỚI trendline (bị từ chối)
            if curr["close"] >= tl_price:
                return None
        else:
            # Low phải chạm trendline từ trên xuống (test hỗ trợ)
            if curr["low"] > tl_price + tolerance:
                return None
            # Close phải nằm TRÊN trendline (bị từ chối)
            if curr["close"] <= tl_price:
                return None

        # 7. Đếm số lần chạm sau P2 (không tính nến hiện tại)
        post_p2_touches = self._count_post_p2_touches(
            df, p1, p2, curr_idx, atr, is_sell
        )
        # post_p2_touches=0 → đây là lần chạm thứ 3 tổng cộng
        current_touch_num = post_p2_touches + 1   # 1-indexed: 1=lần chạm đầu sau P2 (3rd total)
        if current_touch_num < self._touch_min or current_touch_num > self._touch_max:
            logger.debug(
                "TL3 %s: touch#%d out of range [%d,%d] — skip",
                direction, current_touch_num, self._touch_min, self._touch_max,
            )
            return None

        # 8. Filter: nến tiếp cận ngược chiều quá mạnh
        if len(df) >= 2:
            prev = df.iloc[-2]
            if self._approach_too_strong(prev, not is_sell):
                logger.debug("TL3 %s: approach candle too strong — skip", direction)
                return None

        # 9. Filter: yêu cầu nến đúng chiều
        if self._require_bearish:
            if is_sell and curr["close"] >= curr["open"]:
                return None
            if not is_sell and curr["close"] <= curr["open"]:
                return None

        # 10. Filter: yêu cầu râu từ chối
        if self._require_wick_rej:
            if is_sell and not self._has_upper_wick(curr):
                return None
            if not is_sell and not self._has_lower_wick(curr):
                return None

        # 11. Tính SL và kiểm tra RR
        entry = tl_price
        if is_sell:
            sl_level = p1.price + atr * self._sl_buffer_atr   # trên P1
        else:
            sl_level = p1.price - atr * self._sl_buffer_atr   # dưới P1

        sl_distance = abs(entry - sl_level)
        if sl_distance <= 0:
            return None

        sl_pips = sl_distance / self._pip_size()
        if sl_pips <= 0:
            return None

        if self._max_sl_pips > 0 and sl_pips > self._max_sl_pips:
            logger.debug(
                "TL3 %s: sl_pips=%.0f > max=%.0f — skip",
                direction, sl_pips, self._max_sl_pips,
            )
            return None

        # 12. Kiểm tra RR tối thiểu
        if self._min_rr > 0:
            tp_distance = sl_distance * self._min_rr
            if is_sell:
                tp_check = entry - tp_distance
                if tp_check >= entry:  # không hợp lý
                    return None
            else:
                tp_check = entry + tp_distance
                if tp_check <= entry:
                    return None

        lim = entry if self._limit_entry else 0.0
        action = "SELL" if is_sell else "BUY"
        return self._make_signal(
            action,
            entry,
            sl_pips,
            (
                f"TL3-{action} | P1={p1.price:.5f}(#{p1.bar_idx})"
                f" P2={p2.price:.5f}(#{p2.bar_idx})"
                f" TL={tl_price:.5f}"
                f" touch#{current_touch_num}"
                f" sl={sl_pips:.0f}p"
            ),
            limit_price=lim,
            limit_expiry_bars=self._limit_expiry,
            sl_level=sl_level,
            breakeven_at_r=self._breakeven_at_r,
            partial_close_at_r=self._partial_close_at_r,
            partial_close_ratio=self._partial_close_ratio,
            partial_trail_pips=self._partial_trail_pips,
        )

    # ── Swing detection ──────────────────────────────────────────────────────

    def _find_swings(self, df: pd.DataFrame, is_sell: bool) -> list[_Peak]:
        """
        Tìm tất cả swing high (is_sell=True) hoặc swing low (is_sell=False) đã
        được xác nhận (có đủ swing_lookback nến ở mỗi bên).

        Chỉ quét trong swing_history_bars nến gần nhất (0 = toàn bộ lịch sử).
        Bar index trong kết quả luôn là index tuyệt đối trong df gốc.
        Không dùng lookahead: bỏ swing_lookback nến cuối (chưa đủ nến để xác nhận).
        """
        lb = self._swing_lookback
        n_total = len(df)
        
        start_abs = max(lb, n_total - self._swing_history) if self._swing_history > 0 else lb
        # Chỉ lấy phần dữ liệu cần thiết để vector hóa
        prices = df["high"].values if is_sell else df["low"].values
        peaks: list[_Peak] = []

        # Quét từ start_abs đến n_total - lb (bỏ lb nến cuối — chưa xác nhận)
        for i in range(start_abs, n_total - lb):
            val = prices[i]
            
            # Lấy mảng bên trái và bên phải nến hiện tại
            left_side = prices[i - lb : i]
            right_side = prices[i + 1 : i + lb + 1]
            
            if is_sell:
                # Điều kiện: Cao hơn hẳn bên trái VÀ Cao hơn hoặc bằng bên phải 
                # (để tránh lấy trùng 2 đỉnh bằng nhau liên tiếp)
                if all(val > left_side) and all(val >= right_side):
                    peaks.append(_Peak(i, float(val)))
            else:
                if all(val < left_side) and all(val <= right_side):
                    peaks.append(_Peak(i, float(val)))

        return peaks

    def _find_p1_p2(
        self,
        peaks: list[_Peak],
        curr_idx: int,
        is_sell: bool,
    ) -> tuple[_Peak | None, _Peak | None]:
        """
        Tìm cặp (P1, P2) gần nhất thoả:
          SELL: P1.price > P2.price (đỉnh sau thấp hơn đỉnh trước → downtrend)
          BUY : P1.price < P2.price (đáy sau cao hơn đáy trước → uptrend)
          khoảng cách P1→P2 >= min_bars_between
          P2 phải trước curr_idx ít nhất min_bars_after_p2 bars
        Ưu tiên cặp (P1, P2) có P2 gần nhất.
        """
        for j in range(len(peaks) - 1, 0, -1):
            p2 = peaks[j]
            if curr_idx - p2.bar_idx < self._min_bars_after_p2:
                continue
            for i in range(j - 1, -1, -1):
                p1 = peaks[i]
                if p2.bar_idx - p1.bar_idx < self._min_bars_between:
                    continue
                if is_sell and p1.price <= p2.price:
                    continue   # P1 phải cao hơn P2 (downtrend)
                if not is_sell and p1.price >= p2.price:
                    continue   # P1 phải thấp hơn P2 (uptrend)
                return p1, p2
        return None, None

    # ── Touch counting ───────────────────────────────────────────────────────

    def _count_post_p2_touches(
        self,
        df: pd.DataFrame,
        p1: _Peak,
        p2: _Peak,
        curr_idx: int,
        atr: float,
        is_sell: bool,
    ) -> int:
        """
        Đếm số lần chạm trendline sau P2 (không tính nến hiện tại).
        Gộp các nến liên tiếp thành 1 sự kiện để tránh đếm trùng.
        """
        slope = (p2.price - p1.price) / (p2.bar_idx - p1.bar_idx)
        tolerance = atr * self._touch_tol_atr
        prices = df["high"].values if is_sell else df["low"].values

        count = 0
        in_touch = False   # đang trong một sự kiện chạm

        for i in range(p2.bar_idx + 1, curr_idx):
            tl = p1.price + slope * (i - p1.bar_idx)
            if is_sell:
                touching = float(prices[i]) >= tl - tolerance
            else:
                touching = float(prices[i]) <= tl + tolerance

            if touching and not in_touch:
                count += 1
                in_touch = True
            elif not touching:
                in_touch = False

        return count

    # ── Slope filter ─────────────────────────────────────────────────────────

    def _slope_angle_ok(self, slope_price_per_bar: float, atr: float) -> bool:
        """
        Kiểm tra góc của trendline trong khoảng [min_slope_deg, max_slope_deg].
        Dùng ATR để chuẩn hoá: slope_norm = slope / atr_per_bar
        Sau đó tính arctan để ra góc độ.
        """
        if atr <= 0:
            return True
        angle_rad = math.atan(abs(slope_price_per_bar) / atr)
        angle_deg = math.degrees(angle_rad)
        ok = self._min_slope_deg <= angle_deg <= self._max_slope_deg
        if not ok:
            logger.debug(
                "TL3 slope angle=%.1f° outside [%.1f°, %.1f°]",
                angle_deg, self._min_slope_deg, self._max_slope_deg,
            )
        return ok

    # ── Candle filters ───────────────────────────────────────────────────────

    def _approach_too_strong(self, bar: pd.Series, is_bull: bool) -> bool:
        """
        Trả True nếu nến tiếp cận là Marubozu mạnh theo chiều ngược lại.
        Ngăn vào lệnh khi momentum ngược quá mạnh.
        """
        rng = float(bar["high"]) - float(bar["low"])
        if rng <= 0:
            return False
        body = abs(float(bar["close"]) - float(bar["open"]))
        body_ratio = body / rng
        if body_ratio < self._max_approach_body:
            return False
        # Nến phải đúng chiều ngược lại để bị coi là nguy hiểm
        if is_bull:
            return float(bar["close"]) > float(bar["open"])
        else:
            return float(bar["close"]) < float(bar["open"])

    def _has_upper_wick(self, bar: pd.Series) -> bool:
        """Có râu trên đủ dài (từ chối kháng cự) — dùng cho SELL."""
        rng = float(bar["high"]) - float(bar["low"])
        if rng <= 0:
            return False
        upper_wick = float(bar["high"]) - max(float(bar["open"]), float(bar["close"]))
        return (upper_wick / rng) >= self._wick_ratio

    def _has_lower_wick(self, bar: pd.Series) -> bool:
        """Có râu dưới đủ dài (từ chối hỗ trợ) — dùng cho BUY."""
        rng = float(bar["high"]) - float(bar["low"])
        if rng <= 0:
            return False
        lower_wick = min(float(bar["open"]), float(bar["close"])) - float(bar["low"])
        return (lower_wick / rng) >= self._wick_ratio

    # ── Pip size ─────────────────────────────────────────────────────────────

    def _pip_size(self) -> float:
        s = self.symbol.upper()
        if s.startswith("XAUUSD") or s.startswith("XAU_USD"):
            return 0.10
        if s.startswith("XAGUSD"):
            return 0.01
        if "JPY" in s:
            return 0.01
        return 0.0001
