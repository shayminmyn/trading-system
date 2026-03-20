"""
Paper-trade exit check — same SL-before-TP bar priority as BacktestEngine._find_outcome.
"""

from __future__ import annotations


def paper_bar_exit(
    is_buy: bool,
    high: float,
    low: float,
    sl: float,
    tp: float,
) -> str | None:
    """
    Return 'SL', 'TP', or None if neither level touched this bar.

    BUY : SL if low <= sl, else TP if high >= tp
    SELL: SL if high >= sl, else TP if low <= tp
    """
    if is_buy:
        if low <= sl:
            return "SL"
        if high >= tp:
            return "TP"
    else:
        if high >= sl:
            return "SL"
        if low <= tp:
            return "TP"
    return None
