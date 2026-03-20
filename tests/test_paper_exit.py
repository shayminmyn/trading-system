"""Unit tests for paper_bar_exit (same bar priority as backtest)."""

import pytest

from src.utils.paper_exit import paper_bar_exit


def test_buy_sl_before_tp():
    assert paper_bar_exit(True, 1.10, 0.90, sl=1.0, tp=1.05) == "SL"
    assert paper_bar_exit(True, 1.10, 0.99, sl=1.0, tp=1.05) == "TP"


def test_sell_sl_before_tp():
    assert paper_bar_exit(False, 1.10, 0.90, sl=1.0, tp=0.95) == "SL"
    assert paper_bar_exit(False, 1.01, 0.90, sl=1.0, tp=0.95) == "TP"


def test_no_touch():
    assert paper_bar_exit(True, 1.02, 1.01, sl=1.0, tp=1.05) is None
