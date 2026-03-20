"""Unit tests for RiskManager."""

import pytest
from src.strategies.base_strategy import Signal
from src.risk.risk_manager import RiskManager, CompleteSignal

SAMPLE_CONFIG = {
    "risk_management": {
        "account_balance": 10000,
        "risk_per_trade_percent": 1.5,
        "default_rr_ratio": 2.0,
        "min_lot_size": 0.01,
        "max_lot_size": 10.0,
    }
}


@pytest.fixture
def rm():
    return RiskManager(SAMPLE_CONFIG)


def _make_signal(symbol, action, entry, sl_pips) -> Signal:
    return Signal(
        action=action, symbol=symbol, timeframe="H1",
        strategy_name="test", entry=entry, sl_pips=sl_pips,
    )


class TestLotSizeCalculation:
    def test_xauusd_buy_lot(self, rm):
        """XAUUSD: balance=$10,000, risk=1.5%, SL=55pips → $150 risk
        pip_value=10.0 (1 pip=$0.10, 100oz lot), lot = 150 / (55 * 10) = 0.27
        """
        signal = _make_signal("XAUUSD", "BUY", 2150.50, 55.0)
        result = rm.build_complete_signal(signal)
        assert result is not None
        # pip_value=10.0, lot = 150 / (55 * 10.0) = 0.27
        assert result.volume == pytest.approx(0.27, abs=0.01)
        assert result.risk_amount_usd == pytest.approx(150.0, abs=0.01)

    def test_eurusd_buy_lot(self, rm):
        """EURUSD: balance=$10,000, risk=1.5%, SL=30pips → $150 risk"""
        signal = _make_signal("EURUSD", "BUY", 1.0850, 30.0)
        result = rm.build_complete_signal(signal)
        assert result is not None
        # pip_value=10.0, lot = 150 / (30 * 10) = 0.50
        assert result.volume == pytest.approx(0.50, abs=0.01)

    def test_lot_clamped_to_max(self, rm):
        """Very small SL should not produce lot > max_lot_size."""
        signal = _make_signal("EURUSD", "BUY", 1.0850, 0.5)
        result = rm.build_complete_signal(signal)
        assert result is not None
        assert result.volume <= 10.0

    def test_lot_clamped_to_min(self, rm):
        """Very large SL should not produce lot < min_lot_size."""
        signal = _make_signal("EURUSD", "BUY", 1.0850, 5000.0)
        result = rm.build_complete_signal(signal)
        assert result is not None
        assert result.volume >= 0.01


class TestSLTPCalculation:
    def test_xauusd_buy_sl_tp(self, rm):
        """1 pip = $0.10, so SL 55 pips = $5.50 distance."""
        signal = _make_signal("XAUUSD", "BUY", 2150.50, 55.0)
        result = rm.build_complete_signal(signal)
        assert result is not None
        expected_sl  = 2150.50 - (55 * 0.10)   # 2145.00
        expected_tp1 = 2150.50 + (55 * 0.10 * 2.0)  # 2161.50
        assert result.sl  == pytest.approx(expected_sl,  abs=0.01)
        assert result.tp1 == pytest.approx(expected_tp1, abs=0.01)

    def test_eurusd_sell_sl_tp(self, rm):
        signal = _make_signal("EURUSD", "SELL", 1.0850, 30.0)
        result = rm.build_complete_signal(signal)
        assert result is not None
        expected_sl = 1.0850 + (30 * 0.0001)
        expected_tp1 = 1.0850 - (30 * 0.0001 * 2.0)
        assert result.sl == pytest.approx(expected_sl, abs=0.00001)
        assert result.tp1 == pytest.approx(expected_tp1, abs=0.00001)

    def test_tp2_is_larger_than_tp1_for_buy(self, rm):
        signal = _make_signal("XAUUSD", "BUY", 2150.0, 40.0)
        result = rm.build_complete_signal(signal)
        assert result.tp2 > result.tp1

    def test_tp2_is_smaller_than_tp1_for_sell(self, rm):
        signal = _make_signal("XAUUSD", "SELL", 2150.0, 40.0)
        result = rm.build_complete_signal(signal)
        assert result.tp2 < result.tp1


class TestEdgeCases:
    def test_none_signal_returns_none(self, rm):
        signal = _make_signal("XAUUSD", "NONE", 2150.0, 0.0)
        assert rm.build_complete_signal(signal) is None

    def test_zero_sl_pips_returns_none(self, rm):
        signal = _make_signal("XAUUSD", "BUY", 2150.0, 0.0)
        assert rm.build_complete_signal(signal) is None

    def test_pip_value_xauusd(self, rm):
        # 1 pip = $0.10, lot = 100 oz → pip_value = $10/lot
        assert rm.get_pip_value("XAUUSD") == pytest.approx(10.0)

    def test_pip_value_eurusd(self, rm):
        assert rm.get_pip_value("EURUSD") == pytest.approx(10.0)

    def test_complete_signal_str(self, rm):
        signal = _make_signal("XAUUSD", "BUY", 2150.0, 55.0)
        result = rm.build_complete_signal(signal)
        assert "BUY" in str(result)
        assert "XAUUSD" in str(result)
