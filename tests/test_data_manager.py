"""Unit tests for DataManager and MockDataSource."""

import time
import threading
import pytest
import pandas as pd

from src.data.mock_source import generate_ohlcv, MockDataStreamer
from src.data.data_manager import DataManager

SAMPLE_CONFIG = {
    "trading_pairs": [
        {"symbol": "XAUUSD", "timeframes": ["H1"], "strategies": ["MACDCrossover"]},
    ],
    "data": {
        "warmup_bars": 50,
        "poll_interval_seconds": 0.05,
        "fallback_source": "mock",
        "historical_dir": "data/historical",
        "mock_replay_from_historical": False,
    },
    "mt5": {},
}


class TestMockDataSource:
    def test_generate_ohlcv_shape(self):
        df = generate_ohlcv("XAUUSD", "H1", n_bars=100, seed=42)
        assert len(df) == 100
        assert list(df.columns) == ["timestamp", "open", "high", "low", "close", "volume"]

    def test_generate_ohlcv_eurusd(self):
        df = generate_ohlcv("EURUSD", "M15", n_bars=50, seed=1)
        assert df["close"].between(0.5, 2.5).all(), "EURUSD prices should be ~1.0 range"

    def test_generate_ohlcv_xauusd(self):
        df = generate_ohlcv("XAUUSD", "H1", n_bars=50, seed=1)
        assert df["close"].between(1500, 3000).all(), "XAUUSD should be ~2000 range"

    def test_ohlcv_high_gte_low(self):
        df = generate_ohlcv("XAUUSD", "H1", n_bars=200, seed=99)
        assert (df["high"] >= df["low"]).all()

    def test_ohlcv_timestamps_increasing(self):
        df = generate_ohlcv("XAUUSD", "H1", n_bars=100, seed=5)
        assert df["timestamp"].is_monotonic_increasing

    def test_different_seeds_different_data(self):
        df1 = generate_ohlcv("XAUUSD", "H1", n_bars=50, seed=1)
        df2 = generate_ohlcv("XAUUSD", "H1", n_bars=50, seed=2)
        assert not df1["close"].equals(df2["close"])

    def test_same_seed_reproducible(self):
        df1 = generate_ohlcv("XAUUSD", "H1", n_bars=50, seed=42)
        df2 = generate_ohlcv("XAUUSD", "H1", n_bars=50, seed=42)
        pd.testing.assert_frame_equal(df1, df2)


class TestMockDataStreamer:
    def test_streamer_calls_callback(self):
        received = []
        lock = threading.Lock()

        def on_bar(symbol, tf, bar):
            with lock:
                received.append((symbol, tf))

        streamer = MockDataStreamer(
            symbols=["XAUUSD"],
            timeframes=["H1"],
            on_new_bar=on_bar,
            speed_multiplier=10000,
        )
        streamer.start()
        time.sleep(0.3)
        streamer.stop()

        assert len(received) >= 1
        assert received[0] == ("XAUUSD", "H1")

    def test_streamer_multiple_symbols(self):
        received = set()
        lock = threading.Lock()

        def on_bar(symbol, tf, bar):
            with lock:
                received.add(symbol)

        streamer = MockDataStreamer(
            symbols=["XAUUSD", "EURUSD"],
            timeframes=["H1"],
            on_new_bar=on_bar,
            speed_multiplier=10000,
        )
        streamer.start()
        time.sleep(0.3)
        streamer.stop()

        assert "XAUUSD" in received
        assert "EURUSD" in received


class TestDataManager:
    def test_init_and_stop(self):
        dm = DataManager(SAMPLE_CONFIG)
        dm.start()
        time.sleep(0.3)
        dm.stop()

    def test_get_data_returns_dataframe(self):
        dm = DataManager(SAMPLE_CONFIG)
        dm.start()
        time.sleep(0.2)
        df = dm.get_data("XAUUSD", "H1")
        dm.stop()
        assert isinstance(df, pd.DataFrame)

    def test_callback_is_invoked(self):
        received = []
        lock = threading.Lock()

        def cb(symbol, tf, df):
            with lock:
                received.append(symbol)

        dm = DataManager(SAMPLE_CONFIG)
        dm.register_callback("XAUUSD", "H1", cb)
        dm.start()
        time.sleep(0.3)
        dm.stop()

        assert len(received) >= 1
