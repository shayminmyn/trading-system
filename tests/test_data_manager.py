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


class TestMT5MergeClosedBar:
    """Logic buffer MT5 (không cần Windows / MetaTrader5)."""

    def test_merge_replace_same_timestamp_final_ohlc(self):
        dm = DataManager(SAMPLE_CONFIG)
        key = ("XAUUSD", "H1")
        ts = pd.Timestamp("2024-01-01 10:00:00", tz="UTC")
        dm._data[key] = pd.DataFrame(
            [
                {
                    "timestamp": ts,
                    "open": 1.0,
                    "high": 1.5,
                    "low": 0.9,
                    "close": 1.2,
                    "volume": 10,
                }
            ]
        )
        final = pd.Series(
            {
                "timestamp": ts,
                "open": 1.0,
                "high": 2.0,
                "low": 0.8,
                "close": 1.9,
                "volume": 99,
            }
        )
        dm._mt5_merge_closed_bar(key, final)
        out = dm.get_data("XAUUSD", "H1")
        assert len(out) == 1
        assert float(out.iloc[-1]["close"]) == 1.9
        assert float(out.iloc[-1]["high"]) == 2.0

    def test_merge_append_newer_bar(self):
        dm = DataManager(SAMPLE_CONFIG)
        key = ("XAUUSD", "H1")
        t1 = pd.Timestamp("2024-01-01 10:00:00", tz="UTC")
        t2 = pd.Timestamp("2024-01-01 11:00:00", tz="UTC")
        dm._data[key] = pd.DataFrame(
            [
                {
                    "timestamp": t1,
                    "open": 1.0,
                    "high": 1.1,
                    "low": 0.9,
                    "close": 1.05,
                    "volume": 10,
                }
            ]
        )
        nxt = pd.Series(
            {
                "timestamp": t2,
                "open": 1.05,
                "high": 1.2,
                "low": 1.0,
                "close": 1.15,
                "volume": 20,
            }
        )
        dm._mt5_merge_closed_bar(key, nxt)
        out = dm.get_data("XAUUSD", "H1")
        assert len(out) == 2
        assert pd.Timestamp(out.iloc[-1]["timestamp"]) == t2


class TestBufferSpillToDisk:
    def _make_cfg(self, tmp_path, max_bars=3, spill_enabled=True, bar_log_every=20):
        return {
            **SAMPLE_CONFIG,
            "data": {
                **SAMPLE_CONFIG["data"],
                "buffer_max_bars": max_bars,
                "buffer_spill_enabled": spill_enabled,
                "buffer_spill_dir": str(tmp_path),
                "bar_log_every_n": bar_log_every,
            },
        }

    def _make_row(self, i: int) -> pd.Series:
        ts = pd.Timestamp(f"2024-01-{10 + i:02d} 10:00:00", tz="UTC")
        return pd.Series(
            {
                "timestamp": ts,
                "open": float(i),
                "high": float(i) + 1,
                "low": float(i) - 0.5,
                "close": float(i) + 0.5,
                "volume": 100 + i,
            }
        )

    def test_spill_old_rows_to_csv(self, tmp_path):
        dm = DataManager(self._make_cfg(tmp_path))
        for i in range(5):
            dm._append_bar("XAUUSD", "H1", self._make_row(i))
        out = dm.get_data("XAUUSD", "H1")
        assert len(out) == 3
        spill = tmp_path / "XAUUSD_H1_buffer.csv"
        assert spill.exists()
        dumped = pd.read_csv(spill)
        assert len(dumped) == 2

    def test_spill_no_file_when_disabled(self, tmp_path):
        dm = DataManager(self._make_cfg(tmp_path, spill_enabled=False))
        for i in range(5):
            dm._append_bar("XAUUSD", "H1", self._make_row(i))
        out = dm.get_data("XAUUSD", "H1")
        assert len(out) == 3
        spill = tmp_path / "XAUUSD_H1_buffer.csv"
        assert not spill.exists()

    def test_spill_no_duplicate_header(self, tmp_path):
        """Ghi nhiều đợt — file CSV chỉ có 1 dòng header."""
        dm = DataManager(self._make_cfg(tmp_path, max_bars=2))
        for i in range(8):
            dm._append_bar("XAUUSD", "H1", self._make_row(i))
        spill = tmp_path / "XAUUSD_H1_buffer.csv"
        assert spill.exists()
        with open(spill) as f:
            lines = [l for l in f.read().splitlines() if l.strip()]
        header_lines = [l for l in lines if "timestamp" in l.lower()]
        assert len(header_lines) == 1, "CSV chỉ được có 1 dòng header"

    def test_bar_log_first_and_every_n(self, tmp_path):
        """Log INFO tại nến #1 và mỗi 5 nến tiếp theo (every_n=5)."""
        from unittest.mock import patch
        dm = DataManager(self._make_cfg(tmp_path, max_bars=100, bar_log_every=5))
        log_calls: list[tuple] = []
        original_info = dm.__class__.__module__  # just to reference the module

        with patch("src.data.data_manager.logger") as mock_log:
            mock_log.info.side_effect = lambda msg, *a, **kw: log_calls.append(
                (msg % a) if a else msg
            )
            mock_log.debug.return_value = None
            mock_log.warning.return_value = None
            for i in range(11):
                dm._append_bar("XAUUSD", "H1", self._make_row(i))

        bar_msgs = [m for m in log_calls if "Bar #" in m]
        counts = [int(m.split("#")[1].split()[0]) for m in bar_msgs]
        # Nến đầu tiên (#1) luôn log; sau đó #6, #11 (every_n=5)
        assert 1 in counts
        assert 6 in counts
        assert 11 in counts
        # #2–#5, #7–#10 KHÔNG log
        assert 2 not in counts
        assert 5 not in counts
