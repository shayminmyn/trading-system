"""
Tests for PaperStateStore — InMemoryStateStore, RedisStateStore, create_paper_store.

Cấu trúc:
  - Unit tests (luôn chạy, không cần Redis thật):
      * InMemoryStateStore — CRUD, items/values/active_keys
      * RedisStateStore    — dùng MagicMock thay thế redis.Redis
      * create_paper_store — factory logic (enabled/disabled/fallback)
  - Integration tests (chỉ chạy khi có Redis thật):
      * Đánh dấu bằng @pytest.mark.redis
      * Bỏ qua tự động nếu localhost:6379 không phản hồi
      * Dùng DB 15 và key_prefix riêng để không đụng data thật

Chạy chỉ unit tests:
    pytest tests/test_paper_state_store.py -m "not redis"

Chạy tất cả (bao gồm integration):
    pytest tests/test_paper_state_store.py
"""

from __future__ import annotations

import json
import sys
import time
from unittest.mock import MagicMock, patch

import pytest

# ── Kiểm tra Redis thật TRƯỚC khi inject mock ─────────────────────────────────
# Phải làm trước khi mock sys.modules["redis"] để _REDIS_AVAILABLE chính xác.
def _check_real_redis() -> bool:
    try:
        import importlib
        real_redis = importlib.import_module("redis")
        r = real_redis.Redis(host="localhost", port=6379, db=15, socket_connect_timeout=1)
        r.ping()
        return True
    except Exception:
        return False

_REDIS_AVAILABLE: bool = _check_real_redis()

# ── Mock redis module trước khi import bất cứ thứ gì dùng nó ─────────────────
# redis-py là optional dependency; khi không cài, unit tests vẫn chạy được
# bằng cách inject một fake module vào sys.modules.
if "redis" not in sys.modules:
    _fake_redis_mod = MagicMock()
    sys.modules["redis"] = _fake_redis_mod

from src.state.paper_state_store import (
    InMemoryStateStore,
    RedisStateStore,
    PaperStateStore,
    create_paper_store,
)

# ── Helpers ───────────────────────────────────────────────────────────────────

SAMPLE_STATE_PENDING = {
    "status": "PENDING",
    "symbol": "XAUUSD",
    "timeframe": "M5",
    "is_buy": True,
    "limit_price": 3000.0,
    "sl_level": 2980.0,
    "sl": 2980.0,
    "tp": 3040.0,
    "rr_ratio": 2.0,
    "pip_size": 0.1,
    "bars_remaining": 6,
    "minutes_remaining": 30,
    "minutes_total": 30,
    "strategy": "SonicRM5Strategy",
    "notes": "",
    "order_id": "XAUUSD-M5-20260324-0830",
    "mt5_ticket": 0,
}

SAMPLE_STATE_OPEN = {
    "status": "OPEN",
    "symbol": "EURUSD",
    "timeframe": "H1",
    "is_buy": False,
    "entry": 1.08500,
    "sl": 1.08700,
    "tp": 1.08100,
    "strategy": "SonicRH1Strategy",
    "notes": "",
    "order_id": "EURUSD-H1-20260324-0600",
    "mt5_ticket": 12345,
}

KEY_XAUUSD_M5: tuple[str, str] = ("XAUUSD", "M5")
KEY_EURUSD_H1: tuple[str, str] = ("EURUSD", "H1")


# ═══════════════════════════════════════════════════════════════════════════════
# InMemoryStateStore
# ═══════════════════════════════════════════════════════════════════════════════

class TestInMemoryStateStore:

    def setup_method(self):
        self.store = InMemoryStateStore()

    def test_is_paper_state_store(self):
        assert isinstance(self.store, PaperStateStore)

    def test_get_missing_key_returns_none(self):
        assert self.store.get(KEY_XAUUSD_M5) is None

    def test_set_and_get(self):
        self.store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        result = self.store.get(KEY_XAUUSD_M5)
        assert result == SAMPLE_STATE_PENDING

    def test_set_none_clears_key(self):
        self.store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        self.store.set(KEY_XAUUSD_M5, None)
        assert self.store.get(KEY_XAUUSD_M5) is None

    def test_items_empty_when_no_state(self):
        assert self.store.items() == []

    def test_items_excludes_none_values(self):
        self.store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        self.store.set(KEY_EURUSD_H1, None)
        items = self.store.items()
        assert len(items) == 1
        assert items[0][0] == KEY_XAUUSD_M5

    def test_items_returns_all_active(self):
        self.store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        self.store.set(KEY_EURUSD_H1, SAMPLE_STATE_OPEN)
        keys = [k for k, _ in self.store.items()]
        assert set(keys) == {KEY_XAUUSD_M5, KEY_EURUSD_H1}

    def test_values_returns_dicts(self):
        self.store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        self.store.set(KEY_EURUSD_H1, SAMPLE_STATE_OPEN)
        vals = self.store.values()
        assert SAMPLE_STATE_PENDING in vals
        assert SAMPLE_STATE_OPEN in vals

    def test_active_keys(self):
        self.store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        self.store.set(KEY_EURUSD_H1, None)
        assert self.store.active_keys() == [KEY_XAUUSD_M5]

    def test_overwrite_existing(self):
        self.store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        updated = {**SAMPLE_STATE_PENDING, "status": "OPEN", "entry": 3001.0}
        self.store.set(KEY_XAUUSD_M5, updated)
        assert self.store.get(KEY_XAUUSD_M5)["status"] == "OPEN"


# ═══════════════════════════════════════════════════════════════════════════════
# RedisStateStore — unit tests với mock Redis client
# ═══════════════════════════════════════════════════════════════════════════════

def _make_redis_store(mock_redis_client: MagicMock) -> RedisStateStore:
    """Tạo RedisStateStore với Redis client được mock, bỏ qua ping."""
    fake_mod = MagicMock()
    fake_mod.Redis.return_value = mock_redis_client
    with patch.dict(sys.modules, {"redis": fake_mod}):
        store = RedisStateStore(
            host="localhost",
            port=6379,
            db=15,
            password=None,
            key_prefix="test:",
            ttl_seconds=3600,
        )
    return store


class TestRedisStateStoreUnit:

    def setup_method(self):
        self.mock_r = MagicMock()
        self.mock_r.ping.return_value = True
        self.mock_r.keys.return_value = []
        self.store = _make_redis_store(self.mock_r)

    # ── key helpers ───────────────────────────────────────────────────────────

    def test_rkey_format(self):
        assert self.store._rkey(("XAUUSD", "M5")) == "test:paper:XAUUSD:M5"

    def test_rkey_format_h1(self):
        assert self.store._rkey(("EURUSD", "H1")) == "test:paper:EURUSD:H1"

    def test_parse_rkey_valid(self):
        assert self.store._parse_rkey("test:paper:XAUUSD:M5") == ("XAUUSD", "M5")

    def test_parse_rkey_timeframe_with_colon(self):
        # Symbol không có colon, timeframe cũng không — nhưng test parse an toàn
        assert self.store._parse_rkey("test:paper:BTCUSD:M15") == ("BTCUSD", "M15")

    def test_parse_rkey_invalid_returns_none(self):
        assert self.store._parse_rkey("test:paper:XAUUSD") is None

    # ── get ───────────────────────────────────────────────────────────────────

    def test_get_returns_none_when_missing(self):
        self.mock_r.get.return_value = None
        assert self.store.get(("XAUUSD", "M5")) is None

    def test_get_returns_parsed_dict(self):
        self.mock_r.get.return_value = json.dumps(SAMPLE_STATE_PENDING)
        result = self.store.get(KEY_XAUUSD_M5)
        assert result == SAMPLE_STATE_PENDING
        self.mock_r.get.assert_called_once_with("test:paper:XAUUSD:M5")

    def test_get_returns_none_on_corrupt_json(self):
        self.mock_r.get.return_value = "{not valid json"
        assert self.store.get(KEY_XAUUSD_M5) is None

    # ── set ───────────────────────────────────────────────────────────────────

    def test_set_stores_json_with_ttl(self):
        self.store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        self.mock_r.set.assert_called_once_with(
            "test:paper:XAUUSD:M5",
            json.dumps(SAMPLE_STATE_PENDING),
            ex=3600,
        )

    def test_set_none_calls_delete(self):
        self.store.set(KEY_XAUUSD_M5, None)
        self.mock_r.delete.assert_called_once_with("test:paper:XAUUSD:M5")
        self.mock_r.set.assert_not_called()

    def test_set_silently_handles_redis_error(self):
        self.mock_r.set.side_effect = Exception("connection lost")
        self.store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)  # không raise

    # ── items ─────────────────────────────────────────────────────────────────

    def test_items_empty_when_no_keys(self):
        self.mock_r.keys.return_value = []
        assert self.store.items() == []

    def test_items_returns_parsed_states(self):
        rkey = "test:paper:XAUUSD:M5"
        self.mock_r.keys.return_value = [rkey]
        self.mock_r.get.return_value = json.dumps(SAMPLE_STATE_PENDING)
        items = self.store.items()
        assert len(items) == 1
        assert items[0][0] == ("XAUUSD", "M5")
        assert items[0][1] == SAMPLE_STATE_PENDING

    def test_items_skips_corrupt_values(self):
        rkey = "test:paper:XAUUSD:M5"
        self.mock_r.keys.return_value = [rkey]
        self.mock_r.get.return_value = "bad json{"
        assert self.store.items() == []

    def test_items_skips_key_that_disappeared(self):
        rkey = "test:paper:XAUUSD:M5"
        self.mock_r.keys.return_value = [rkey]
        self.mock_r.get.return_value = None  # expired between keys() and get()
        assert self.store.items() == []

    def test_items_skips_unparseable_rkey(self):
        self.mock_r.keys.return_value = ["test:paper:XAUUSD"]  # thiếu timeframe
        self.mock_r.get.return_value = json.dumps(SAMPLE_STATE_PENDING)
        assert self.store.items() == []

    def test_items_returns_error_gracefully_on_keys_failure(self):
        self.mock_r.keys.side_effect = Exception("redis down")
        assert self.store.items() == []

    def test_values_derived_from_items(self):
        rkey = "test:paper:XAUUSD:M5"
        self.mock_r.keys.return_value = [rkey]
        self.mock_r.get.return_value = json.dumps(SAMPLE_STATE_PENDING)
        assert self.store.values() == [SAMPLE_STATE_PENDING]

    def test_active_keys_derived_from_items(self):
        rkey = "test:paper:XAUUSD:M5"
        self.mock_r.keys.return_value = [rkey]
        self.mock_r.get.return_value = json.dumps(SAMPLE_STATE_PENDING)
        assert self.store.active_keys() == [("XAUUSD", "M5")]

    # ── ping / init ───────────────────────────────────────────────────────────

    def test_ping_called_on_init(self):
        self.mock_r.ping.assert_called_once()

    def test_init_raises_if_ping_fails(self):
        failing_redis = MagicMock()
        failing_redis.ping.side_effect = Exception("Connection refused")
        failing_redis.keys.return_value = []
        fake_mod = MagicMock()
        fake_mod.Redis.return_value = failing_redis
        with pytest.raises(Exception, match="Connection refused"):
            with patch.dict(sys.modules, {"redis": fake_mod}):
                RedisStateStore("localhost", 6379, 15, None, "test:", 3600)


# ═══════════════════════════════════════════════════════════════════════════════
# create_paper_store factory
# ═══════════════════════════════════════════════════════════════════════════════

class TestCreatePaperStore:

    def test_returns_inmemory_when_no_redis_section(self):
        store = create_paper_store({})
        assert isinstance(store, InMemoryStateStore)

    def test_returns_inmemory_when_redis_disabled(self):
        store = create_paper_store({"redis": {"enabled": False}})
        assert isinstance(store, InMemoryStateStore)

    def test_returns_inmemory_when_redis_null(self):
        store = create_paper_store({"redis": None})
        assert isinstance(store, InMemoryStateStore)

    def test_returns_redis_store_when_enabled(self):
        mock_r = MagicMock()
        mock_r.ping.return_value = True
        mock_r.keys.return_value = []
        fake_mod = MagicMock()
        fake_mod.Redis.return_value = mock_r
        with patch.dict(sys.modules, {"redis": fake_mod}):
            store = create_paper_store({
                "redis": {
                    "enabled": True,
                    "host": "localhost",
                    "port": 6379,
                    "db": 15,
                    "password": None,
                    "key_prefix": "test:",
                    "state_ttl_seconds": 3600,
                }
            })
        assert isinstance(store, RedisStateStore)

    def test_falls_back_to_inmemory_on_connection_error(self):
        mock_r = MagicMock()
        mock_r.ping.side_effect = Exception("Connection refused")
        fake_mod = MagicMock()
        fake_mod.Redis.return_value = mock_r
        with patch.dict(sys.modules, {"redis": fake_mod}):
            store = create_paper_store({
                "redis": {
                    "enabled": True,
                    "host": "badhost",
                    "port": 9999,
                }
            })
        assert isinstance(store, InMemoryStateStore)

    def test_default_values_applied(self):
        """create_paper_store dùng defaults hợp lý khi không đủ config."""
        mock_r = MagicMock()
        mock_r.ping.return_value = True
        mock_r.keys.return_value = []
        captured = {}

        def fake_redis_cls(**kwargs):
            captured.update(kwargs)
            return mock_r

        fake_mod = MagicMock()
        fake_mod.Redis.side_effect = fake_redis_cls
        with patch.dict(sys.modules, {"redis": fake_mod}):
            create_paper_store({"redis": {"enabled": True}})

        assert captured["host"] == "localhost"
        assert captured["port"] == 6379
        assert captured["db"] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# Integration tests — chỉ chạy khi Redis thật sẵn sàng trên localhost:6379
# ═══════════════════════════════════════════════════════════════════════════════

redis_available = pytest.mark.skipif(
    not _REDIS_AVAILABLE,
    reason="Redis không có sẵn trên localhost:6379 — bỏ qua integration tests",
)

_TEST_PREFIX = "pytest_trading_test:"
_TEST_DB = 15
_TEST_TTL = 60


@pytest.fixture
def real_store():
    """Tạo RedisStateStore thật, xoá sạch key test sau khi xong."""
    import importlib
    real_redis_mod = importlib.import_module("redis")
    # Tạm thời dùng real redis module để khởi tạo store
    with patch.dict(sys.modules, {"redis": real_redis_mod}):
        store = RedisStateStore(
            host="localhost",
            port=6379,
            db=_TEST_DB,
            password=None,
            key_prefix=_TEST_PREFIX,
            ttl_seconds=_TEST_TTL,
        )
    yield store
    # cleanup
    r = real_redis_mod.Redis(host="localhost", port=6379, db=_TEST_DB, decode_responses=True)
    for k in r.keys(_TEST_PREFIX + "*"):
        r.delete(k)


@redis_available
class TestRedisStateStoreIntegration:

    def test_ping_succeeds(self, real_store):
        """Redis kết nối được và ping trả về True."""
        assert real_store._r.ping() is True

    def test_set_and_get_roundtrip(self, real_store):
        real_store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        result = real_store.get(KEY_XAUUSD_M5)
        assert result is not None
        assert result["status"] == "PENDING"
        assert result["order_id"] == SAMPLE_STATE_PENDING["order_id"]

    def test_get_missing_returns_none(self, real_store):
        assert real_store.get(("MISSING", "X1")) is None

    def test_set_none_deletes_from_redis(self, real_store):
        real_store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        real_store.set(KEY_XAUUSD_M5, None)
        assert real_store.get(KEY_XAUUSD_M5) is None

    def test_overwrite_updates_value(self, real_store):
        real_store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        opened = {**SAMPLE_STATE_PENDING, "status": "OPEN", "entry": 3001.5}
        real_store.set(KEY_XAUUSD_M5, opened)
        assert real_store.get(KEY_XAUUSD_M5)["status"] == "OPEN"
        assert real_store.get(KEY_XAUUSD_M5)["entry"] == 3001.5

    def test_items_reflects_stored_states(self, real_store):
        real_store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        real_store.set(KEY_EURUSD_H1, SAMPLE_STATE_OPEN)
        keys = {k for k, _ in real_store.items()}
        assert KEY_XAUUSD_M5 in keys
        assert KEY_EURUSD_H1 in keys

    def test_items_excludes_deleted(self, real_store):
        real_store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        real_store.set(KEY_EURUSD_H1, SAMPLE_STATE_OPEN)
        real_store.set(KEY_EURUSD_H1, None)
        keys = [k for k, _ in real_store.items()]
        assert KEY_XAUUSD_M5 in keys
        assert KEY_EURUSD_H1 not in keys

    def test_ttl_is_set(self, real_store):
        """Key phải có TTL dương sau khi set."""
        real_store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        ttl = real_store._r.ttl(real_store._rkey(KEY_XAUUSD_M5))
        assert 0 < ttl <= _TEST_TTL

    def test_mt5_ticket_update_pattern(self, real_store):
        """Mô phỏng pattern cập nhật mt5_ticket trong _on_order_result."""
        real_store.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)
        target_order_id = SAMPLE_STATE_PENDING["order_id"]

        for k, slot in real_store.items():
            if slot.get("order_id") == target_order_id:
                updated = {**slot, "mt5_ticket": 99999}
                real_store.set(k, updated)
                break

        assert real_store.get(KEY_XAUUSD_M5)["mt5_ticket"] == 99999

    def test_state_survives_reconnect(self):
        """State persist qua lần khởi tạo mới (simulate restart)."""
        import importlib
        real_redis_mod = importlib.import_module("redis")
        with patch.dict(sys.modules, {"redis": real_redis_mod}):
            s1 = RedisStateStore("localhost", 6379, _TEST_DB, None, _TEST_PREFIX, _TEST_TTL)
            s1.set(KEY_XAUUSD_M5, SAMPLE_STATE_PENDING)

            s2 = RedisStateStore("localhost", 6379, _TEST_DB, None, _TEST_PREFIX, _TEST_TTL)
            result = s2.get(KEY_XAUUSD_M5)
            s2.set(KEY_XAUUSD_M5, None)

        assert result is not None
        assert result["order_id"] == SAMPLE_STATE_PENDING["order_id"]
