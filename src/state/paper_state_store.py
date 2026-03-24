"""
Paper Trading State Store
=========================
Lưu trữ trạng thái paper trading với hai backend:
  - InMemoryStateStore : mặc định, không cần cài thêm gì, mất khi restart.
  - RedisStateStore    : bật khi có `redis.enabled: true` trong config,
                         tự động phục hồi lại state sau khi restart.

Cả hai cùng implement interface PaperStateStore để main.py
không cần biết backend nào đang chạy.
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# Kiểu key: (symbol, timeframe) ví dụ ("XAUUSD", "M5")
StateKey = tuple[str, str]


class PaperStateStore(ABC):
    """Interface chung cho paper trading state storage."""

    @abstractmethod
    def get(self, key: StateKey) -> dict | None:
        """Trả về state dict hoặc None nếu không có vị thế."""

    @abstractmethod
    def set(self, key: StateKey, value: dict | None) -> None:
        """Lưu state. Truyền None để xoá (đóng vị thế)."""

    @abstractmethod
    def items(self) -> list[tuple[StateKey, dict]]:
        """Danh sách (key, state) của các vị thế đang mở (non-None)."""

    def values(self) -> list[dict]:
        """Danh sách state đang mở."""
        return [v for _, v in self.items()]

    def active_keys(self) -> list[StateKey]:
        """Danh sách key đang có vị thế."""
        return [k for k, _ in self.items()]


# ── In-memory (default) ───────────────────────────────────────────────────────

class InMemoryStateStore(PaperStateStore):
    """Lưu state trong RAM. Mặc định khi không cấu hình Redis."""

    def __init__(self) -> None:
        self._data: dict[StateKey, dict | None] = {}

    def get(self, key: StateKey) -> dict | None:
        return self._data.get(key)

    def set(self, key: StateKey, value: dict | None) -> None:
        self._data[key] = value

    def items(self) -> list[tuple[StateKey, dict]]:
        return [(k, v) for k, v in self._data.items() if v is not None]


# ── Redis ─────────────────────────────────────────────────────────────────────

class RedisStateStore(PaperStateStore):
    """
    Lưu state vào Redis dưới dạng JSON.
    Key Redis: ``{prefix}paper:{symbol}:{timeframe}``

    Khi khởi động, tự động log các state còn tồn tại (phục hồi sau restart).
    """

    def __init__(
        self,
        host: str,
        port: int,
        db: int,
        password: str | None,
        key_prefix: str,
        ttl_seconds: int,
    ) -> None:
        import redis as _redis  # lazy import — không bắt buộc khi dùng InMemory

        self._r = _redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password or None,
            decode_responses=True,
            socket_connect_timeout=5,
        )
        self._prefix = key_prefix + "paper:"
        self._ttl = ttl_seconds

        self._r.ping()  # fail-fast nếu không kết nối được
        logger.info(
            "RedisStateStore: connected %s:%d db=%d prefix=%s ttl=%ds",
            host, port, db, self._prefix, ttl_seconds,
        )
        self._log_restored()

    # ── internal helpers ──────────────────────────────────────────────────────

    def _rkey(self, key: StateKey) -> str:
        return f"{self._prefix}{key[0]}:{key[1]}"

    def _parse_rkey(self, rkey: str) -> StateKey | None:
        suffix = rkey[len(self._prefix):]
        parts = suffix.split(":", 1)
        return (parts[0], parts[1]) if len(parts) == 2 else None

    def _log_restored(self) -> None:
        existing = self.items()
        if not existing:
            logger.info("RedisStateStore: no existing states found")
            return
        logger.info("RedisStateStore: restored %d active paper state(s):", len(existing))
        for k, v in existing:
            logger.info(
                "  ↳ %s/%s  status=%-7s  order_id=%s",
                k[0], k[1], v.get("status", "?"), v.get("order_id", "—"),
            )

    # ── interface ─────────────────────────────────────────────────────────────

    def get(self, key: StateKey) -> dict | None:
        raw = self._r.get(self._rkey(key))
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except Exception:
            logger.warning("RedisStateStore: corrupt JSON for key %s", key)
            return None

    def set(self, key: StateKey, value: dict | None) -> None:
        rk = self._rkey(key)
        if value is None:
            self._r.delete(rk)
        else:
            try:
                self._r.set(rk, json.dumps(value), ex=self._ttl)
            except Exception as exc:
                logger.error("RedisStateStore: set failed key=%s: %s", key, exc)

    def items(self) -> list[tuple[StateKey, dict]]:
        pattern = self._prefix + "*"
        try:
            rkeys = self._r.keys(pattern)
        except Exception as exc:
            logger.error("RedisStateStore: keys() failed: %s", exc)
            return []
        result: list[tuple[StateKey, dict]] = []
        for rk in rkeys:
            raw = self._r.get(rk)
            if raw is None:
                continue
            parsed_key = self._parse_rkey(rk)
            if parsed_key is None:
                continue
            try:
                result.append((parsed_key, json.loads(raw)))
            except Exception:
                pass
        return result


# ── Factory ───────────────────────────────────────────────────────────────────

def create_paper_store(raw_cfg: dict) -> PaperStateStore:
    """
    Tạo PaperStateStore phù hợp dựa trên config.

    Config mẫu (config.yaml):
    ::

        redis:
          enabled: true
          host: localhost
          port: 6379
          db: 0
          password: null           # hoặc chuỗi password
          key_prefix: "trading:"
          state_ttl_seconds: 604800  # 7 ngày

    Nếu ``redis.enabled`` là false hoặc phần redis không tồn tại,
    trả về InMemoryStateStore.  Nếu Redis enabled nhưng kết nối thất bại,
    tự động fallback về InMemoryStateStore và log warning.
    """
    redis_cfg: dict = raw_cfg.get("redis", {}) or {}
    if not redis_cfg.get("enabled", False):
        logger.info("PaperStateStore: Redis disabled — using in-memory store")
        return InMemoryStateStore()

    host          = str(redis_cfg.get("host", "localhost"))
    port          = int(redis_cfg.get("port", 6379))
    db            = int(redis_cfg.get("db", 0))
    password      = redis_cfg.get("password") or None
    key_prefix    = str(redis_cfg.get("key_prefix", "trading:"))
    ttl_seconds   = int(redis_cfg.get("state_ttl_seconds", 604800))  # 7 ngày

    try:
        return RedisStateStore(host, port, db, password, key_prefix, ttl_seconds)
    except Exception as exc:
        logger.error(
            "PaperStateStore: Redis connection failed (%s) — falling back to in-memory", exc
        )
        return InMemoryStateStore()
