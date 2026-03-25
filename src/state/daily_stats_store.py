"""
Daily Stats Store
=================
Lưu trữ thống kê giao dịch hằng ngày (TP / SL / Expired) với hai backend:
  - InMemoryDailyStats : mặc định, mất khi restart.
  - RedisDailyStats    : persistent, tự phục hồi sau restart.

Key Redis: ``{prefix}stats:{strategy}:{symbol}:{timeframe}``
Fields   : ``tp``, ``sl``, ``expired``  (dùng HINCRBY — atomic, không race condition)

Khi reset() (sau khi gửi summary) tất cả keys bị xoá.
TTL tự động bảo vệ tránh tích lũy vô hạn nếu reset bị miss.
"""

from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from collections import defaultdict

logger = logging.getLogger(__name__)

# (strategy, symbol, timeframe) → {tp, sl, expired}
StatsKey = tuple[str, str, str]
StatsDict = dict[str, int]   # {"tp": N, "sl": N, "expired": N}


class DailyStatsStore(ABC):
    """Interface chung cho daily stats storage."""

    OUTCOMES = ("tp", "sl", "expired")

    @abstractmethod
    def increment(self, strategy: str, symbol: str, timeframe: str, outcome: str) -> None:
        """Tăng counter của outcome ('tp' | 'sl' | 'expired') lên 1."""

    @abstractmethod
    def get_all(self) -> dict[StatsKey, StatsDict]:
        """Trả về toàn bộ stats hiện tại dạng {(strategy, symbol, tf): {tp, sl, expired}}."""

    @abstractmethod
    def reset(self) -> None:
        """Xoá toàn bộ stats (gọi sau khi đã gửi daily summary)."""


# ── In-memory (default) ───────────────────────────────────────────────────────

class InMemoryDailyStats(DailyStatsStore):
    """Lưu stats trong RAM. Mặc định khi không cấu hình Redis."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[StatsKey, StatsDict] = defaultdict(
            lambda: {"tp": 0, "sl": 0, "expired": 0}
        )

    def increment(self, strategy: str, symbol: str, timeframe: str, outcome: str) -> None:
        if outcome not in self.OUTCOMES:
            return
        with self._lock:
            self._data[(strategy, symbol, timeframe)][outcome] += 1

    def get_all(self) -> dict[StatsKey, StatsDict]:
        with self._lock:
            return {k: dict(v) for k, v in self._data.items()}

    def reset(self) -> None:
        with self._lock:
            self._data.clear()


# ── Redis ─────────────────────────────────────────────────────────────────────

class RedisDailyStats(DailyStatsStore):
    """
    Lưu stats vào Redis dùng Hash + HINCRBY (atomic).
    Key Redis: ``{prefix}stats:{strategy}:{symbol}:{timeframe}``

    Tự động phục hồi sau restart — không mất dữ liệu trong ngày.
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
        import redis as _redis

        self._r = _redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password or None,
            decode_responses=True,
            socket_connect_timeout=5,
        )
        self._prefix = key_prefix + "stats:"
        self._ttl = ttl_seconds

        self._r.ping()
        logger.info(
            "RedisDailyStats: connected %s:%d db=%d prefix=%s ttl=%ds",
            host, port, db, self._prefix, ttl_seconds,
        )
        self._log_restored()

    # ── internal ──────────────────────────────────────────────────────────────

    def _rkey(self, strategy: str, symbol: str, timeframe: str) -> str:
        return f"{self._prefix}{strategy}:{symbol}:{timeframe}"

    def _log_restored(self) -> None:
        data = self.get_all()
        if not data:
            logger.info("RedisDailyStats: no existing stats found")
            return
        total = sum(sum(v.values()) for v in data.values())
        logger.info("RedisDailyStats: restored %d buckets (%d outcomes total):", len(data), total)
        for (strat, sym, tf), v in sorted(data.items()):
            logger.info(
                "  ↳ %s %s/%s  tp=%d sl=%d expired=%d",
                strat, sym, tf, v["tp"], v["sl"], v["expired"],
            )

    # ── interface ─────────────────────────────────────────────────────────────

    def increment(self, strategy: str, symbol: str, timeframe: str, outcome: str) -> None:
        if outcome not in self.OUTCOMES:
            return
        rk = self._rkey(strategy, symbol, timeframe)
        try:
            self._r.hincrby(rk, outcome, 1)
            self._r.expire(rk, self._ttl)
        except Exception as exc:
            logger.error("RedisDailyStats.increment failed: %s", exc)

    def get_all(self) -> dict[StatsKey, StatsDict]:
        pattern = self._prefix + "*"
        result: dict[StatsKey, StatsDict] = {}
        try:
            rkeys = self._r.keys(pattern)
        except Exception as exc:
            logger.error("RedisDailyStats.get_all keys() failed: %s", exc)
            return result

        for rk in rkeys:
            try:
                raw = self._r.hgetall(rk)
                if not raw:
                    continue
                suffix = rk[len(self._prefix):]
                parts = suffix.split(":", 2)   # strategy : symbol : timeframe
                if len(parts) != 3:
                    continue
                key: StatsKey = (parts[0], parts[1], parts[2])
                result[key] = {
                    "tp":      int(raw.get("tp", 0)),
                    "sl":      int(raw.get("sl", 0)),
                    "expired": int(raw.get("expired", 0)),
                }
            except Exception as exc:
                logger.warning("RedisDailyStats: failed to parse key %s: %s", rk, exc)

        return result

    def reset(self) -> None:
        pattern = self._prefix + "*"
        try:
            rkeys = self._r.keys(pattern)
            if rkeys:
                self._r.delete(*rkeys)
                logger.info("RedisDailyStats.reset: deleted %d keys", len(rkeys))
        except Exception as exc:
            logger.error("RedisDailyStats.reset failed: %s", exc)


# ── Factory ───────────────────────────────────────────────────────────────────

def create_daily_stats_store(raw_cfg: dict) -> DailyStatsStore:
    """
    Tạo DailyStatsStore phù hợp dựa trên config.

    Dùng chung cấu hình ``redis`` với PaperStateStore.
    Nếu Redis disabled hoặc kết nối thất bại → fallback InMemoryDailyStats.
    """
    redis_cfg: dict = raw_cfg.get("redis", {}) or {}
    if not redis_cfg.get("enabled", False):
        logger.info("DailyStatsStore: Redis disabled — using in-memory store")
        return InMemoryDailyStats()

    host        = str(redis_cfg.get("host", "localhost"))
    port        = int(redis_cfg.get("port", 6379))
    db          = int(redis_cfg.get("db", 0))
    password    = redis_cfg.get("password") or None
    key_prefix  = str(redis_cfg.get("key_prefix", "trading:"))
    ttl_seconds = int(redis_cfg.get("stats_ttl_seconds", 172800))  # 2 ngày default

    try:
        return RedisDailyStats(host, port, db, password, key_prefix, ttl_seconds)
    except Exception as exc:
        logger.error(
            "DailyStatsStore: Redis connection failed (%s) — falling back to in-memory", exc
        )
        return InMemoryDailyStats()
