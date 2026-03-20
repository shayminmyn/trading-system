"""
Telegram Notifier — sends formatted trading signals via Telegram Bot API.

Uses httpx (async-capable, faster than requests) directly instead of
python-telegram-bot to avoid heavy dependency overhead.

Features:
  - Deduplication: won't resend the same signal within cooldown_seconds
  - Async send queue: submissions from strategy threads are non-blocking
  - Graceful degradation: logs to console if Telegram is unreachable
"""

from __future__ import annotations

import asyncio
import hashlib
import queue
import threading
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from ..utils.logger import get_logger

if TYPE_CHECKING:
    from ..risk.risk_manager import CompleteSignal

logger = get_logger("telegram_notifier")


def _format_signal(signal: "CompleteSignal") -> str:
    """Build the Telegram HTML-formatted message for a signal."""
    action_emoji = "📈" if "BUY" in signal.action else "📉"
    tp2_line = f"✅ <b>Take Profit 2:</b> {signal.tp2:.5f}\n" if signal.tp2 else ""

    return (
        "🚨 <b>TÍN HIỆU GIAO DỊCH</b> 🚨\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔹 <b>Cặp:</b> {signal.symbol}\n"
        f"⏱ <b>Khung TG:</b> {signal.timeframe}\n"
        f"{action_emoji} <b>Lệnh:</b> {signal.action}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Entry:</b> {signal.entry:.5f}\n"
        f"🛑 <b>Stoploss:</b> {signal.sl:.5f} ({signal.sl_pips:.0f} pips)\n"
        f"✅ <b>Take Profit 1:</b> {signal.tp1:.5f} <i>(RR 1:{signal.rr_ratio:.1f})</i>\n"
        f"{tp2_line}"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚖️ <b>Volume:</b> {signal.volume:.2f} Lot "
        f"<i>(Risk: {signal.risk_percent:.1f}% / ${signal.risk_amount_usd:.2f})</i>\n"
        f"🤖 <b>Strategy:</b> {signal.strategy_name}\n"
        f"🕐 <i>{signal.timestamp.strftime('%Y-%m-%d %H:%M UTC')}</i>"
    )


def _signal_fingerprint(signal: "CompleteSignal") -> str:
    """Unique key per signal to prevent duplicates within cooldown window."""
    raw = f"{signal.symbol}{signal.timeframe}{signal.action}{signal.entry:.5f}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


class TelegramNotifier:
    """
    Thread-safe Telegram signal notifier.

    Runs a background sender thread with an internal queue so that
    strategy threads can call send_signal() without blocking.
    """

    def __init__(self, config: dict) -> None:
        tg = config.get("telegram", {})
        self._token: str = tg.get("bot_token", "")
        self._chat_id: str = str(tg.get("chat_id", ""))
        self._enabled: bool = bool(self._token and self._chat_id and
                                    self._token != "YOUR_BOT_TOKEN")
        self._cooldown: int = tg.get("cooldown_seconds", 60)

        self._queue: queue.Queue = queue.Queue(maxsize=100)
        self._sent_cache: dict[str, float] = {}   # fingerprint → sent_at timestamp
        self._cache_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._sender_thread: threading.Thread | None = None

        if not self._enabled:
            logger.warning(
                "Telegram not configured — signals will be logged to console only. "
                "Set telegram.bot_token and telegram.chat_id in config.yaml."
            )

    def start(self) -> None:
        """Start background sender thread."""
        self._sender_thread = threading.Thread(
            target=self._sender_loop,
            name="telegram-sender",
            daemon=True,
        )
        self._sender_thread.start()
        logger.info("TelegramNotifier started (enabled=%s)", self._enabled)

    def stop(self) -> None:
        self._stop_event.set()
        if self._sender_thread:
            self._sender_thread.join(timeout=5)
        logger.info("TelegramNotifier stopped")

    def send_signal(self, signal: "CompleteSignal") -> None:
        """
        Enqueue signal for sending. Non-blocking — safe to call from any thread.
        Skips if same signal was sent recently (deduplication).
        """
        fingerprint = _signal_fingerprint(signal)
        now = time.time()

        with self._cache_lock:
            last_sent = self._sent_cache.get(fingerprint, 0)
            if now - last_sent < self._cooldown:
                logger.debug("Signal deduplicated (cooldown): %s", fingerprint)
                return
            self._sent_cache[fingerprint] = now
            # Cleanup old entries
            self._sent_cache = {
                k: v for k, v in self._sent_cache.items()
                if now - v < self._cooldown * 2
            }

        try:
            self._queue.put_nowait(signal)
            logger.debug("Signal enqueued: %s", signal)
        except queue.Full:
            logger.warning("Telegram send queue full — signal dropped: %s", signal)

    def send_text(self, text: str) -> None:
        """Send a raw text message (e.g. startup/status alerts)."""
        try:
            self._queue.put_nowait(text)
        except queue.Full:
            logger.warning("Telegram queue full — text message dropped")

    # ── Background sender loop ────────────────────────────────────────────────

    def _sender_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=1.0)
                if isinstance(item, str):
                    self._do_send(item)
                else:
                    self._do_send(_format_signal(item))
                self._queue.task_done()
            except queue.Empty:
                continue
            except Exception:
                logger.exception("Unexpected error in Telegram sender loop")

    def _do_send(self, text: str) -> None:
        """Synchronously send text to Telegram API via httpx."""
        if not self._enabled:
            logger.info("[TELEGRAM SIGNAL]\n%s", text)
            return

        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            import httpx
            with httpx.Client(timeout=10.0) as client:
                resp = client.post(url, json=payload)
                resp.raise_for_status()
            logger.info("Telegram message sent (status=%d)", resp.status_code)
        except Exception as exc:
            logger.error("Failed to send Telegram message: %s", exc)
            logger.info("[FALLBACK SIGNAL]\n%s", text)
