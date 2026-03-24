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

import hashlib
import html
import queue
import threading
import time
from typing import TYPE_CHECKING

from ..utils.logger import get_logger

if TYPE_CHECKING:
    from ..risk.risk_manager import CompleteSignal

logger = get_logger("telegram_notifier")


def _pip_size(symbol: str) -> float:
    """1 pip in price units (same convention as RiskManager)."""
    sym = symbol.upper()
    if sym in ("XAUUSD", "XAUUSDm"):
        return 0.10
    if "JPY" in sym:
        return 0.01
    return 0.0001


def _price_fmt(symbol: str, price: float) -> str:
    sym = symbol.upper()
    if sym in ("XAUUSD", "XAUUSDm"):
        return f"{price:.2f}"
    if "JPY" in sym:
        return f"{price:.3f}"
    return f"{price:.5f}"


def _distance_pips(symbol: str, a: float, b: float) -> float:
    return abs(a - b) / _pip_size(symbol)


def _format_signal(signal: "CompleteSignal") -> str:
    """Build the Telegram HTML-formatted message for a signal."""
    action_emoji = "📈" if "BUY" in signal.action else "📉"
    sym = signal.symbol
    ep   = _price_fmt(sym, signal.entry)
    sl   = _price_fmt(sym, signal.sl)
    tp1  = _price_fmt(sym, signal.tp1)
    tp1_pips = _distance_pips(sym, signal.entry, signal.tp1)

    tp2_line = ""
    if signal.tp2 is not None:
        tp2      = _price_fmt(sym, signal.tp2)
        tp2_pips = _distance_pips(sym, signal.entry, signal.tp2)
        tp2_line = f"✅ TP2 <code>{tp2}</code> <i>(~{tp2_pips:.0f}p)</i>\n"

    notes_line = ""
    if signal.notes and str(signal.notes).strip():
        notes_line = f"📝 {html.escape(str(signal.notes)[:400])}\n"

    ts = signal.timestamp
    ts_str = ts.strftime("%d/%m %H:%M") if hasattr(ts, "strftime") else str(ts)

    oid_line = ""
    if getattr(signal, "order_id", ""):
        oid_line = f"🆔 <code>{html.escape(signal.order_id)}</code>\n"

    return (
        f"{action_emoji} <b>{html.escape(signal.action)}</b>  "
        f"{html.escape(sym)}  <code>{html.escape(signal.timeframe)}</code>\n"
        f"💰 <code>{ep}</code>  "
        f"🛑 <code>{sl}</code> <i>({signal.sl_pips:.0f}p)</i>  "
        f"RR <b>1:{signal.rr_ratio:.1f}</b>\n"
        f"✅ TP1 <code>{tp1}</code> <i>(~{tp1_pips:.0f}p)</i>\n"
        f"{tp2_line}"
        f"⚖️ {signal.volume:.2f}L  "
        f"Risk {signal.risk_percent:.1f}% ~${signal.risk_amount_usd:,.0f}  "
        f"🤖 {html.escape(signal.strategy_name)}\n"
        f"{notes_line}"
        f"{oid_line}"
        f"🕐 <i>{ts_str}</i>"
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
        self._parse_mode: str = tg.get("parse_mode", "HTML")
        self._enabled: bool = bool(self._token and self._chat_id and
                                    self._token != "YOUR_BOT_TOKEN")
        self._cooldown: int = int(tg.get("cooldown_seconds", 60))

        self._queue: queue.Queue = queue.Queue(maxsize=100)
        self._sent_cache: dict[str, float] = {}   # fingerprint → sent_at timestamp
        self._cache_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._sender_thread: threading.Thread | None = None

        if not self._enabled:
            logger.warning(
                "Telegram not configured — signals will be logged to console only. "
                "Set telegram.bot_token and telegram.chat_id (user / group / channel -100…)."
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
        """Send a raw text message (e.g. startup/status alerts).

        Automatically splits messages longer than 4096 characters
        (Telegram's per-message limit) into multiple sequential chunks.
        Each chunk is enqueued separately.
        """
        MAX = 4000  # leave a small buffer below the 4096 hard limit
        if len(text) <= MAX:
            self._enqueue_text(text)
            return
        # Split on newline boundaries to avoid cutting mid-tag
        lines = text.split("\n")
        chunk_lines: list[str] = []
        chunk_len = 0
        for line in lines:
            line_len = len(line) + 1  # +1 for the newline
            if chunk_lines and chunk_len + line_len > MAX:
                self._enqueue_text("\n".join(chunk_lines))
                chunk_lines = []
                chunk_len = 0
            chunk_lines.append(line)
            chunk_len += line_len
        if chunk_lines:
            self._enqueue_text("\n".join(chunk_lines))

    def _enqueue_text(self, text: str) -> None:
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
            "parse_mode": self._parse_mode,
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
