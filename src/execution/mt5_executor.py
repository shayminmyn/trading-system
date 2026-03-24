"""
MT5 Order Executor — đặt lệnh thực tế qua MetaTrader5 Python API.

Thread-safety:
  MT5 Python API không thread-safe. Tất cả lệnh gọi order_send / order_check
  phải chạy từ cùng 1 thread đã gọi initialize().  Executor dùng một hàng đợi
  nội bộ và một luồng chuyên biệt (_exec_loop) để đảm bảo điều này.

  submit_signal() có thể gọi từ bất kỳ thread nào — non-blocking.

Hỗ trợ:
  • Market order  (BUY / SELL)         → TRADE_ACTION_DEAL
  • Limit  order  (BUY LIMIT / SELL LIMIT) → TRADE_ACTION_PENDING

Config (config.yaml):
  execution:
    enabled: false              # PHẢI đặt true để gửi lệnh thật
    magic_number: 20260101      # Định danh lệnh của bot trong MT5
    deviation_points: 20        # Slippage tối đa cho market order (points)
    comment_prefix: "Bot"       # Tiền tố comment lệnh
    expiration_hours: 0         # Giờ hết hạn cho pending limit (0 = GTC)
    filling_mode: "IOC"         # IOC | FOK | RETURN (tuỳ broker)
    retry_on_requote: true      # Tự retry 1 lần nếu broker trả REQUOTE
"""

from __future__ import annotations

import queue
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING, Callable

from ..utils.logger import get_logger

if TYPE_CHECKING:
    from ..risk.risk_manager import CompleteSignal

logger = get_logger("mt5_executor")

# MT5 trade return codes (defined here so we don't import mt5 at module level)
_RETCODE_DONE             = 10009   # mt5.TRADE_RETCODE_DONE
_RETCODE_PLACED           = 10008   # mt5.TRADE_RETCODE_PLACED
_RETCODE_CANCEL           = 10007   # mt5.TRADE_RETCODE_CANCEL — pending order removed
_RETCODE_REQUOTE          = 10004   # mt5.TRADE_RETCODE_REQUOTE
_RETCODE_AUTO_TRADING_OFF = 10027   # AutoTrading disabled by client
_RETCODE_INVALID_ORDER    = 10006   # order not found (already closed/filled)
_RETCODE_OK_CANCEL = (_RETCODE_DONE, _RETCODE_CANCEL)  # acceptable retcodes for cancel

_FILLING_MAP = {
    "IOC":    1,   # mt5.ORDER_FILLING_IOC
    "FOK":    0,   # mt5.ORDER_FILLING_FOK
    "RETURN": 2,   # mt5.ORDER_FILLING_RETURN
}

# Hệ số scale thời gian hết hạn theo timeframe.
# expiration_hours (config) là giá trị cơ sở cho H1/H4.
# Timeframe nhỏ hơn → limit hết hạn sớm hơn để không treo lệnh quá lâu.
# Ví dụ config expiration_hours=1:
#   H1/H4 → 1h   M30 → 1h   M15 → 1h   M5 → 0.5h   M1 → 0.2h
_TF_EXPIRY_SCALE: dict[str, float] = {
    "M1":  0.10,
    "M5":  0.50,
    "M15": 1.00,
    "M30": 1.00,
    "H1":  1.00,
    "H4":  1.00,
    "D1":  1.00,
    "W1":  1.00,
    "MN1": 1.00,
}


@dataclass
class OrderResult:
    """Kết quả sau mỗi lần gửi lệnh."""

    success: bool
    order_type: str           # "MARKET" | "LIMIT" | "CANCEL"
    symbol: str
    action: str               # "BUY" | "SELL" | "BUY LIMIT" | "SELL LIMIT" | "CANCEL"
    volume: float
    price: float              # giá thực tế fill (market) hoặc limit price (pending)
    sl: float
    tp: float
    ticket: int = 0           # MT5 order ticket (0 nếu thất bại)
    error_code: int = 0
    error_msg: str = ""
    strategy_name: str = ""
    order_id: str = ""        # định danh nội bộ: SYMBOL-TF-YYYYMMdd-HHMM

    def __str__(self) -> str:
        if self.success:
            return (
                f"[OK] ticket={self.ticket} {self.action} {self.symbol} "
                f"vol={self.volume:.2f} price={self.price} "
                f"sl={self.sl} tp={self.tp}"
            )
        return (
            f"[FAIL] {self.action} {self.symbol} "
            f"err={self.error_code} {self.error_msg}"
        )


class MT5OrderExecutor:
    """
    Gửi lệnh thực tế vào MT5 từ một luồng chuyên biệt.

    Luồng gọi chiến lược (strategy thread) không bị block — chỉ enqueue.
    Luồng executor (_exec_loop) gọi MT5 API tuần tự, đảm bảo thread-safety.
    """

    def __init__(self, config: dict, connector) -> None:
        exec_cfg = config.get("execution", {})
        self._enabled: bool        = bool(exec_cfg.get("enabled", False))
        self._magic: int           = int(exec_cfg.get("magic_number", 20260101))
        self._deviation: int       = int(exec_cfg.get("deviation_points", 20))
        self._comment_prefix: str  = str(exec_cfg.get("comment_prefix", "Bot"))
        self._expiry_hours: int    = int(exec_cfg.get("expiration_hours", 0))
        self._filling: int         = _FILLING_MAP.get(
            str(exec_cfg.get("filling_mode", "IOC")).upper(), 1
        )
        self._retry_requote: bool  = bool(exec_cfg.get("retry_on_requote", True))
        self._connector            = connector   # MT5Connector instance (may be None on mock)

        self._queue: queue.Queue                        = queue.Queue(maxsize=200)
        self._stop_event                                = threading.Event()
        self._exec_thread: threading.Thread | None      = None
        self._result_callbacks: list[Callable[[OrderResult], None]] = []

    # ── Public API ────────────────────────────────────────────────────────────

    def add_result_callback(self, cb: Callable[[OrderResult], None]) -> None:
        """Đăng ký callback nhận OrderResult sau mỗi lần gửi lệnh."""
        self._result_callbacks.append(cb)

    def start(self) -> None:
        if not self._enabled:
            logger.info(
                "MT5OrderExecutor disabled (execution.enabled=false) — "
                "signals will NOT be sent to MT5"
            )
            return
        if self._connector is None or not self._connector.is_connected():
            logger.warning(
                "MT5OrderExecutor: connector not available — "
                "execution.enabled=true nhưng MT5 chưa kết nối. "
                "Tắt execution hoặc chạy trên Windows với MT5 đang mở."
            )
            self._enabled = False
            return
        self._exec_thread = threading.Thread(
            target=self._exec_loop,
            name="mt5-executor",
            daemon=True,
        )
        self._exec_thread.start()
        logger.info(
            "MT5OrderExecutor started | magic=%d deviation=%d pts filling=%s",
            self._magic, self._deviation,
            next(k for k, v in _FILLING_MAP.items() if v == self._filling),
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._exec_thread:
            self._exec_thread.join(timeout=5)
        logger.info("MT5OrderExecutor stopped")

    def submit_signal(self, signal: "CompleteSignal") -> None:
        """
        Enqueue signal để thực thi. Non-blocking — an toàn từ bất kỳ thread.
        Nếu executor disabled hoặc queue đầy thì bỏ qua (chỉ log).
        """
        if not self._enabled:
            return
        try:
            self._queue.put_nowait(signal)
            logger.debug("Order enqueued: %s", signal)
        except queue.Full:
            logger.warning("MT5 executor queue full — signal dropped: %s", signal)

    def cancel_order_async(self, ticket: int, order_id: str = "") -> None:
        """
        Enqueue lệnh huỷ pending order theo ticket. Non-blocking.
        Dùng khi paper track phát hiện limit hết hạn → đồng bộ huỷ trên MT5.
        """
        if not self._enabled:
            return
        cmd = {"_cmd": "CANCEL", "ticket": ticket, "order_id": order_id}
        try:
            self._queue.put_nowait(cmd)
            logger.debug("Cancel command enqueued: ticket=%d order_id=%s", ticket, order_id)
        except queue.Full:
            logger.warning("MT5 executor queue full — cancel dropped: ticket=%d", ticket)

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    # ── Execution loop ────────────────────────────────────────────────────────

    def _exec_loop(self) -> None:
        # MT5 Python API là thread-local: mỗi thread gọi MT5 phải tự
        # initialize() + login() trong chính thread đó.
        # _exec_loop chạy trong thread riêng → phải init trước khi nhận lệnh.
        if not self._connector.init_thread():
            logger.error(
                "MT5 executor thread: init_thread() thất bại — "
                "không thể đặt lệnh. Kiểm tra MT5 đang mở và credentials đúng."
            )
            return

        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=1.0)
                if isinstance(item, dict) and item.get("_cmd") == "CANCEL":
                    result = self._execute_cancel(item["ticket"], item.get("order_id", ""))
                else:
                    result = self._execute(item)
                self._queue.task_done()
                self._fire_callbacks(result)
            except queue.Empty:
                continue
            except Exception:
                logger.exception("Unexpected error in MT5 executor loop")

    def _fire_callbacks(self, result: OrderResult) -> None:
        for cb in self._result_callbacks:
            try:
                cb(result)
            except Exception:
                logger.exception("Order result callback raised exception")

    # ── Order dispatch ────────────────────────────────────────────────────────

    def _execute(self, signal: "CompleteSignal") -> OrderResult:
        mt5 = getattr(self._connector, "_mt5", None)
        if mt5 is None or not self._connector.is_connected():
            return self._fail(signal, "", -1, "MT5 not connected")

        is_buy   = "BUY"   in signal.action.upper()
        is_limit = "LIMIT" in signal.action.upper()

        # MT5 comment: max 31 chars, chỉ dùng ký tự an toàn (không pipe, không ký tự đặc biệt).
        # Format: "{prefix} {order_id}" để vừa nhận ra bot vừa trace được lệnh.
        oid = getattr(signal, "order_id", "") or ""
        max_oid_len = 31 - len(self._comment_prefix) - 1   # 1 cho dấu cách
        if oid:
            comment = f"{self._comment_prefix} {oid[:max_oid_len]}"
        else:
            strat_short = signal.strategy_name.replace("Strategy", "").replace("Strat", "")
            comment = f"{self._comment_prefix} {strat_short}"[:31]

        if is_limit:
            return self._send_limit(mt5, signal, is_buy, comment)
        return self._send_market(mt5, signal, is_buy, comment)

    # ── Symbol selection helper ───────────────────────────────────────────────

    def _ensure_symbol(self, mt5, signal: "CompleteSignal", order_type: str) -> "OrderResult | None":
        """
        Đảm bảo symbol được chọn vào Market Watch của MT5.
        Trả về None nếu OK, trả về OrderResult lỗi nếu thất bại.

        order_check trả về None khi symbol không có trong Market Watch hoặc
        chưa được load đầy đủ thông tin. symbol_select() bắt buộc phải gọi
        trước khi đặt lệnh bất kỳ với symbol đó.
        """
        if not mt5.symbol_select(signal.symbol, True):
            err_code, err_msg = mt5.last_error()
            logger.error(
                "MT5 symbol_select(%s) failed: (%d) %s — "
                "kiểm tra symbol đúng chính xác (kể cả suffix) và có trong Market Watch",
                signal.symbol, err_code, err_msg,
            )
            return self._fail(
                signal, order_type, err_code,
                f"symbol_select({signal.symbol}) failed: ({err_code}) {err_msg}",
            )

        # Verify symbol info loaded after select
        info = mt5.symbol_info(signal.symbol)
        if info is None:
            err_code, err_msg = mt5.last_error()
            logger.error(
                "MT5 symbol_info(%s) returned None after symbol_select: (%d) %s",
                signal.symbol, err_code, err_msg,
            )
            return self._fail(
                signal, order_type, err_code,
                f"symbol_info({signal.symbol}) None: ({err_code}) {err_msg}",
            )

        logger.debug("MT5 symbol_select OK: %s trade_mode=%s", signal.symbol, info.trade_mode)
        return None

    # ── Market order ──────────────────────────────────────────────────────────

    def _send_market(self, mt5, signal: "CompleteSignal", is_buy: bool, comment: str) -> OrderResult:
        err = self._ensure_symbol(mt5, signal, "MARKET")
        if err is not None:
            return err

        tick = mt5.symbol_info_tick(signal.symbol)
        if tick is None:
            err_code, err_msg = mt5.last_error()
            return self._fail(signal, "MARKET", err_code, f"no tick for {signal.symbol}: ({err_code}) {err_msg}")

        price      = tick.ask if is_buy else tick.bid
        order_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL

        request = {
            "action":       mt5.TRADE_ACTION_DEAL,
            "symbol":       signal.symbol,
            "volume":       float(signal.volume),
            "type":         order_type,
            "price":        price,
            "sl":           float(signal.sl),
            "tp":           float(signal.tp1),
            "deviation":    self._deviation,
            "magic":        self._magic,
            "comment":      comment,
            "type_time":    mt5.ORDER_TIME_GTC,
            "type_filling": self._filling,
        }
        return self._do_send(mt5, request, "MARKET", signal, price)

    # ── Limit / Pending order ─────────────────────────────────────────────────

    def _scaled_expiry_hours(self, timeframe: str) -> float:
        """
        Tính thời gian hết hạn thực tế dựa trên timeframe.

        expiration_hours (config) là cơ sở cho H1/H4.
        TF nhỏ hơn nhân với _TF_EXPIRY_SCALE để tránh treo lệnh quá lâu:
          M1 × 0.20,  M5 × 0.50,  M15+ × 1.00
        """
        scale = _TF_EXPIRY_SCALE.get(timeframe.upper(), 1.0)
        return self._expiry_hours * scale

    def _send_limit(self, mt5, signal: "CompleteSignal", is_buy: bool, comment: str) -> OrderResult:
        err = self._ensure_symbol(mt5, signal, "LIMIT")
        if err is not None:
            return err

        order_type = mt5.ORDER_TYPE_BUY_LIMIT if is_buy else mt5.ORDER_TYPE_SELL_LIMIT
        actual_expiry = self._scaled_expiry_hours(signal.timeframe)

        request = {
            "action":       mt5.TRADE_ACTION_PENDING,
            "symbol":       signal.symbol,
            "volume":       float(signal.volume),
            "type":         order_type,
            "price":        float(signal.entry),   # entry == limit_price (sau risk_manager fix)
            "sl":           float(signal.sl),
            "tp":           float(signal.tp1),
            "deviation":    self._deviation,
            "magic":        self._magic,
            "comment":      comment,
            "type_time":    (
                mt5.ORDER_TIME_SPECIFIED if actual_expiry > 0
                else mt5.ORDER_TIME_GTC
            ),
        }

        # if actual_expiry > 0:
        #     exp = datetime.now(tz=timezone.utc) + timedelta(hours=actual_expiry)
        #     # MT5 yêu cầu: không có tzinfo, không có microseconds
        #     request["expiration"] = exp.replace(tzinfo=None, microsecond=0)
        #     logger.debug(
        #         "Limit expiry for %s %s: %.2fh (base=%.2fh × scale=%.2f)",
        #         signal.symbol, signal.timeframe,
        #         actual_expiry, self._expiry_hours,
        #         _TF_EXPIRY_SCALE.get(signal.timeframe.upper(), 1.0),
        #     )

        return self._do_send(mt5, request, "LIMIT", signal, float(signal.entry))

    # ── Core send ─────────────────────────────────────────────────────────────

    def _do_send(
        self,
        mt5,
        request: dict,
        order_type: str,
        signal: "CompleteSignal",
        requested_price: float,
        _retry: bool = False,
    ) -> OrderResult:
        # Pre-flight check
        check = mt5.order_check(request)
        if check is None:
            err_code, err_msg = mt5.last_error()
            msg = f"order_check returned None — MT5 last_error: ({err_code}) {err_msg}"
            logger.error(
                "MT5 order_check [%s] symbol=%s: %s | request=%s",
                order_type, signal.symbol, msg, request,
            )
            return self._fail(signal, order_type, err_code if err_code else -1, msg)

        if check.retcode != 0:
            msg = check.comment or f"retcode={check.retcode}"
            logger.error(
                "MT5 order_check failed [%s] symbol=%s retcode=%d: %s",
                order_type, signal.symbol, check.retcode, msg,
            )
            return self._fail(signal, order_type, check.retcode, f"order_check: {msg}")

        logger.info(
            "MT5 order_send [%s] symbol=%s vol=%.2f price=%s sl=%s tp=%s",
            order_type, signal.symbol, signal.volume,
            request.get("price"), request.get("sl"), request.get("tp"),
        )
        result = mt5.order_send(request)

        if result is None:
            code, msg = mt5.last_error()
            logger.error(
                "MT5 order_send returned None [%s] symbol=%s: (%d) %s",
                order_type, signal.symbol, code, msg,
            )
            return self._fail(signal, order_type, code, f"order_send None: ({code}) {msg}")

        # Requote retry (once)
        if result.retcode == _RETCODE_REQUOTE and self._retry_requote and not _retry:
            logger.warning("MT5 REQUOTE — retrying once with fresh price")
            tick = mt5.symbol_info_tick(signal.symbol)
            if tick:
                is_buy = "BUY" in signal.action.upper()
                new_price = tick.ask if is_buy else tick.bid
                request["price"] = new_price
                return self._do_send(mt5, request, order_type, signal, new_price, _retry=True)

        if result.retcode not in (_RETCODE_DONE, _RETCODE_PLACED):
            msg = result.comment or f"retcode={result.retcode}"
            if result.retcode == _RETCODE_AUTO_TRADING_OFF:
                logger.error(
                    "MT5 AutoTrading DISABLED — bật nút 'AutoTrading' trên toolbar MT5 "
                    "hoặc Tools → Options → Expert Advisors → Allow automated trading. "
                    "symbol=%s retcode=%d",
                    signal.symbol, result.retcode,
                )
            else:
                logger.error(
                    "MT5 order failed [%s] symbol=%s retcode=%d: %s",
                    order_type, signal.symbol, result.retcode, msg,
                )
            return self._fail(signal, order_type, result.retcode, msg)

        ticket    = result.order
        fill_price = result.price if result.price else requested_price
        logger.info(
            "MT5 order OK [%s] ticket=%d fill=%.5f vol=%.2f",
            order_type, ticket, fill_price, result.volume,
        )
        return OrderResult(
            success=True,
            order_type=order_type,
            symbol=signal.symbol,
            action=signal.action,
            volume=result.volume or signal.volume,
            price=fill_price,
            sl=float(signal.sl),
            tp=float(signal.tp1),
            ticket=ticket,
            strategy_name=signal.strategy_name,
            order_id=getattr(signal, "order_id", ""),
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _fail(
        self,
        signal: "CompleteSignal",
        order_type: str,
        code: int,
        msg: str,
    ) -> OrderResult:
        logger.error(
            "MT5OrderExecutor FAIL [%s] %s %s: %d %s",
            order_type, signal.action, signal.symbol, code, msg,
        )
        return OrderResult(
            success=False,
            order_type=order_type,
            symbol=signal.symbol,
            action=signal.action,
            volume=signal.volume,
            price=signal.entry,
            sl=signal.sl,
            tp=signal.tp1,
            error_code=code,
            error_msg=msg,
            strategy_name=signal.strategy_name,
            order_id=getattr(signal, "order_id", ""),
        )

    def _execute_cancel(self, ticket: int, order_id: str) -> OrderResult:
        """
        Huỷ pending order trên MT5 theo ticket (TRADE_ACTION_REMOVE).

        Trước khi gửi lệnh huỷ, kiểm tra order còn tồn tại qua orders_get().
        Nếu order đã được fill / đã huỷ trước đó → coi như thành công (idempotent).
        """
        mt5 = getattr(self._connector, "_mt5", None)
        if mt5 is None or not self._connector.is_connected():
            logger.error("MT5 cancel: connector not available ticket=%d", ticket)
            return OrderResult(
                success=False, order_type="CANCEL", symbol="", action="CANCEL",
                volume=0, price=0, sl=0, tp=0, ticket=ticket,
                error_code=-1, error_msg="MT5 not connected", order_id=order_id,
            )

        # Kiểm tra order có còn pending không
        pending = mt5.orders_get(ticket=ticket)
        if pending is None or len(pending) == 0:
            # Order không còn tồn tại — đã fill hoặc đã huỷ rồi
            logger.info(
                "MT5 cancel: ticket=%d not found (already filled/cancelled) — skip",
                ticket,
            )
            return OrderResult(
                success=True, order_type="CANCEL", symbol="", action="CANCEL",
                volume=0, price=0, sl=0, tp=0, ticket=ticket, order_id=order_id,
                error_msg="order not found (already closed)",
            )

        symbol = pending[0].symbol if pending else ""
        request = {
            "action": mt5.TRADE_ACTION_REMOVE,
            "order":  ticket,
        }
        result = mt5.order_send(request)

        if result is None:
            code, msg = mt5.last_error()
            logger.error(
                "MT5 cancel failed ticket=%d order_id=%s: (%d) %s",
                ticket, order_id, code, msg,
            )
            return OrderResult(
                success=False, order_type="CANCEL", symbol=symbol, action="CANCEL",
                volume=0, price=0, sl=0, tp=0, ticket=ticket,
                error_code=code, error_msg=f"order_send None: ({code}) {msg}",
                order_id=order_id,
            )

        if result.retcode not in _RETCODE_OK_CANCEL:
            msg = result.comment or f"retcode={result.retcode}"
            logger.error(
                "MT5 cancel failed ticket=%d order_id=%s retcode=%d: %s",
                ticket, order_id, result.retcode, msg,
            )
            return OrderResult(
                success=False, order_type="CANCEL", symbol=symbol, action="CANCEL",
                volume=0, price=0, sl=0, tp=0, ticket=ticket,
                error_code=result.retcode, error_msg=msg, order_id=order_id,
            )

        logger.info(
            "MT5 pending order cancelled OK: ticket=%d symbol=%s order_id=%s retcode=%d",
            ticket, symbol, order_id, result.retcode,
        )
        return OrderResult(
            success=True, order_type="CANCEL", symbol=symbol, action="CANCEL",
            volume=0, price=0, sl=0, tp=0, ticket=ticket, order_id=order_id,
        )
