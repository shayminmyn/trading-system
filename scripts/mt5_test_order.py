#!/usr/bin/env python3
"""
Đặt lệnh thử trên MT5 (market hoặc limit) để debug.
Logic giống hệt MT5OrderExecutor: symbol_select, comment format,
expiration, fallback GTC khi 10022.

Chạy trên Windows, terminal MT5 đã mở và đã login đúng tài khoản.

  python scripts/mt5_test_order.py
  python scripts/mt5_test_order.py --type limit --offset-pips 50
  python scripts/mt5_test_order.py --symbol XAUUSDm --side sell --volume 0.01 --dry-run
  python scripts/mt5_test_order.py --type limit --expiry-hours 1 --sl-pips 30 --tp-pips 60

Đọc mt5.* và execution.* từ config.yaml (cùng thư mục gốc project).
"""

from __future__ import annotations

import argparse
import platform
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.config_loader import ConfigLoader  # noqa: E402

_RETCODE_DONE               = 10009
_RETCODE_PLACED             = 10008
_RETCODE_INVALID_EXPIRATION = 10022


def _filling_map(mt5) -> dict[str, int]:
    return {
        "IOC":    mt5.ORDER_FILLING_IOC,
        "FOK":    mt5.ORDER_FILLING_FOK,
        "RETURN": mt5.ORDER_FILLING_RETURN,
    }


def _dump_result(prefix: str, r, mt5) -> None:
    """In mọi thuộc tính của kết quả order_check / order_send."""
    if r is None:
        print(f"{prefix}: None")
        return
    priority = [
        "retcode", "retcode_external", "comment", "request_id",
        "deal", "order", "volume", "price", "bid", "ask",
        "margin_initial", "margin_maintenance", "margin_free",
    ]
    lines = [f"{prefix}:"]
    seen: set[str] = set()
    for name in priority:
        if hasattr(r, name):
            lines.append(f"  {name}: {getattr(r, name)!r}")
            seen.add(name)
    for name in sorted(dir(r)):
        if name.startswith("_") or name in seen:
            continue
        try:
            val = getattr(r, name)
        except Exception:
            continue
        if callable(val):
            continue
        lines.append(f"  {name}: {val!r}")
    print("\n".join(lines))
    rc = getattr(r, "retcode", None)
    if rc is not None:
        done  = getattr(mt5, "TRADE_RETCODE_DONE",   _RETCODE_DONE)
        placed = getattr(mt5, "TRADE_RETCODE_PLACED", _RETCODE_PLACED)
        print(f"  (RETCODE_DONE={done}  RETCODE_PLACED={placed})")


def _clamp_volume(vol: float, info) -> float:
    vol = max(vol, float(info.volume_min))
    vol = min(vol, float(info.volume_max))
    step = float(getattr(info, "volume_step", 0.01) or 0.01)
    return round(round(vol / step) * step, 8)


def _build_comment(prefix: str, order_id: str) -> str:
    """Giống logic mt5_executor._execute: '{prefix} {order_id}'[:31]"""
    max_oid = 31 - len(prefix) - 1
    if order_id:
        return f"{order_id[:max_oid]}"[:31]
    return f"{prefix[:31]}"[:31]


def _send_and_check(mt5, request: dict, dry_run: bool, label: str) -> int:
    """
    Gọi order_check rồi order_send.
    Giống _do_send trong MT5OrderExecutor:
    - Nếu order_check trả 10022 → fallback GTC rồi retry 1 lần.
    Trả về exit code (0 = OK).
    """
    print(f"\n--- {label} Request ---")
    for k, v in request.items():
        print(f"  {k}: {v!r}")

    # ── order_check ──────────────────────────────────────────────────────────
    check = mt5.order_check(request)
    print(f"\n--- {label} order_check ---")
    _dump_result("check", check, mt5)

    if check is None:
        print(f"last_error: {mt5.last_error()}", file=sys.stderr)
        return 1

    # 10022: broker không hỗ trợ ORDER_TIME_SPECIFIED → fallback GTC
    if check.retcode == _RETCODE_INVALID_EXPIRATION and "expiration" in request:
        print(
            f"\n  ⚠ retcode=10022 (invalid expiration) — "
            f"broker không hỗ trợ SPECIFIED, thử lại với GTC..."
        )
        request.pop("expiration")
        request["type_time"] = mt5.ORDER_TIME_GTC
        check = mt5.order_check(request)
        print(f"\n--- {label} order_check (retry GTC) ---")
        _dump_result("check", check, mt5)
        if check is None:
            print(f"last_error: {mt5.last_error()}", file=sys.stderr)
            return 1

    if check.retcode != 0:
        print(
            f"\n  ✗ order_check retcode={check.retcode}: {check.comment!r}",
            file=sys.stderr,
        )
        print(f"  last_error: {mt5.last_error()}", file=sys.stderr)
        return 1

    if dry_run:
        print(f"\n--dry-run: bỏ qua order_send  ✓ check OK")
        return 0

    # ── order_send ────────────────────────────────────────────────────────────
    print(f"\n--- {label} order_send ---")
    result = mt5.order_send(request)
    _dump_result("send", result, mt5)

    if result is None:
        print(f"last_error: {mt5.last_error()}", file=sys.stderr)
        return 1

    if result.retcode not in (_RETCODE_DONE, _RETCODE_PLACED):
        print(
            f"\n  ✗ FAILED retcode={result.retcode}: {result.comment!r}",
            file=sys.stderr,
        )
        print(f"  last_error: {mt5.last_error()}", file=sys.stderr)
        return 1

    ticket = getattr(result, "order", None)
    deal   = getattr(result, "deal", None)
    print(f"\n  ✓ OK  ticket={ticket}  deal={deal}  retcode={result.retcode}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="MT5 test order — market hoặc limit, log chi tiết như mt5_executor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config",  default=str(ROOT / "config.yaml"))
    parser.add_argument("--symbol",  default=None, help="Symbol (mặc định: trading_pairs[0])")
    parser.add_argument("--side",    choices=("buy", "sell"), default="buy")
    parser.add_argument("--volume",  type=float, default=None)
    parser.add_argument(
        "--type", dest="order_type",
        choices=("market", "limit"), default="market",
        help="market (TRADE_ACTION_DEAL) hoặc limit (TRADE_ACTION_PENDING)",
    )
    # Limit-specific
    parser.add_argument(
        "--offset-pips", type=float, default=20.0,
        help="Limit price = current ± offset_pips pips (default 20). "
             "BUY LIMIT: bid - offset; SELL LIMIT: ask + offset",
    )
    parser.add_argument(
        "--limit-price", type=float, default=None,
        help="Ghi đè limit price tuyệt đối (bỏ qua --offset-pips)",
    )
    parser.add_argument(
        "--sl-pips", type=float, default=0.0,
        help="Stop loss tính từ limit price (pips). 0 = không đặt SL",
    )
    parser.add_argument(
        "--tp-pips", type=float, default=0.0,
        help="Take profit tính từ limit price (pips). 0 = không đặt TP",
    )
    parser.add_argument(
        "--expiry-hours", type=float, default=0.0,
        help="Giờ hết hạn cho limit order (0 = GTC). "
             "Giống expiration_hours trong config",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if platform.system() != "Windows":
        print("Lỗi: MetaTrader5 Python API chỉ chạy trên Windows.", file=sys.stderr)
        return 2

    try:
        import MetaTrader5 as mt5
    except ImportError as e:
        print(f"Lỗi: pip install MetaTrader5\n{e}", file=sys.stderr)
        return 2

    cfg_path = Path(args.config)
    if not cfg_path.is_file():
        print(f"Lỗi: không tìm thấy {cfg_path}", file=sys.stderr)
        return 2

    ConfigLoader._instance = None  # type: ignore[attr-defined]
    cfg      = ConfigLoader.load(str(cfg_path))
    raw      = cfg.raw
    mt5_cfg  = raw.get("mt5",  {}) or {}
    exec_cfg = raw.get("execution", {}) or {}
    risk_cfg = raw.get("risk_management", {}) or {}

    login    = int(mt5_cfg.get("login", 0))
    password = str(mt5_cfg.get("password", ""))
    server   = str(mt5_cfg.get("server", ""))
    timeout  = int(mt5_cfg.get("timeout", 60_000))

    if not login or not password:
        print("Lỗi: thiếu mt5.login / mt5.password trong config.yaml", file=sys.stderr)
        return 2

    symbol = args.symbol or (raw.get("trading_pairs") or [{}])[0].get("symbol", "")
    if not symbol:
        print("Lỗi: chỉ định --symbol hoặc thêm trading_pairs vào config", file=sys.stderr)
        return 2

    vol          = args.volume if args.volume is not None else float(risk_cfg.get("min_lot_size", 0.01))
    deviation    = int(exec_cfg.get("deviation_points", 20))
    magic        = int(exec_cfg.get("magic_number", 20260101))
    filling_name = str(exec_cfg.get("filling_mode", "IOC")).upper()
    prefix       = str(exec_cfg.get("comment_prefix", "Bot"))

    # order_id cho comment (giống format SYMBOL-TF-YYYYMMdd-HHMM)
    now_str  = datetime.now().strftime("%Y%m%d-%H%M")
    order_id = f"{symbol.upper()}-TEST-{now_str}"
    comment  = _build_comment(prefix, order_id)

    print("=" * 50)
    print(f"  symbol={symbol}  side={args.side}  type={args.order_type}  vol={vol}")
    print(f"  filling={filling_name}  deviation={deviation}  magic={magic}")
    print(f"  comment={comment!r}")
    print("=" * 50)

    # ── Connect MT5 ──────────────────────────────────────────────────────────
    if not mt5.initialize():
        print(f"Lỗi: initialize() failed: {mt5.last_error()}", file=sys.stderr)
        return 1

    if not mt5.login(login=login, password=password, server=server, timeout=timeout):
        print(f"Lỗi: login failed: {mt5.last_error()}", file=sys.stderr)
        mt5.shutdown()
        return 1

    acc = mt5.account_info()
    if acc:
        print(f"  account: {acc.login}@{acc.server}  balance={acc.balance} {acc.currency}")

    # ── Symbol select (giống _ensure_symbol trong executor) ──────────────────
    if not mt5.symbol_select(symbol, True):
        print(f"  ⚠ symbol_select({symbol}) failed: {mt5.last_error()}")

    info = mt5.symbol_info(symbol)
    if info is None:
        print(f"Lỗi: symbol_info({symbol}) = None  last_error={mt5.last_error()}", file=sys.stderr)
        mt5.shutdown()
        return 1

    print(
        f"  symbol_info: digits={info.digits}  point={info.point}"
        f"  vol_min={info.volume_min}  vol_max={info.volume_max}"
        f"  trade_mode={info.trade_mode}"
    )

    vol = _clamp_volume(vol, info)
    print(f"  volume (clamped): {vol}")

    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        print(f"Lỗi: không có tick cho {symbol}  last_error={mt5.last_error()}", file=sys.stderr)
        mt5.shutdown()
        return 1

    print(f"  tick: bid={tick.bid}  ask={tick.ask}")

    filling    = _filling_map(mt5).get(filling_name, mt5.ORDER_FILLING_IOC)
    is_buy     = args.side == "buy"
    pip_size   = info.point * 10  # 1 pip

    # ── Build request ─────────────────────────────────────────────────────────
    if args.order_type == "market":
        price  = tick.ask if is_buy else tick.bid
        otype  = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
        request = {
            "action":       mt5.TRADE_ACTION_DEAL,
            "symbol":       symbol,
            "volume":       float(vol),
            "type":         otype,
            "price":        price,
            "sl":           0.0,
            "tp":           0.0,
            "deviation":    deviation,
            "magic":        magic,
            "comment":      comment,
            "type_time":    mt5.ORDER_TIME_GTC,
            "type_filling": filling,
        }
        label = "MARKET"

    else:  # limit
        # Limit price: ghi đè tuyệt đối hoặc tính từ offset pips
        if args.limit_price is not None:
            limit_price = args.limit_price
        else:
            offset = args.offset_pips * pip_size
            # BUY LIMIT đặt dưới giá thị trường, SELL LIMIT đặt trên
            limit_price = (tick.bid - offset) if is_buy else (tick.ask + offset)

        # SL / TP tính từ limit price
        sl_price = 0.0
        tp_price = 0.0
        if args.sl_pips > 0:
            sl_dist  = args.sl_pips * pip_size
            sl_price = (limit_price - sl_dist) if is_buy else (limit_price + sl_dist)
        if args.tp_pips > 0:
            tp_dist  = args.tp_pips * pip_size
            tp_price = (limit_price + tp_dist) if is_buy else (limit_price - tp_dist)

        otype = mt5.ORDER_TYPE_BUY_LIMIT if is_buy else mt5.ORDER_TYPE_SELL_LIMIT

        # Expiry — giống _send_limit trong executor
        expiry_hours = args.expiry_hours if args.expiry_hours > 0 else float(
            exec_cfg.get("expiration_hours", 0)
        )
        use_expiry = expiry_hours > 0

        request = {
            "action":       mt5.TRADE_ACTION_PENDING,
            "symbol":       symbol,
            "volume":       float(vol),
            "type":         otype,
            "price":        round(limit_price, info.digits),
            "sl":           round(sl_price, info.digits),
            "tp":           round(tp_price, info.digits),
            "deviation":    deviation,
            "magic":        magic,
            "comment":      comment,
            "type_time":    mt5.ORDER_TIME_SPECIFIED if use_expiry else mt5.ORDER_TIME_GTC,
        }

        if use_expiry:
            exp = datetime.now(tz=timezone.utc) + timedelta(hours=expiry_hours)
            expire_timestamp = int(exp.timestamp())
            # MT5 yêu cầu: không tzinfo, không microseconds
            request["expiration"] = expire_timestamp

        direction  = "BUY LIMIT" if is_buy else "SELL LIMIT"
        label      = f"LIMIT ({direction})"
        expiry_str = f"expiry={request.get('expiration', 'GTC')}"
        print(
            f"\n  limit_price={request['price']}  sl={request['sl']}  tp={request['tp']}"
            f"  {expiry_str}"
        )
        print(
            f"  offset={args.offset_pips}pips  sl_pips={args.sl_pips}  tp_pips={args.tp_pips}"
        )

    rc = _send_and_check(mt5, request, args.dry_run, label)
    mt5.shutdown()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
