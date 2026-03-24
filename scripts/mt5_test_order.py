#!/usr/bin/env python3
"""
Đặt 1 lệnh thử trên MT5 (market) để debug — log chi tiết order_check / order_send.

Chạy trên Windows, terminal MT5 đã mở và đã login đúng tài khoản.

  cd trading-system
  python scripts/mt5_test_order.py
  python scripts/mt5_test_order.py --symbol XAUUSDm --side sell --volume 0.01 --dry-run

Đọc mt5.* và execution.* từ config.yaml (cùng thư mục gốc project).
"""

from __future__ import annotations

import argparse
import platform
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.config_loader import ConfigLoader  # noqa: E402


def _filling_map(mt5) -> dict[str, int]:
    return {
        "IOC": mt5.ORDER_FILLING_IOC,
        "FOK": mt5.ORDER_FILLING_FOK,
        "RETURN": mt5.ORDER_FILLING_RETURN,
    }


def _dump_trade_result(prefix: str, r, mt5) -> None:
    """Log mọi thuộc tính hữu ích của kết quả order_check / order_send."""
    if r is None:
        print(f"{prefix}: None")
        return
    lines = [f"{prefix}:"]
    for name in (
        "retcode",
        "retcode_external",
        "comment",
        "request_id",
        "deal",
        "order",
        "volume",
        "price",
        "bid",
        "ask",
        "margin_initial",
        "margin_maintenance",
        "margin_free",
    ):
        if hasattr(r, name):
            lines.append(f"  {name}: {getattr(r, name)!r}")
    seen = {name for name in (
        "retcode retcode_external comment request_id deal order volume price bid ask "
        "margin_initial margin_maintenance margin_free"
    ).split()}
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
        print(
            f"  (so sánh: TRADE_RETCODE_DONE={getattr(mt5, 'TRADE_RETCODE_DONE', '?')} "
            f"TRADE_RETCODE_PLACED={getattr(mt5, 'TRADE_RETCODE_PLACED', '?')})"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="MT5: 1 lệnh thử + log chi tiết")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"), help="Đường dẫn config.yaml")
    parser.add_argument("--symbol", default=None, help="Symbol (mặc định: cặp đầu trong trading_pairs)")
    parser.add_argument("--side", choices=("buy", "sell"), default="buy")
    parser.add_argument("--volume", type=float, default=None, help="Lot (mặc định: min_lot từ risk_management)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Chỉ chạy order_check, không gọi order_send",
    )
    args = parser.parse_args()

    if platform.system() != "Windows":
        print("Lỗi: MetaTrader5 Python API chỉ chạy trên Windows.", file=sys.stderr)
        return 2

    try:
        import MetaTrader5 as mt5
    except ImportError as e:
        print("Lỗi: pip install MetaTrader5", file=sys.stderr)
        print(e, file=sys.stderr)
        return 2

    cfg_path = Path(args.config)
    if not cfg_path.is_file():
        print(f"Lỗi: không tìm thấy {cfg_path}", file=sys.stderr)
        return 2

    # Load config (singleton — reset path bằng cách tạo loader trực tiếp nếu cần)
    ConfigLoader._instance = None  # type: ignore[attr-defined]
    cfg = ConfigLoader.load(str(cfg_path))
    raw = cfg.raw
    mt5_cfg = raw.get("mt5", {}) or {}
    exec_cfg = raw.get("execution", {}) or {}
    risk_cfg = raw.get("risk_management", {}) or {}

    login = int(mt5_cfg.get("login", 0))
    password = str(mt5_cfg.get("password", ""))
    server = str(mt5_cfg.get("server", ""))
    timeout = int(mt5_cfg.get("timeout", 60_000))

    if not login or not password:
        print("Lỗi: mt5.login / mt5.password trong config.yaml", file=sys.stderr)
        return 2

    symbol = args.symbol
    if not symbol:
        pairs = raw.get("trading_pairs", [])
        if not pairs:
            print("Lỗi: trading_pairs trống — dùng --symbol", file=sys.stderr)
            return 2
        symbol = pairs[0]["symbol"]

    vol = args.volume if args.volume is not None else float(risk_cfg.get("min_lot_size", 0.01))
    deviation = int(exec_cfg.get("deviation_points", 20))
    magic = int(exec_cfg.get("magic_number", 20260101))
    filling_name = str(exec_cfg.get("filling_mode", "IOC")).upper()
    filling = _filling_map(mt5).get(filling_name, mt5.ORDER_FILLING_IOC)
    comment = str(exec_cfg.get("comment_prefix", "TestOrder"))[:31]

    print("=== MT5 test order ===")
    print(f"  symbol={symbol}  side={args.side}  volume={vol}")
    print(f"  filling={filling_name}  deviation={deviation}  magic={magic}")

    if not mt5.initialize():
        err = mt5.last_error()
        print(f"Lỗi: mt5.initialize() failed: {err}", file=sys.stderr)
        return 1

    if not mt5.login(login=login, password=password, server=server, timeout=timeout):
        err = mt5.last_error()
        print(f"Lỗi: mt5.login failed: {err}", file=sys.stderr)
        mt5.shutdown()
        return 1

    acc = mt5.account_info()
    if acc:
        print(f"  account: login={acc.login} server={acc.server} balance={acc.balance} {acc.currency}")

    if not mt5.symbol_select(symbol, True):
        err = mt5.last_error()
        print(f"Cảnh báo: symbol_select({symbol}) — last_error={err}")

    info = mt5.symbol_info(symbol)
    if info is None:
        err = mt5.last_error()
        print(f"Lỗi: symbol_info({symbol}) = None  last_error={err}", file=sys.stderr)
        mt5.shutdown()
        return 1

    print(
        f"  symbol_info: digits={info.digits} point={info.point} "
        f"volume_min={info.volume_min} volume_max={info.volume_max} "
        f"trade_mode={info.trade_mode}"
    )

    vol = max(vol, float(info.volume_min))
    vol = min(vol, float(info.volume_max))
    step = float(getattr(info, "volume_step", 0.01) or 0.01)
    vol = round(round(vol / step) * step, 8)
    print(f"  volume sau clamp (step={step}): {vol}")

    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        err = mt5.last_error()
        print(f"Lỗi: không có tick cho {symbol}  last_error={err}", file=sys.stderr)
        mt5.shutdown()
        return 1

    is_buy = args.side == "buy"
    price = tick.ask if is_buy else tick.bid
    otype = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": float(vol),
        "type": otype,
        "price": price,
        "sl": 0.0,
        "tp": 0.0,
        "deviation": deviation,
        "magic": magic,
        "comment": comment,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": filling,
    }

    print("\n--- Request ---")
    for k, v in request.items():
        print(f"  {k}: {v!r}")

    check = mt5.order_check(request)
    print("\n--- order_check ---")
    _dump_trade_result("check", check, mt5)
    if check is None:
        print(f"last_error sau order_check: {mt5.last_error()}")
        mt5.shutdown()
        return 1
    if check.retcode != 0:
        print(f"\norder_check retcode != 0 — không gửi lệnh. last_error: {mt5.last_error()}")
        mt5.shutdown()
        return 1

    if args.dry_run:
        print("\n--dry-run: bỏ qua order_send")
        mt5.shutdown()
        return 0

    print("\n--- order_send ---")
    result = mt5.order_send(request)
    _dump_trade_result("send", result, mt5)
    if result is None:
        print(f"last_error: {mt5.last_error()}")
        mt5.shutdown()
        return 1

    done = getattr(mt5, "TRADE_RETCODE_DONE", 10009)
    if result.retcode != done:
        print(f"\nThất bại: retcode={result.retcode} comment={result.comment!r}")
        print(f"last_error: {mt5.last_error()}")
        mt5.shutdown()
        return 1

    print(f"\nOK: ticket/order={getattr(result, 'order', None)} deal={getattr(result, 'deal', None)}")
    mt5.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
