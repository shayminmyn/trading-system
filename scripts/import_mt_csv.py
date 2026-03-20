"""
MetaTrader 4/5 Historical Data Importer
========================================
Converts MT4/MT5 CSV exports to the system's standard format and saves them to
data/historical/{SYMBOL}_{TIMEFRAME}.csv.

Supported MT export formats
----------------------------
1. MT4 History Center export (no header):
       2022.04.08,18:00,1942.248,1944.128,1941.842,1943.918,60,342
2. MT5 Toolbox export (with header):
       <DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<TICKVOL>,<VOL>,<SPREAD>
       2022.04.08,18:00:00,1942.248,1944.128,1941.842,1943.918,60,0,342
3. Generic date+time split CSV (YYYY-MM-DD, HH:MM:SS):
       2022-04-08,18:00:00,1942.248,1944.128,1941.842,1943.918,60,342

Output standard format
-----------------------
    timestamp,open,high,low,close,volume
    2022-04-08 18:00:00+00:00,1942.248,1944.128,...

Usage
-----
    # Auto-detect symbol + TF from filename:
    python scripts/import_mt_csv.py XAUUSD_H1.csv

    # Explicit symbol + TF:
    python scripts/import_mt_csv.py path/to/file.csv --symbol XAUUSD --tf H1

    # Import multiple files at once:
    python scripts/import_mt_csv.py *.csv --symbol XAUUSD

    # Derive H4 from H1 after import:
    python scripts/import_mt_csv.py XAUUSD_H1.csv --derive-h4
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd

DATA_DIR = Path("data/historical")

# Timeframe label → pandas resample frequency
TF_RESAMPLE: dict[str, str] = {
    "M1": "1min", "M5": "5min", "M15": "15min", "M30": "30min",
    "H1": "1h", "H4": "4h", "D1": "1D",
}

# Keep this much data after import (days) — matches download_historical TF_DEPTHS
TF_DEPTHS: dict[str, int] = {
    "D1": 3 * 365, "H4": 3 * 365, "H1": 2 * 365,
    "M15": 90, "M5": 90, "M1": 90,
}

# ── Parsing helpers ───────────────────────────────────────────────────────────

def _guess_symbol_tf(filename: str) -> tuple[str | None, str | None]:
    """Try to extract SYMBOL and TIMEFRAME from a filename like XAUUSD_H1.csv."""
    stem = Path(filename).stem.upper()
    tf_pat = re.search(r"_(M1|M5|M15|M30|H1|H4|D1)$", stem)
    if tf_pat:
        tf = tf_pat.group(1)
        sym = stem[: tf_pat.start()]
        return sym or None, tf
    return None, None


def _parse_mt_csv(path: Path) -> pd.DataFrame:
    """
    Parse any MT4/MT5 CSV variant into a clean DataFrame with columns:
      timestamp (tz-aware UTC), open, high, low, close, volume
    """
    # Sniff the first two content lines
    lines: list[str] = []
    with path.open(encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = raw.strip()
            if line:
                lines.append(line)
            if len(lines) == 2:
                break

    if not lines:
        raise ValueError(f"Empty file: {path}")

    def _is_header(line: str) -> bool:
        first = line.split(",")[0].strip().lstrip("<").rstrip(">").upper()
        return first in ("DATE", "TIMESTAMP", "TIME", "DATETIME",
                         "OPEN", "TICKER", "SYMBOL")

    has_header = len(lines) >= 1 and _is_header(lines[0])
    # Sample the first data row
    data_line = lines[1] if has_header and len(lines) > 1 else lines[0]
    tokens = data_line.split(",")
    n = len(tokens)

    # Column assignment
    # Minimum 6 cols: date, time, open, high, low, close
    # Optional: tickvol / vol / spread
    col_names = ["date", "time", "open", "high", "low", "close",
                 "tickvol", "vol", "spread"][:n]

    df = pd.read_csv(
        path,
        header=0 if has_header else None,
        names=col_names,
        usecols=list(range(min(n, 7))),
        dtype=str,
        on_bad_lines="skip",
    )
    # Trim to the cols we named
    df = df.iloc[:, :len(col_names)]
    df.columns = col_names

    # Build timestamp from date + time columns
    date_str = df["date"].str.strip().str.replace(".", "-", regex=False)
    time_str = df["time"].str.strip()
    ts_combined = date_str + " " + time_str

    df["timestamp"] = pd.to_datetime(
        ts_combined, format="mixed", dayfirst=False, utc=True, errors="coerce"
    )

    for col in ("open", "high", "low", "close"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    vol_col = "tickvol" if "tickvol" in df.columns else "vol" if "vol" in df.columns else None
    df["volume"] = pd.to_numeric(df[vol_col], errors="coerce").fillna(0) \
        if vol_col else 0.0

    df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df[["timestamp", "open", "high", "low", "close", "volume"]].astype(
        {"open": float, "high": float, "low": float,
         "close": float, "volume": float}
    )


def _trim_to_depth(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    depth = TF_DEPTHS.get(tf)
    if depth is None:
        return df
    cutoff = pd.Timestamp(datetime.now(tz=timezone.utc) - timedelta(days=depth))
    return df[df["timestamp"] >= cutoff].reset_index(drop=True)


def _derive_h4(h1_path: Path, symbol: str) -> None:
    """Resample H1 CSV → H4 and save to DATA_DIR."""
    out = DATA_DIR / f"{symbol}_H4.csv"
    print(f"  Deriving H4 from {h1_path.name} ...")

    h1 = _parse_mt_csv(h1_path) if _sniff_is_mt(h1_path) else _load_standard(h1_path)
    h1 = h1.set_index("timestamp")

    h4 = h1.resample("4h").agg(
        open=("open", "first"), high=("high", "max"),
        low=("low", "min"), close=("close", "last"),
        volume=("volume", "sum"),
    ).dropna(subset=["open"]).reset_index()

    h4 = _trim_to_depth(h4, "H4")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    h4.to_csv(out, index=False)
    _print_result(symbol, "H4", h4, out)


def _sniff_is_mt(path: Path) -> bool:
    """Quick check: does the file look like MT4/MT5 format?"""
    with path.open(encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = raw.strip()
            if line:
                tokens = line.split(",")
                if len(tokens) >= 7:
                    return True
                # date.dot separated?
                if "." in tokens[0] and tokens[0].replace(".", "").isdigit():
                    return True
                return False
    return False


def _load_standard(path: Path) -> pd.DataFrame:
    """Load our own standard format CSV."""
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True,
                                     errors="coerce")
    df = df.dropna(subset=["timestamp"])
    return df[["timestamp", "open", "high", "low", "close", "volume"]].astype(
        {"open": float, "high": float, "low": float,
         "close": float, "volume": float}
    )


def _print_result(symbol: str, tf: str, df: pd.DataFrame, out: Path) -> None:
    if df.empty:
        print(f"  ✗  {symbol}/{tf}: 0 bars (check file format)")
        return
    first = df["timestamp"].iloc[0]
    last  = df["timestamp"].iloc[-1]
    bar_h = (df["timestamp"].diff().dropna().dt.total_seconds().median() / 3600)
    print(
        f"  ✓  {symbol}/{tf}: {len(df):,} bars"
        f"  |  {first.date()} → {last.date()}"
        f"  |  bar≈{bar_h:.1f}h"
        f"  →  {out.name}"
    )


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Import MetaTrader 4/5 CSV exports into the trading system.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("files", nargs="+", metavar="FILE",
                   help="MT4/MT5 CSV export file(s) to import.")
    p.add_argument("--symbol", "-s", default=None,
                   help="Symbol name, e.g. XAUUSD. Auto-detected from filename if omitted.")
    p.add_argument("--tf", "-t", default=None,
                   choices=list(TF_DEPTHS.keys()),
                   help="Timeframe label. Auto-detected from filename if omitted.")
    p.add_argument("--out-dir", default=str(DATA_DIR), metavar="DIR",
                   help=f"Output directory (default: {DATA_DIR}).")
    p.add_argument("--derive-h4", action="store_true",
                   help="After importing H1, also derive and save H4.")
    p.add_argument("--no-trim", action="store_true",
                   help="Do not trim to TF_DEPTHS (keep all bars).")
    return p.parse_args()


def main() -> None:
    args  = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    global DATA_DIR
    DATA_DIR = out_dir  # allow --out-dir override for derive-h4

    print(f"\n  MetaTrader CSV Importer  →  {out_dir}\n")

    for file_arg in args.files:
        for path in sorted(Path(".").glob(file_arg)) if "*" in file_arg else [Path(file_arg)]:
            if not path.exists():
                print(f"  ✗  File not found: {path}")
                continue

            # Determine symbol + TF
            sym, tf = args.symbol, args.tf
            if sym is None or tf is None:
                auto_sym, auto_tf = _guess_symbol_tf(path.name)
                sym = sym or auto_sym
                tf  = tf  or auto_tf

            if not sym or not tf:
                print(
                    f"  ✗  Cannot detect symbol/TF from filename '{path.name}'. "
                    "Use --symbol and --tf."
                )
                continue

            sym = sym.upper()
            tf  = tf.upper()

            print(f"  Importing  {path.name}  →  {sym}/{tf}")
            try:
                if _sniff_is_mt(path):
                    df = _parse_mt_csv(path)
                else:
                    df = _load_standard(path)
            except Exception as exc:
                print(f"  ✗  Parse error: {exc}")
                continue

            if not args.no_trim:
                df = _trim_to_depth(df, tf)

            out = out_dir / f"{sym}_{tf}.csv"
            df.to_csv(out, index=False)
            _print_result(sym, tf, df, out)

            if args.derive_h4 and tf == "H1":
                _derive_h4(path, sym)

    print()


if __name__ == "__main__":
    main()
