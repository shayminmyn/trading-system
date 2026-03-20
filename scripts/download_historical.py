#!/usr/bin/env python3
"""
Historical Data Downloader
==========================
Downloads historical OHLCV data for Forex pairs and CFDs.

Data depth per timeframe
------------------------
  D1, H4, H1  →  2-3 years   (suitable for trend / swing analysis)
  M15, M5     →  3 months    (suitable for intraday / scalp backtest)

Backends
--------
1. Yahoo Finance (yfinance)  — all symbols, no API key
   - D1  : up to 10 years
   - H1  : up to 730 days; H4 derived by resampling H1
   - M15 : chunked 2 × 60-day windows → 120 days (covers 3 months)
   - M5  : same chunked approach

2. Dukascopy Public HTTP API  — Forex + Metals, no API key
   - Downloads daily M1 candles (LZMA bi5 binary, 24 bytes/bar)
   - Long TFs (D1, H4, H1) : pulls up to `--years` of M1
   - Short TFs (M15, M5)   : pulls only 3 months of M1 (fast, ~90 requests)
   - Resample M1 → M5 / M15 / H1 / H4 / D1 and trims each file to target depth
   - ~365 requests per year per symbol

Usage
-----
  python scripts/download_historical.py                         # all symbols
  python scripts/download_historical.py --symbols XAUUSD EURUSD US500
  python scripts/download_historical.py --source yfinance
  python scripts/download_historical.py --source dukascopy
  python scripts/download_historical.py --timeframes D1 H4 H1
  python scripts/download_historical.py --list
"""

from __future__ import annotations

import argparse
import lzma
import struct
import sys
import time
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Iterator

import pandas as pd
import requests
from tqdm import tqdm

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT     = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data" / "historical"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── Symbol registries ─────────────────────────────────────────────────────────

# yfinance ticker → canonical symbol name
YFINANCE_MAP: dict[str, str] = {
    # ── Forex ──
    "EURUSD=X": "EURUSD",
    "GBPUSD=X": "GBPUSD",
    "USDJPY=X": "USDJPY",
    "AUDUSD=X": "AUDUSD",
    "USDCAD=X": "USDCAD",
    "USDCHF=X": "USDCHF",
    "NZDUSD=X": "NZDUSD",
    "EURGBP=X": "EURGBP",
    "EURJPY=X": "EURJPY",
    "GBPJPY=X": "GBPJPY",
    # ── Metals ──
    "GC=F":   "XAUUSD",   # Gold futures ≈ spot
    "SI=F":   "XAGUSD",   # Silver futures
    # ── Energy ──
    "CL=F":   "USOIL",    # WTI Crude Oil
    # ── Index CFDs ──
    "^GSPC":  "US500",
    "^IXIC":  "NAS100",
    "^DJI":   "US30",
    "^FTSE":  "UK100",
    "^GDAXI": "GER40",
    "^N225":  "JPN225",
}

# Dukascopy instrument name → (canonical symbol, point multiplier for price decoding)
# point: actual_price = raw_uint32 * point
DUKASCOPY_MAP: dict[str, tuple[str, float]] = {
    # ── 5-decimal Forex ──
    "EURUSD": ("EURUSD", 1e-5),
    "GBPUSD": ("GBPUSD", 1e-5),
    "AUDUSD": ("AUDUSD", 1e-5),
    "NZDUSD": ("NZDUSD", 1e-5),
    "USDCAD": ("USDCAD", 1e-5),
    "USDCHF": ("USDCHF", 1e-5),
    "EURGBP": ("EURGBP", 1e-5),
    "EURAUD": ("EURAUD", 1e-5),
    "GBPAUD": ("GBPAUD", 1e-5),
    "EURCAD": ("EURCAD", 1e-5),
    # ── JPY pairs (3-decimal) ──
    "USDJPY": ("USDJPY", 1e-3),
    "EURJPY": ("EURJPY", 1e-3),
    "GBPJPY": ("GBPJPY", 1e-3),
    "AUDJPY": ("AUDJPY", 1e-3),
    # ── Metals ──
    "XAUUSD": ("XAUUSD", 1e-3),   # Gold  ~2150 → raw ~2150000
    "XAGUSD": ("XAGUSD", 1e-3),   # Silver ~27  → raw ~27000
}

# Pandas resample rules
RESAMPLE_RULES: dict[str, str] = {
    "M1":  "1min",
    "M5":  "5min",
    "M15": "15min",
    "H1":  "1h",
    "H4":  "4h",
    "D1":  "1D",
}

# Target data depth per timeframe (days to keep in saved CSV)
TF_DEPTHS: dict[str, int] = {
    "D1":  3 * 365,   # 3 years
    "H4":  3 * 365,   # 3 years
    "H1":  2 * 365,   # 2 years
    "M15": 90,        # 3 months
    "M5":  90,        # 3 months
    "M1":  90,        # 3 months (Dukascopy base for short TFs)
}

# yfinance native intervals and their hard API caps (days)
# H4 is NOT a native yfinance interval → derived by resampling H1
# (marked with resample_from to indicate post-processing)
YF_INTERVALS: list[dict] = [
    {"interval": "1d",  "tf": "D1",  "days": TF_DEPTHS["D1"],  "yf_cap": 3650, "chunk": False},
    {"interval": "1h",  "tf": "H1",  "days": TF_DEPTHS["H1"],  "yf_cap": 729,  "chunk": False},
    {"interval": "1h",  "tf": "H4",  "days": TF_DEPTHS["H4"],  "yf_cap": 729,  "chunk": False,
     "resample_from": "H1"},   # download H1, resample → H4
    {"interval": "15m", "tf": "M15", "days": TF_DEPTHS["M15"], "yf_cap": 59,   "chunk": True},
    {"interval": "5m",  "tf": "M5",  "days": TF_DEPTHS["M5"],  "yf_cap": 59,   "chunk": True},
]

_SESSION = requests.Session()
_SESSION.headers["User-Agent"] = "Mozilla/5.0 TradingDataBot/2.0"


# ══════════════════════════════════════════════════════════════════════════════
# Backend 1 — Yahoo Finance
# ══════════════════════════════════════════════════════════════════════════════

def download_yfinance(
    symbols: list[str],
    timeframes: list[str] | None = None,
) -> None:
    """
    Download historical OHLCV from Yahoo Finance.

    - D1 / H1 : single request, long history
    - H4       : download H1 then resample → no extra request
    - M15 / M5 : chunked (2 × 60-day windows) to reach 90-day target
    """
    tf_set = set(timeframes) if timeframes else None
    end    = datetime.now(tz=timezone.utc)

    sym_to_ticker = {v: k for k, v in YFINANCE_MAP.items()}
    targets = [s for s in symbols if s in sym_to_ticker]
    if not targets:
        print("  No yfinance-supported symbols requested.")
        return

    import yfinance as yf

    # Build job list (skip H4 here — derived after H1)
    jobs = [
        (sym, spec)
        for sym in targets
        for spec in YF_INTERVALS
        if (tf_set is None or spec["tf"] in tf_set)
        and "resample_from" not in spec   # H4 handled separately
    ]

    for sym, spec in tqdm(jobs, desc="yfinance", unit="job"):
        ticker    = sym_to_ticker[sym]
        interval  = spec["interval"]
        tf_label  = spec["tf"]
        target_days = spec["days"]
        yf_cap    = spec["yf_cap"]
        chunked   = spec["chunk"]

        out = DATA_DIR / f"{sym}_{tf_label}.csv"
        if out.exists() and _is_fresh(out, hours=23):
            tqdm.write(f"  ↷  {sym}/{tf_label} up to date, skipping")
            continue

        if chunked:
            df = _yf_fetch_chunked(yf, ticker, interval, target_days, yf_cap, end)
        else:
            days = min(target_days, yf_cap)
            df   = _yf_fetch(yf, ticker, interval, end - timedelta(days=days), end)

        if df.empty:
            tqdm.write(f"  ✗  {sym}/{tf_label}: no data")
            continue

        # Trim to target depth
        cutoff = end - timedelta(days=target_days)
        df = df[df["timestamp"] >= cutoff].reset_index(drop=True)

        _save_csv(df, out)
        tqdm.write(
            f"  ✓  {sym}/{tf_label}: {len(df):,} bars"
            f"  ({df['timestamp'].iloc[0].date()} → {df['timestamp'].iloc[-1].date()})"
        )
        time.sleep(0.4)

        # Derive H4 from H1 (whether just downloaded or loaded from skip)
        _maybe_derive_h4_from_path(sym, out if tf_label == "H1" else DATA_DIR / f"{sym}_H1.csv",
                                   tf_label, tf_set)

    # Final pass: derive H4 for any symbol whose H1 was skipped (already fresh)
    for sym in targets:
        h4_out = DATA_DIR / f"{sym}_H4.csv"
        h1_path = DATA_DIR / f"{sym}_H1.csv"
        if (tf_set is None or "H4" in tf_set) and h1_path.exists() and not (h4_out.exists() and _is_fresh(h4_out, hours=23)):
            _maybe_derive_h4_from_path(sym, h1_path, "H1", tf_set)


def _maybe_derive_h4_from_path(
    sym: str,
    h1_path: Path,
    just_tf: str,
    tf_set: set[str] | None,
) -> None:
    """Resample H1 CSV → H4 and save. Skip if H4 already fresh."""
    if just_tf != "H1":
        return
    if tf_set is not None and "H4" not in tf_set:
        return
    if not h1_path.exists():
        return

    out = DATA_DIR / f"{sym}_H4.csv"
    if out.exists() and _is_fresh(out, hours=23):
        return

    h1 = pd.read_csv(h1_path)
    h1["timestamp"] = pd.to_datetime(h1["timestamp"], format="ISO8601", utc=True)
    h1 = h1.set_index("timestamp")

    h4 = h1.resample("4h").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).dropna(subset=["open"])

    cutoff = pd.Timestamp(datetime.now(tz=timezone.utc) - timedelta(days=TF_DEPTHS["H4"]))
    h4 = h4[h4.index >= cutoff].reset_index()

    _save_csv(h4, out)
    tqdm.write(
        f"  ✓  {sym}/H4:  {len(h4):,} bars"
        f"  ({h4['timestamp'].iloc[0].date()} → {h4['timestamp'].iloc[-1].date()})"
        f"  [resampled ← H1]"
    )


def _yf_fetch_chunked(
    yf,
    ticker: str,
    interval: str,
    target_days: int,
    yf_cap: int,
    end: datetime,
) -> pd.DataFrame:
    """
    Download intraday data in backward chunks, each ≤ yf_cap days.
    Never requests data older than yf_cap days from today (API hard limit).

    Example: target_days=90, yf_cap=59, today=Mar20
      absolute oldest allowed = Mar20 - 59d = Jan20
      chunk 1: Jan20  → Mar20  (59 days)  ← within cap, SUCCEEDS
      Note: yfinance M15/M5 hard cap is 60 days, so effective range = yf_cap
    """
    # yfinance won't serve data older than yf_cap days from NOW regardless of target
    hard_oldest = end - timedelta(days=yf_cap)
    effective_start = max(
        end - timedelta(days=target_days),
        hard_oldest,
    )

    chunks: list[pd.DataFrame] = []
    chunk_end = end

    while chunk_end > effective_start:
        chunk_start = max(chunk_end - timedelta(days=yf_cap), effective_start)
        df = _yf_fetch(yf, ticker, interval, chunk_start, chunk_end)
        if not df.empty:
            chunks.append(df)
        chunk_end = chunk_start - timedelta(hours=1)   # step back with tiny gap
        time.sleep(0.5)
        if chunk_start <= effective_start:
            break

    if not chunks:
        return pd.DataFrame()

    combined = pd.concat(chunks, ignore_index=True)
    combined = combined.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    return combined.reset_index(drop=True)


def _yf_fetch(yf, ticker: str, interval: str, start: datetime, end: datetime) -> pd.DataFrame:
    try:
        df = yf.download(
            ticker,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            interval=interval,
            auto_adjust=True,
            progress=False,
        )
    except Exception as exc:
        tqdm.write(f"  [yfinance error] {ticker}: {exc}")
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.reset_index()

    # Flatten MultiIndex columns (yfinance ≥0.2 wraps ticker in column level)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(str(c) for c in col).strip("_").lower() for col in df.columns]
        # After flattening, pick the correct columns
        col_map = {}
        for c in df.columns:
            for target in ("timestamp", "datetime", "date", "open", "high", "low", "close", "volume"):
                if target in c.lower():
                    col_map[c] = target
                    break
        df = df.rename(columns=col_map)
    else:
        df.columns = [c.lower() for c in df.columns]

    # Normalise timestamp column name
    for cand in ("datetime", "date"):
        if cand in df.columns and "timestamp" not in df.columns:
            df = df.rename(columns={cand: "timestamp"})

    required = {"timestamp", "open", "high", "low", "close"}
    if not required.issubset(df.columns):
        return pd.DataFrame()

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    if "volume" not in df.columns:
        df["volume"] = 0

    df = df[["timestamp", "open", "high", "low", "close", "volume"]].copy()
    df = df.dropna(subset=["open", "high", "low", "close"])
    return df.sort_values("timestamp").reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# Backend 2 — Dukascopy daily M1 candles
# ══════════════════════════════════════════════════════════════════════════════
#
# URL format (month is 0-indexed!):
#   https://datafeed.dukascopy.com/datafeed/{INSTR}/{YEAR}/{MONTH0:02d}/{DAY:02d}/BID_candles_min_1.bi5
#
# bi5 format = LZMA-compressed binary, records of 24 bytes each (big-endian):
#   uint32  time_ms     milliseconds from midnight (start of day)
#   uint32  open_raw    open  × (1/point)
#   uint32  high_raw    high  × (1/point)
#   uint32  low_raw     low   × (1/point)
#   uint32  close_raw   close × (1/point)
#   float32 volume

_DUKA_CANDLE_BASE = "https://datafeed.dukascopy.com/datafeed"
_BAR_STRUCT = struct.Struct(">IIIIIf")   # 24 bytes
_BAR_SIZE   = _BAR_STRUCT.size           # 24


def _duka_candle_url(instrument: str, day: date) -> str:
    return (
        f"{_DUKA_CANDLE_BASE}/{instrument}"
        f"/{day.year}/{day.month - 1:02d}/{day.day:02d}"
        f"/BID_candles_min_1.bi5"
    )


def _decode_day_candles(blob: bytes, point: float, day: date) -> list[dict]:
    """Decompress and decode one day of Dukascopy M1 candles."""
    if not blob:
        return []
    try:
        raw = lzma.decompress(blob)
    except lzma.LZMAError:
        return []

    midnight = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    bars = []
    for off in range(0, len(raw) - _BAR_SIZE + 1, _BAR_SIZE):
        t_ms, o_raw, h_raw, l_raw, c_raw, vol = _BAR_STRUCT.unpack_from(raw, off)
        # Floor to minute — Dukascopy stores bar-open offset in ms, may have
        # sub-second jitter (e.g. t_ms=60 → 00:00:00.060). We only need minute
        # precision for M1 candles.
        ts = midnight + timedelta(minutes=t_ms // 60000)
        bars.append({
            "timestamp": ts,
            "open":   round(o_raw * point, 6),
            "high":   round(h_raw * point, 6),
            "low":    round(l_raw * point, 6),
            "close":  round(c_raw * point, 6),
            "volume": round(vol, 2),
        })
    return bars


def _fetch_day(instrument: str, day: date, retries: int = 3) -> bytes:
    url = _duka_candle_url(instrument, day)
    for attempt in range(retries):
        try:
            r = _SESSION.get(url, timeout=15)
            if r.status_code == 200:
                return r.content
            if r.status_code == 404:
                return b""     # no trading on this day
            time.sleep(0.5)
        except requests.RequestException:
            time.sleep(1 * (attempt + 1))
    return b""


def _trading_days(start: date, end: date) -> Iterator[date]:
    """Yield weekdays between start and end (inclusive)."""
    cur = start
    while cur <= end:
        if cur.weekday() < 5:   # Mon–Fri
            yield cur
        cur += timedelta(days=1)


def download_dukascopy(
    symbols: list[str],
    timeframes: list[str] | None = None,
) -> None:
    """
    Download Dukascopy M1 candles and resample to requested timeframes.

    M1 download depth is determined by the deepest TF requested:
      - If D1 / H4 / H1 requested → 3 years of M1
      - If only M15 / M5 requested → 3 months of M1  (much faster)
    Each saved TF file is trimmed to TF_DEPTHS[tf].
    """
    tf_set  = set(timeframes) if timeframes else None
    end_d   = date.today() - timedelta(days=1)

    # Determine how far back to pull M1 based on requested TFs
    long_tfs = {"D1", "H4", "H1"}
    need_long = (tf_set is None) or bool(tf_set & long_tfs)
    m1_days   = TF_DEPTHS["D1"] if need_long else TF_DEPTHS["M1"]
    start_d   = end_d - timedelta(days=m1_days)

    targets = [s for s in symbols if s in DUKASCOPY_MAP]
    if not targets:
        print("  No Dukascopy-supported symbols requested.")
        return

    for symbol in targets:
        instrument, point = DUKASCOPY_MAP[symbol]
        m1_path = DATA_DIR / f"{symbol}_M1.csv"

        # Resume detection — only re-download missing days
        existing: pd.DataFrame = pd.DataFrame()
        resume_from: date = start_d

        if m1_path.exists():
            existing = pd.read_csv(m1_path)
            existing["timestamp"] = pd.to_datetime(
                existing["timestamp"], format="ISO8601", utc=True
            )
            if not existing.empty:
                last_day   = existing["timestamp"].max().date()
                resume_from = last_day + timedelta(days=1)
                if resume_from > end_d:
                    tqdm.write(f"  ↷  {symbol}/M1 up to date")
                    _duka_resample(symbol, m1_path, tf_set)
                    continue
                tqdm.write(f"  ↻  {symbol}/M1 resuming from {resume_from}")

        days_to_fetch = list(_trading_days(resume_from, end_d))
        all_bars: list[dict] = []

        for day in tqdm(days_to_fetch, desc=f"  {symbol}", unit="day", leave=False):
            blob = _fetch_day(instrument, day)
            all_bars.extend(_decode_day_candles(blob, point, day))
            time.sleep(0.1)

        if not all_bars:
            if not existing.empty:
                tqdm.write(f"  ✓  {symbol}/M1: no new bars (already current)")
                _duka_resample(symbol, m1_path, tf_set)
            else:
                tqdm.write(f"  ✗  {symbol}/M1: no data received from Dukascopy")
            continue

        new_df = pd.DataFrame(all_bars)
        new_df["timestamp"] = pd.to_datetime(new_df["timestamp"], utc=True)

        # Merge with existing and deduplicate
        if not existing.empty:
            df_m1 = pd.concat([existing, new_df], ignore_index=True)
            df_m1 = df_m1.drop_duplicates(subset=["timestamp"])
        else:
            df_m1 = new_df

        df_m1 = df_m1.sort_values("timestamp").reset_index(drop=True)

        # Trim M1 to TF_DEPTHS["M1"] to avoid unbounded growth
        m1_cutoff = pd.Timestamp(
            datetime.now(tz=timezone.utc) - timedelta(days=m1_days)
        )
        df_m1 = df_m1[df_m1["timestamp"] >= m1_cutoff].reset_index(drop=True)

        _save_csv(df_m1, m1_path)
        tqdm.write(
            f"  ✓  {symbol}/M1: {len(df_m1):,} bars"
            f"  ({df_m1['timestamp'].iloc[0].date()} → {df_m1['timestamp'].iloc[-1].date()})"
        )

        _duka_resample(symbol, m1_path, tf_set)


def _duka_resample(
    symbol: str,
    m1_path: Path,
    tf_set: set[str] | None,
) -> None:
    """Read M1 CSV, resample to each timeframe, trim to TF_DEPTHS, and save."""
    df = pd.read_csv(m1_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)
    df = df.set_index("timestamp")
    now = datetime.now(tz=timezone.utc)

    for tf_label, freq in RESAMPLE_RULES.items():
        if tf_label == "M1":
            continue
        if tf_set and tf_label not in tf_set:
            continue

        out    = DATA_DIR / f"{symbol}_{tf_label}.csv"
        depth  = TF_DEPTHS.get(tf_label, 365)
        cutoff = pd.Timestamp(now - timedelta(days=depth))

        rs = df.resample(freq).agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        ).dropna(subset=["open"])

        rs = rs[rs.index >= cutoff].reset_index()
        _save_csv(rs, out)
        tqdm.write(
            f"  ✓  {symbol}/{tf_label}: {len(rs):,} bars"
            f"  ({rs['timestamp'].iloc[0].date()} → {rs['timestamp'].iloc[-1].date()})"
            f"  → {out.name}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Utilities
# ══════════════════════════════════════════════════════════════════════════════

def _save_csv(df: pd.DataFrame, path: Path) -> None:
    cols = ["timestamp", "open", "high", "low", "close", "volume"]
    df[cols].to_csv(path, index=False)


def _is_fresh(path: Path, hours: int = 23) -> bool:
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return (datetime.now(tz=timezone.utc) - mtime).total_seconds() < hours * 3600


def _all_symbols() -> list[str]:
    return sorted(set(YFINANCE_MAP.values()) | set(DUKASCOPY_MAP.keys()))


def print_summary(symbols: list[str], source: str, timeframes: list[str] | None) -> None:
    print("\n" + "═" * 62)
    print("  Historical Data Downloader")
    print("═" * 62)
    print(f"  D1 / H4 / H1 : up to 3 years")
    print(f"  M15 / M5     : 3 months (90 days)")
    print(f"  Symbols : {', '.join(symbols)}")
    print(f"  Source  : {source}")
    if timeframes:
        print(f"  TFs     : {', '.join(timeframes)}")
    print(f"  Output  : {DATA_DIR}")
    print("═" * 62 + "\n")


def print_final_summary() -> None:
    files = sorted(DATA_DIR.glob("*.csv"))
    if not files:
        print("  No files downloaded.")
        return
    print(f"\n{'═'*62}")
    print(f"  Download complete — {len(files)} files in {DATA_DIR.name}/")
    print(f"{'─'*62}")
    for f in sorted(files):
        try:
            n = sum(1 for _ in open(f)) - 1
            kb = f.stat().st_size / 1024
            print(f"  {f.name:<38} {n:>8,} bars  {kb:>7.1f} KB")
        except Exception:
            pass
    print(f"{'═'*62}\n")


# ══════════════════════════════════════════════════════════════════════════════
# CLI Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download Forex/CFD OHLCV data for backtesting "
                    "(D1/H4/H1: 3yr | M15/M5: 3 months)"
    )
    p.add_argument("--symbols",    nargs="+", metavar="SYM",
                   help="Symbols to download (default: all). E.g. XAUUSD EURUSD US500")
    p.add_argument("--source",     choices=["all", "yfinance", "dukascopy"],
                   default="all",
                   help="Data backend (default: all).")
    p.add_argument("--timeframes", nargs="+", metavar="TF",
                   help="Timeframes to generate (default: all). E.g. D1 H4 H1 M15 M5")
    p.add_argument("--list",       action="store_true",
                   help="Print available symbols and exit.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.list:
        print("\nAvailable symbols:")
        print(f"  [yfinance]   : {', '.join(sorted(YFINANCE_MAP.values()))}")
        print(f"  [dukascopy]  : {', '.join(sorted(DUKASCOPY_MAP.keys()))}")
        print(f"\nTimeframes and data depth:")
        for tf, days in TF_DEPTHS.items():
            label = f"{days//365}yr" if days >= 365 else f"{days}d"
            print(f"  {tf:>4}  →  {label}")
        return

    symbols = args.symbols or _all_symbols()
    print_summary(symbols, args.source, args.timeframes)

    if args.source in ("all", "yfinance"):
        print("── Yahoo Finance ─────────────────────────────────────────")
        download_yfinance(symbols, timeframes=args.timeframes)

    if args.source in ("all", "dukascopy"):
        print("\n── Dukascopy M1 candles (Forex + Metals) ─────────────────")
        download_dukascopy(symbols, timeframes=args.timeframes)

    print_final_summary()


if __name__ == "__main__":
    main()
