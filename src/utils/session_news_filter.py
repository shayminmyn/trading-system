"""
Session & news blackout filter for entry timing.

Blocks new entries when:
  • Calendar Monday — first N hours (default UTC Mon 00:00–02:00).
  • Friday — last N hours before configurable weekly FX close (default Fri 16:00–21:00 UTC).
  • High-impact news — ±margin hours around each event time (CSV, UTC).

Historical bars use UTC timestamps (see tz_utils). All filter times are in UTC unless
you set monday/friday to use a different zone (advanced — default UTC).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

_UTC = timezone.utc

# Cache: path -> (mtime_ns, list of event datetimes UTC)
_news_cache: dict[str, tuple[int | None, list[datetime]]] = {}


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def _to_utc(ts: Any) -> datetime | None:
    """Normalize bar timestamp to aware UTC datetime."""
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return None
    if isinstance(ts, pd.Timestamp):
        ts = ts.to_pydatetime()
    if isinstance(ts, str):
        ts = pd.Timestamp(ts).to_pydatetime()
    if not isinstance(ts, datetime):
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=_UTC)
    return ts.astimezone(_UTC)


def _load_news_datetimes(path_str: str | None) -> list[datetime]:
    """Load HIGH-impact event times from CSV (column datetime_utc)."""
    if not path_str or not str(path_str).strip():
        return []

    path = Path(path_str)
    if not path.is_absolute():
        path = _project_root() / path

    if not path.is_file():
        logger.warning("session_filters: news file not found: %s — skipping news blackout", path)
        return []

    try:
        mtime = path.stat().st_mtime_ns
    except OSError:
        mtime = None

    key = str(path.resolve())
    if key in _news_cache and _news_cache[key][0] == mtime:
        return _news_cache[key][1]

    df = pd.read_csv(path)
    if df.empty:
        _news_cache[key] = (mtime, [])
        return []

    col = None
    for c in ("datetime_utc", "datetime", "time_utc", "event_time"):
        if c in df.columns:
            col = c
            break
    if col is None:
        logger.warning("session_filters: no datetime column in %s — expected datetime_utc", path)
        _news_cache[key] = (mtime, [])
        return []

    impact_col = "impact" if "impact" in df.columns else None
    out: list[datetime] = []
    for _, row in df.iterrows():
        if impact_col and str(row.get(impact_col, "HIGH")).upper() not in (
            "HIGH", "H", "RED", "3",
        ):
            # Skip non-HIGH if impact column exists
            continue
        try:
            t = pd.Timestamp(row[col])
            if t.tzinfo is None:
                t = t.tz_localize("UTC")
            else:
                t = t.tz_convert("UTC")
            out.append(t.to_pydatetime())
        except Exception:
            continue

    _news_cache[key] = (mtime, out)
    logger.info("session_filters: loaded %d news events from %s", len(out), path.name)
    return out


def is_friday_week_close_blackout(
    bar_timestamp: Any, session_filters: dict[str, Any] | None
) -> bool:
    """
    True if the bar falls in the Friday "last N hours before weekly close" window.

    Uses the same parameters as session_filters (UTC):
      friday_avoid_hours_before_week_close, friday_week_close_hour_utc,
      friday_week_close_minute_utc.

    Does **not** check session_filters.enabled — callers use this for backtest
    limit cancellation even when entry blocking is configured separately.
    """
    if not session_filters:
        return False
    avoid_h = int(session_filters.get("friday_avoid_hours_before_week_close", 0) or 0)
    if avoid_h <= 0:
        return False

    dt = _to_utc(bar_timestamp)
    if dt is None or dt.weekday() != 4:  # Friday
        return False

    close_h = int(session_filters.get("friday_week_close_hour_utc", 21))
    close_m = int(session_filters.get("friday_week_close_minute_utc", 0))
    close_sec = close_h * 3600 + close_m * 60
    start_sec = close_sec - avoid_h * 3600
    bar_sec = dt.hour * 3600 + dt.minute * 60 + dt.second
    return start_sec <= bar_sec < close_sec


def is_entry_allowed(bar_timestamp: Any, session_filters: dict[str, Any]) -> bool:
    """
    Return False if entries should be blocked at this bar's open/close time.

    `session_filters` is the YAML dict (e.g. cfg['session_filters']).
    If missing or enabled=False → allow all.
    """
    if not session_filters or not session_filters.get("enabled", False):
        return True

    dt = _to_utc(bar_timestamp)
    if dt is None:
        return True

    # ── Monday: first N hours (calendar Monday, UTC) ─────────────────────
    mon_h = int(session_filters.get("monday_avoid_first_hours", 0) or 0)
    if mon_h > 0:
        if dt.weekday() == 0 and dt.hour < mon_h:
            return False

    # ── Friday: last N hours before weekly close ───────────────────────────
    if is_friday_week_close_blackout(dt, session_filters):
        return False

    # ── News: ± margin around each HIGH event ──────────────────────────────
    margin_h = float(session_filters.get("news_margin_hours", 1.0) or 0.0)
    if margin_h > 0:
        fpath = session_filters.get("news_events_file", "")
        events = _load_news_datetimes(fpath if fpath else None)
        delta = timedelta(hours=margin_h)
        for ev in events:
            if ev - delta <= dt <= ev + delta:
                return False

    return True


def merge_session_filters_into_params(
    strategy_params: dict[str, Any],
    global_session: dict[str, Any] | None,
) -> dict[str, Any]:
    """Attach global session_filters to strategy parameters dict."""
    out = dict(strategy_params)
    if global_session is not None:
        out["session_filters"] = global_session
    return out
