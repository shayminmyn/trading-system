"""
Timezone display helpers.

Historical data is stored in UTC+0 (confirmed: yfinance / Dukascopy
both return timestamps with +00:00 suffix).

VN_TZ = UTC+7 is used for all display purposes (reports, console logs).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

VN_TZ = timezone(timedelta(hours=7))
_UTC  = timezone.utc


def to_vn_time(ts) -> datetime:
    """
    Convert any timestamp-like value to a timezone-aware datetime in UTC+7.

    Accepts:
      - datetime (with or without tzinfo)
      - pandas Timestamp
      - str  (ISO format, e.g. "2024-01-01 08:00:00+00:00")
      - numeric (Unix epoch seconds)
    """
    import pandas as pd

    if isinstance(ts, str):
        try:
            ts = pd.Timestamp(ts)
        except Exception:
            return datetime.now(tz=VN_TZ)      # fallback

    if isinstance(ts, pd.Timestamp):
        ts = ts.to_pydatetime()

    if isinstance(ts, (int, float)):
        ts = datetime.fromtimestamp(ts, tz=_UTC)

    if not isinstance(ts, datetime):
        return datetime.now(tz=VN_TZ)

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=_UTC)            # assume UTC when naive

    return ts.astimezone(VN_TZ)


def fmt_ts(ts, fmt: str = "%Y-%m-%d %H:%M") -> str:
    """
    Format a timestamp as UTC+7 string.

    Default format "%Y-%m-%d %H:%M" omits seconds for compact display.
    Use fmt="%Y-%m-%d %H:%M:%S" if seconds are needed.
    """
    try:
        return to_vn_time(ts).strftime(fmt)
    except Exception:
        return str(ts)[:19]
