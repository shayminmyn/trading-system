"""Timezone utilities — display helpers for UTC+7 (Vietnam time)."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

_VN_TZ = timezone(timedelta(hours=7))
_DEFAULT_FMT = "%Y-%m-%d %H:%M"


def to_vn(ts: datetime | str | None) -> datetime | None:
    """Convert a timestamp to UTC+7. Accepts datetime, ISO string, or None."""
    if ts is None or ts == "":
        return None
    if isinstance(ts, str):
        ts = ts.strip()
        if not ts:
            return None
        try:
            dt = datetime.fromisoformat(ts)
        except ValueError:
            return None
    else:
        dt = ts

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_VN_TZ)


def fmt_ts(ts: datetime | str | None, fmt: str = _DEFAULT_FMT) -> str:
    """Format a timestamp as UTC+7 string, or empty string if None/invalid."""
    dt = to_vn(ts)
    return dt.strftime(fmt) if dt is not None else ""
