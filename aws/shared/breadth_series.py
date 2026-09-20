"""Numeric-only compatibility for native breadth research, never voting authority.

Older study readers cannot represent gaps. Return only the contiguous valid suffix
ending at the packet's requested current session; never compress interior gaps.
The full packet remains the source for dates, units, populations and missing rows.
"""
from datetime import datetime, timezone, date
import hashlib
import json
import math


def native_suffix(packet, metric, start, at=None):
    try:
        at = at or datetime.now(timezone.utc)
        if packet.get('contract') != 'breadth-native-research.v1': return {}
        generated = datetime.fromisoformat(packet['generated_at'].replace('Z', '+00:00'))
        if generated.tzinfo is None or not 0 <= (at-generated).total_seconds() <= 96*3600: return {}
        # A weekend may have no new completed market session. This adapter's
        # bounded publication age is a research SLA, not investment eligibility.
        if not 0 <= (at.date()-date.fromisoformat(packet['as_of'])).days <= 5: return {}
        raw = json.dumps({k: v for k, v in packet.items() if k != 'replay'}, sort_keys=True,
            separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
        if hashlib.sha256(raw).hexdigest() != packet.get('replay', {}).get('output_sha256'): return {}
        days = packet['calendar']['requested_sessions']; series = packet['series'][metric]
        if not days or days != sorted(set(days)) or days[-1] != packet['as_of'] or set(series) != set(days): return {}
        suffix = {}
        for day in reversed(days):
            value = series[day]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value): break
            if day >= start: suffix[day] = float(value)
        return dict(sorted(suffix.items()))
    except (KeyError, ValueError, TypeError, OverflowError):
        return {}
