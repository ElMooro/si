"""Numeric-only compatibility for native breadth research, never voting authority.

Older study readers cannot represent gaps. Return only the contiguous valid suffix
ending at the packet's requested current session; never compress interior gaps.
The full packet remains the source for dates, units, populations and missing rows.
"""
from datetime import datetime, timezone, date
import hashlib
import json
import math
import re


def unverified_breadth_mapping(symbol, entry):
    entry = entry if isinstance(entry, dict) else {}
    source = str(entry.get('source', '')).upper()
    identity = str(entry.get('id', entry.get('source_id', ''))).upper()
    symbol = str(symbol).upper()
    return (symbol.startswith('USI:') or source == 'INTERNALS' or
        bool(re.search(r'(?:^|~)INTERNALS(?:~|$)', identity)) or
        source == 'FORMULA' and 'USI:' in identity or
        source == 'BREADTH_NATIVE' and symbol != 'JH_BREADTH:'+identity)


def filter_mappings(mappings):
    """Withhold cached aliases and formulas depending on them; keep input untouched."""
    accepted = {key: entry for key, entry in mappings.items() if isinstance(entry, dict) and not unverified_breadth_mapping(key, entry)}
    while True:
        remove = []
        for key, entry in accepted.items():
            if entry.get('source') != 'FORMULA': continue
            operands = [s.strip().upper() for s in re.split(r'[+\-*/()]', str(entry.get('id', '')))]
            if any(symbol in mappings and symbol not in accepted for symbol in operands): remove.append(key)
        if not remove: break
        for key in remove: accepted.pop(key)
    return accepted, set(mappings)-set(accepted)


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
