"""Descriptive alert evidence from the dedicated COT v2 publisher."""
from datetime import date, datetime, timezone
import math
import re


def _number(value):
    return value if type(value) in (int, float) and math.isfinite(value) else None


def extreme_rows(document, now=None):
    now = now or datetime.now(timezone.utc)
    if (not isinstance(document, dict)
            or document.get('engine') != 'justhodl-cot-extremes-scanner'
            or document.get('schema_version') != 'cot-extremes.v2'
            or document.get('execution_eligible') is not False):
        return []
    try:
        stamp = datetime.fromisoformat(document['generated_at'].replace('Z', '+00:00'))
        if stamp.tzinfo is None or not 0 <= (now - stamp).total_seconds() <= 8 * 86400:
            return []
    except (KeyError, TypeError, ValueError, AttributeError):
        return []
    contracts = document.get('contracts')
    if not isinstance(contracts, list):
        return []
    result, seen = [], set()
    for row in contracts:
        if not isinstance(row, dict):
            continue
        contract = row.get('contract')
        if not isinstance(contract, str) or not re.fullmatch(r'[A-Za-z0-9._-]{1,32}', contract):
            continue
        if contract in seen:
            return []
        seen.add(contract)
        pct, prior = _number(row.get('percentile')), _number(row.get('n_prior_observations'))
        if (row.get('status') != 'ok' or row.get('execution_eligible') is not False
                or row.get('report_type') not in ('tff', 'disagg', 'legacy')
                or pct is None or not 0 <= pct <= 100
                or prior is None or prior < 26 or prior != int(prior)):
            continue
        try:
            report = date.fromisoformat(row.get('report_date'))
        except (TypeError, ValueError):
            continue
        if not 0 <= (now.date() - report).days <= 10 or report > stamp.astimezone(timezone.utc).date():
            continue
        side = 'high' if pct >= 95 else 'low' if pct <= 5 else None
        if side is None or row.get('extreme') != side:
            continue
        result.append({'contract': contract, 'report_date': report.isoformat(),
                       'report_type': row['report_type'], 'percentile': pct,
                       'n_prior_observations': int(prior), 'side': side})
    return sorted(result, key=lambda row: (min(row['percentile'], 100 - row['percentile']), row['contract']))
