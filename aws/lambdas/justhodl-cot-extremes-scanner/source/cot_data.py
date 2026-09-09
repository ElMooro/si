"""Typed current-vintage CFTC positioning; no invented zeros or mixed reports."""
import math
import re
from datetime import date, timedelta

FIELDS = {
    'tff': ('asset_mgr_positions_long', 'asset_mgr_positions_short',
            'lev_money_positions_long', 'lev_money_positions_short'),
    'disagg': ('m_money_positions_long_all', 'm_money_positions_short_all'),
    'legacy': ('noncomm_positions_long_all', 'noncomm_positions_short_all'),
}
POSITIONING_BASIS = {
    'tff': 'Asset manager plus leveraged fund net positions / open interest',
    'disagg': 'Managed money net positions / open interest',
    'legacy': 'Noncommercial net positions / open interest',
}


def integer(value):
    if isinstance(value, bool) or value is None:
        return None
    text = str(value).replace(',', '').strip()
    if not re.fullmatch(r'[+-]?\d+(?:\.0+)?', text):
        return None
    number = int(text.split('.')[0])
    return number if abs(number) <= 2**53 - 1 else None


def parse_rows(records, report_type, code, today=None):
    today = today or date.today()
    if not isinstance(records, list) or report_type not in FIELDS:
        raise ValueError('PROVIDER_SCHEMA_INVALID')
    rows, dates = [], set()
    for record in records:
        if not isinstance(record, dict) or record.get('cftc_contract_market_code') != code:
            raise ValueError('PROVIDER_CONTRACT_MISMATCH')
        stamp = str(record.get('report_date_as_yyyy_mm_dd', ''))[:10]
        try:
            day = date.fromisoformat(stamp)
        except ValueError:
            raise ValueError('PROVIDER_DATE_INVALID') from None
        if day > today or stamp in dates:
            raise ValueError('PROVIDER_DATE_INVALID')
        dates.add(stamp)
        positions = {field: integer(record.get(field)) for field in FIELDS[report_type]}
        oi = integer(record.get('open_interest_all'))
        valid = oi is not None and oi > 0 and all(value is not None and value >= 0 for value in positions.values())
        net = sum(value if i % 2 == 0 else -value for i, value in enumerate(positions.values())) if valid else None
        valid = valid and abs(net) <= oi and all(value <= oi for value in positions.values())
        rows.append({'date': stamp, 'spec_net': net if valid else None, 'open_int': oi,
                     'ratio': net / oi if valid else None, 'positions': positions,
                     'status': 'VALID' if valid else 'INVALID_POSITION_OR_OPEN_INTEREST'})
    return sorted(rows, key=lambda row: row['date'])


def percentile_rank(values, value):
    if not values or any(type(v) not in (float, int) or not math.isfinite(v) for v in [*values, value]):
        return None
    return round(100 * (sum(v < value for v in values) + .5 * sum(v == value for v in values)) / len(values), 1)


def summarize(contract, info, document, today):
    history = document.get('history', []) if isinstance(document, dict) else []
    report_type = document.get('report_type') if isinstance(document, dict) else None
    row = {'contract': contract, 'name': info['name'], 'category': info['category'],
           'report_type': report_type, 'positioning_basis': POSITIONING_BASIS.get(report_type),
           'status': 'provider_unavailable', 'n_weeks_history': len(history),
           'percentile': None, 'extreme': None, 'trend_4w': None, 'trend_baseline_date': None,
           'execution_eligible': False, 'calibration_status': 'DESCRIPTIVE_UNCALIBRATED',
           'history_key': 'cot/history/' + contract + '.json'}
    if not history:
        return row
    current = history[-1]
    row.update(report_date=current['date'], current_ratio=current['ratio'], spec_net=current['spec_net'], open_int=current['open_int'])
    row['report_age_days'] = (today - date.fromisoformat(current['date'])).days
    prior = history[:-1]
    if document.get('refresh_status') != 'FETCHED':
        row['status'] = 'refresh_failed'
    elif current.get('status') != 'VALID':
        row['status'] = 'invalid_current_observation'
    elif not 0 <= row['report_age_days'] <= 10:
        row['status'] = 'stale_report'
    elif any(item.get('status') != 'VALID' for item in prior):
        row['status'] = 'invalid_history'
    elif len(prior) < 26:
        row['status'] = 'insufficient_history'
    else:
        row['status'] = 'ok'
        pct = percentile_rank([item['ratio'] for item in prior], current['ratio'])
        row['percentile'] = pct
        row['extreme'] = 'high' if pct >= 95 else 'low' if pct <= 5 else None
        cutoff = date.fromisoformat(current['date']) - timedelta(weeks=4)
        baseline = next((item for item in reversed(prior) if date.fromisoformat(item['date']) <= cutoff), None)
        if baseline and (cutoff - date.fromisoformat(baseline['date'])).days <= 7:
            row['trend_4w'] = round(current['ratio'] - baseline['ratio'], 5)
            row['trend_baseline_date'] = baseline['date']
    row['n_prior_observations'] = len(prior)
    row['percentile_basis'] = 'Midrank against valid prior observations; current observation excluded'
    return row
