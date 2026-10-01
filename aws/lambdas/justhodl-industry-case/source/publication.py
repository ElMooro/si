"""Industry-case source qualification; no I/O or invented freshness SLA."""
from copy import deepcopy
import math
from tape_truth_qualification import clock

CONTRACT = 'industry-case-publication.v1'
SOURCE_KEYS = ('data/universe.json', 'data/industry-boom.json', 'data/earnings.json',
               'data/tape-truth.json', 'spx-beaters/weekly-closes.json')


def number(value):
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except (OverflowError, TypeError, ValueError):
        return False


def text(value):
    return value if isinstance(value, str) else None


def retained(value):
    # JSON's non-standard NaN/Infinity extensions are invalid clocks, never serialized as JSON numbers.
    if isinstance(value, float) and not math.isfinite(value):
        return {"invalid_number": repr(value)}
    if isinstance(value, list):
        return [retained(v) for v in value]
    if isinstance(value, dict):
        return {k: retained(v) for k, v in value.items()}
    return value


def source(packet, key, now):
    obj = packet if isinstance(packet, dict) else {}
    clocks = {k: clock(obj.get(k), 'timestamp' if k == 'generated_at' or
                       (isinstance(obj.get(k), str) and 'T' in obj[k]) else 'date', now)
              for k in ('generated_at', 'as_of')}
    # Preserve the ledger's original observation-date sequence; never infer its age from output time.
    dates = obj.get('dates')
    if key == SOURCE_KEYS[-1]:
        clocks['dates'] = {'values': deepcopy(dates), 'status': 'MISSING' if dates is None else 'INVALID'}
        if isinstance(dates, list):
            statuses = [clock(v, 'date', now)['status'] for v in dates]
            clocks['dates'].update(status='VALID' if dates and all(v == 'VALID' for v in statuses) else 'INVALID',
                                   entry_statuses=statuses)
    return {'source_key': key, 'availability': 'UNAVAILABLE' if packet is None else
            'INVALID' if not isinstance(packet, dict) else 'AVAILABLE',
            'source_status': text(obj.get('status')), 'clocks': retained(clocks),
            'freshness': 'UNKNOWN', 'freshness_reason': 'NO_AUTHORITATIVE_SOURCE_SLA',
            'discarded_rows': 0, 'invalid_values': 0}


def prepare(packets, now):
    """Reject invalid structure/types without changing arithmetic on measured finite values."""
    meta = {key: source(packets.get(key), key, now) for key in SOURCE_KEYS}
    clean = {k: deepcopy(v) if isinstance(v, dict) else {} for k, v in packets.items()}

    def rows(key, field, validator):
        obj = clean[key]
        original = obj.get(field)
        if not isinstance(original, list):
            meta[key]['availability'] = 'INVALID' if meta[key]['availability'] == 'INVALID' or original is not None else 'UNAVAILABLE'
            return []
        accepted = [r for r in original if isinstance(r, dict) and validator(r)]
        meta[key]['discarded_rows'] += len(original)-len(accepted)
        if len(accepted) != len(original):
            meta[key]['availability'] = 'PARTIAL'
        elif not accepted:
            meta[key]['availability'] = 'EMPTY'
        return accepted

    uni = clean[SOURCE_KEYS[0]]
    uni['stocks'] = rows(SOURCE_KEYS[0], 'stocks', lambda r: bool(text(r.get('symbol'))) and
                         bool(text(r.get('industry'))) and number(r.get('market_cap')) and r['market_cap'] >= 0)
    for r in uni['stocks']:
        for k in ('name', 'sector', 'cap_bucket'):
            r[k] = text(r.get(k))
    boom = clean[SOURCE_KEYS[1]]
    boom['league'] = rows(SOURCE_KEYS[1], 'league', lambda r: bool(text(r.get('industry'))))
    for r in boom['league']:
        r['comp'] = r.get('comp') if isinstance(r.get('comp'), dict) else {}
        for obj, fields in ((r, ('boom_score', 'n')), (r['comp'], ('inst_net_bps', 'insider_buys_30d'))):
            for k in fields:
                if obj.get(k) is not None and not number(obj[k]):
                    meta[SOURCE_KEYS[1]]['invalid_values'] += 1
                    obj[k] = None
    earn = clean[SOURCE_KEYS[2]]
    earn['beat_league'] = rows(SOURCE_KEYS[2], 'beat_league', lambda r: bool(text(r.get('t'))))
    growth = earn.get('growth_calls')
    if growth is not None and not isinstance(growth, dict):
        meta[SOURCE_KEYS[2]]['invalid_values'] += 1
    picks = growth.get('picks') if isinstance(growth, dict) else None
    if picks is not None and not isinstance(picks, list):
        meta[SOURCE_KEYS[2]]['invalid_values'] += 1
    accepted = [p for p in picks or [] if isinstance(p, dict) and text(p.get('t'))] if isinstance(picks, list) else []
    if isinstance(picks, list):
        meta[SOURCE_KEYS[2]]['discarded_rows'] += len(picks)-len(accepted)
    earn['growth_calls'] = {'picks': accepted}
    for r in earn['beat_league'] + accepted:
        for k in ('rank', 'beat_score', 'eps_surprise_pct', 'pick_score'):
            if r.get(k) is not None and not number(r[k]):
                meta[SOURCE_KEYS[2]]['invalid_values'] += 1
                r[k] = None
    tape = clean[SOURCE_KEYS[3]]
    if not isinstance(tape.get('symbols'), dict):
        meta[SOURCE_KEYS[3]]['availability'] = 'INVALID' if meta[SOURCE_KEYS[3]]['availability'] == 'INVALID' or tape.get('symbols') is not None else 'UNAVAILABLE'
        tape['symbols'] = {}
    led = clean[SOURCE_KEYS[4]]
    closes = led.get('closes')
    if not isinstance(closes, dict):
        meta[SOURCE_KEYS[4]]['availability'] = 'INVALID' if meta[SOURCE_KEYS[4]]['availability'] == 'INVALID' or closes is not None else 'UNAVAILABLE'
        closes = {}
    valid = {}
    for ticker, values in closes.items():
        if not isinstance(values, list):
            meta[SOURCE_KEYS[4]]['invalid_values'] += 1
            continue
        valid[ticker] = [v if number(v) else None for v in values]
        meta[SOURCE_KEYS[4]]['invalid_values'] += sum(v is not None and not number(v) for v in values)
    led['closes'] = valid
    for item in meta.values():
        if (item['discarded_rows'] or item['invalid_values']) and item['availability'] == 'AVAILABLE':
            item['availability'] = 'PARTIAL'
    return clean, meta
