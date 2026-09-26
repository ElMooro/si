"""Complete per-source FI/FX measurements. Pure, deterministic and research-only."""
from bisect import bisect_left, bisect_right, insort
from collections import deque
from copy import deepcopy
from datetime import date
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
from fractions import Fraction
import hashlib, json, math
import fifx_catalog as catalog
import fifx_originals as originals
import fifx_timezones as timezones

CONTRACT = 'fifx-source-candidate.v1'


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def scalar(value):
    display = float(value) if value is not None else None
    if display is not None and not math.isfinite(display):
        raise ValueError('Nonfinite display statistic')
    return {'value': display, 'calculated_decimal': str(value) if value is not None else None}


def dec(value):
    return Decimal(value.numerator) / Decimal(value.denominator)


def variance(values):
    values = [Fraction(v) for v in values]
    mean = sum(values) / len(values)
    return sum((v - mean) ** 2 for v in values) / (len(values) - 1)


def estimates(rows, spec, cutoff):
    numeric = [r for r in rows if r['date'] <= cutoff and originals.decimal(r['value']) is not None]
    n = spec['window_changes']
    if not n:
        return [{'date': r['date'], 'original_row': r['original_row'], 'estimate': scalar(originals.decimal(r['value'])),
                 'valid_intervals': originals.decimal(r['value']) >= 0} for r in numeric]
    changes = []
    for first, last in zip(numeric, numeric[1:]):
        a, b = originals.decimal(first['value']), originals.decimal(last['value'])
        # ln(b) - ln(a) avoids silently rounding b/a first.
        value = (b - a) * 100 if spec['measurement'] == 'yield_change_dispersion' else (b.ln() - a.ln()) * 100 if a > 0 and b > 0 else None
        changes.append(value)
    out = []
    for end in range(n, len(numeric)):
        window = numeric[end-n:end+1]
        first, last = window[0], window[-1]
        gaps = [(date.fromisoformat(b['date']) - date.fromisoformat(a['date'])).days for a, b in zip(window, window[1:])]
        deltas = changes[end-n:end]
        var = dec(variance(deltas)) if all(v is not None for v in deltas) else None
        out.append({'date': last['date'], 'original_row': last['original_row'], 'start_date': first['date'],
            'start_original_row': first['original_row'], 'numeric_observations': n+1, 'change_count': n,
            'elapsed_calendar_days': (date.fromisoformat(last['date']) - date.fromisoformat(first['date'])).days,
            'missing_rows_inside_window': last['original_row']-first['original_row']-n,
            'max_interval_days': max(gaps), 'invalid_log_changes': sum(v is None for v in deltas),
            'valid_intervals': max(gaps) <= catalog.MAX_GAP_DAYS and var is not None,
            'sample_variance': scalar(var), 'step_dispersion': scalar(var.sqrt() if var is not None else None),
            'estimate': scalar((var*catalog.ANNUAL_STEPS).sqrt() if var is not None else None)})
    return out


def rank_history(history):
    """Exact sums of rounded 72-digit estimates, prior-only 504-window baseline."""
    prior = deque()
    ordered = []
    total = squares = Fraction(0)
    invalid = 0
    for row in history:
        value = row['estimate']['calculated_decimal']
        current = Fraction(value) if value is not None else None
        valid = row['valid_intervals'] and current is not None
        state = 'insufficient_prior_windows' if len(prior) != catalog.BASELINE else 'invalid_prior_or_current' if invalid or not valid else 'available'
        mean = sd = z = percentile = None
        if state == 'available':
            n = len(prior)
            mean = dec(total/n)
            sd = dec((squares-total*total/n)/(n-1)).sqrt()
            z = (dec(current)-mean)/sd if sd else None
            percentile = Decimal(100)*(Decimal(bisect_left(ordered, current))+Decimal(bisect_right(ordered, current)-bisect_left(ordered, current))/2)/n
            if not sd:
                state = 'flat_baseline'
        row['baseline'] = {'status': state, 'prior_count': len(prior), 'invalid_prior_count': invalid,
            'first_prior_date': prior[0][0] if prior else None, 'last_prior_date': prior[-1][0] if prior else None,
            'mean': scalar(mean), 'sample_sd': scalar(sd), 'z_score': scalar(z), 'midrank_percentile': scalar(percentile)}
        prior.append((row['date'], current, valid))
        if valid:
            total += current; squares += current*current; insort(ordered, current)
        else:
            invalid += 1
        if len(prior) > catalog.BASELINE:
            _, old, was_valid = prior.popleft()
            if was_valid:
                total -= old; squares -= old*old; ordered.pop(bisect_left(ordered, old))
            else:
                invalid -= 1


def build_source(sid, raw, receipt, generated_at, definition=None):
    if sid not in catalog.SOURCES:
        raise ValueError('Unreviewed source identity')
    spec = deepcopy(catalog.SPECS[sid])
    out = {'contract': CONTRACT, 'candidate_only': True, 'source_id': sid, 'generated_at': generated_at,
        'specification': spec, 'receipt': deepcopy(receipt), 'original_sha256': hashlib.sha256(raw).hexdigest() if raw is not None else None,
        'original_bytes': len(raw) if raw is not None else None, 'original_rows': [], 'history': [],
        'source_identity': None, 'latest_reported': None, 'current': None, 'last_calculated': None,
        'quality': {'status': 'unavailable'}, 'methodology': deepcopy(catalog.METHOD),
        'independent_votes': 0, **catalog.AUTHORITY}
    if raw is None:
        if receipt is not None:
            raise ValueError('Missing original cannot have a receipt')
        return out
    now, acquired = originals.receipt_check(sid, raw, receipt, generated_at)
    if receipt['http_status'] != 200:
        out['quality']['status'] = 'http_error'
        return out
    try:
        rows, identity = originals.parse_csv(raw, sid, definition) if sid in catalog.FRED else originals.parse_quote(raw, sid)
    except (ValueError, KeyError, TypeError, IndexError, UnicodeError, ArithmeticError) as exc:
        out['quality'].update(status='invalid_original_schema', error_type=type(exc).__name__)
        return out
    zone = timezones.zone(identity['timezone']['name']) if identity['timezone'] else None
    cutoff = str(acquired.astimezone(zone).date() if zone else acquired.date())
    latest = rows[-1]
    age = ((now.astimezone(zone).date() if zone else now.date()) - date.fromisoformat(latest['date'])).days
    eligible = [r for r in rows if r['date'] <= cutoff]
    with localcontext() as arithmetic:
        arithmetic.prec = 72; arithmetic.rounding = ROUND_HALF_EVEN
        history = estimates(rows, spec, cutoff) if identity['identity_reviewed'] else []
        rank_history(history)
    last = history[-1] if history else None
    state = ('identity_mismatch' if not identity['identity_reviewed'] else
        'future_original_rows' if len(eligible) != len(rows) else
        'provisional_session' if zone and latest['date'] >= cutoff else
        'missing_latest_value' if originals.decimal(latest['value']) is None else
        'stale_acquisition' if (now-acquired).total_seconds() > catalog.MAX_ACQUISITION_SECONDS else
        'stale_observation' if not 0 <= age <= spec['max_observation_age_days'] else
        'insufficient_history' if last is None else
        'invalid_current_window' if not last['valid_intervals'] or last['date'] != latest['date'] else 'within_age_ceiling')
    out.update(original_rows=rows, history=history, source_identity=identity, latest_reported=deepcopy(latest),
        last_calculated=deepcopy(last), current=deepcopy(last) if state == 'within_age_ceiling' else None,
        quality={'status': state, 'observation_age_days': age, 'acquisition_age_seconds': (now-acquired).total_seconds(),
                 'acquisition_cutoff_date': cutoff, 'excluded_future_rows': len(rows)-len(eligible),
                 'max_observation_age_days': spec['max_observation_age_days'],
                 'max_acquisition_age_seconds': catalog.MAX_ACQUISITION_SECONDS, 'release_calendar_verified': False})
    return out
