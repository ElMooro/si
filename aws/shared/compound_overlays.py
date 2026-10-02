"""Descriptive Compound overlays with explicit missingness and calculation inputs.

Family weights and lifecycle decay are declared heuristics, not learned alpha.
Neither a score percentile nor a reversal label grants forecast permission.
"""
from collections import Counter
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import math
import re

from compound_numeric import number, symbol, InvalidNumber
from context_evidence_store import strict, MAX_BYTES

CONTRACT = 'compound-overlays.v1'
CONTEXT_KEYS = ('data/trend-reversal.json', 'data/compound-firstseen.json',
                'data/risk-gate.json', 'data/compound-history.json')
FAMILIES = {
    'flow': ('optionsflow', 'smartmoney', 'fundsbuying', '13f', 'insider', 'congress', 'darkpool', 'whale'),
    'fundamental': ('revaccel', 'epsvelocity', 'pead', 'nobrainer', 'deepvalue', 'magicformula', 'earnings', 'guidance'),
    'technical': ('momentum', 'volsqueeze', 'prepump', 'breakout', 'squeeze', 'trend', 'gap'),
}
PRIORS = {'flow': 1.35, 'fundamental': 1.15, 'technical': 1.0}
HALF_LIFE = {'flow': 5.0, 'fundamental': 30.0, 'technical': 4.0}


def read_context(client, bucket, key):
    if key not in CONTEXT_KEYS:
        raise ValueError('Named existing Compound context required')
    result = {'document': None, 'evidence': {'source': key, 'status': 'unavailable',
        'original_bytes_retained': False, 'source_qualified': False}}
    body = None
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        body = response['Body']
        length = response.get('ContentLength')
        if type(length) is not int or not 0 <= length <= MAX_BYTES:
            raise ValueError('Whole bounded length required')
        raw = bytearray()
        while True:
            part = body.read(min(65536, MAX_BYTES + 1 - len(raw)))
            if not isinstance(part, bytes):
                raise ValueError('Byte stream required')
            if not part:
                break
            raw.extend(part)
            if len(raw) > MAX_BYTES:
                raise ValueError('Context exceeds bound')
        if len(raw) != length:
            raise ValueError('Whole source required')
        raw = bytes(raw)
        doc = strict(raw, response.get('ContentEncoding', ''))
        if not isinstance(doc, dict):
            raise ValueError('Object root required')
        result['document'] = doc
        result['evidence'].update(status='received', source_sha256=hashlib.sha256(raw).hexdigest(),
            source_bytes=len(raw), received_at=datetime.now(timezone.utc).isoformat(),
            whole_source_parsed=True, reported_generated_at=doc.get('generated_at'))
    except Exception:
        result['evidence']['reason'] = 'context_read_or_validation_failed'
    finally:
        if body is not None:
            try:
                body.close()
            except Exception:
                pass
    return result


def day(value):
    if not isinstance(value, str) or re.fullmatch(r'\d{4}-\d{2}-\d{2}', value) is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def pointer(value):
    return str(value).replace('~', '~0').replace('/', '~1')


def family(system):
    normalized = re.sub('[^a-z0-9]', '', system.lower())
    return normalized, next((name for name, keys in FAMILIES.items() if any(k in normalized for k in keys)), None)


def lifecycle(row, first_seen, today):
    components, families, reasons = [], set(), []
    for system in row['systems']:
        normalized, group = family(system)
        if group:
            families.add(group)
        key = row['symbol'] + '|' + normalized
        component = {'system': system, 'family': group,
            'family_prior': PRIORS.get(group, 1.0), 'prior_basis': 'declared_uncalibrated_heuristic',
            'half_life_days': HALF_LIFE.get(group, 10.0), 'first_seen_pointer': '/' + pointer(key),
            'status': 'unavailable', 'decay': None, 'age_days': None}
        components.append(component)
        if first_seen is None:
            component['reason'] = 'first_seen_source_unavailable'
        else:
            new = key not in first_seen
            if new:
                first_seen[key] = today.isoformat()
            raw = first_seen[key]
            stamp = day(raw)
            component.update(first_seen=raw, origin='initialized_this_evaluation' if new else 'stored')
            if stamp is None or stamp > today:
                component['reason'] = 'first_seen_invalid_or_future'
            else:
                age = (today - stamp).days
                decay = 0.5 ** (age / component['half_life_days'])
                if decay == 0 or not math.isfinite(decay):
                    component['reason'] = 'lifecycle_numeric_underflow'
                else:
                    component.update(status='usable', age_days=age, decay=decay)
        if component['status'] != 'usable':
            reasons.append({'system': system, 'reason': component['reason']})
    weight = sum(c['family_prior'] for c in components)
    calculation = {'contract': CONTRACT, 'status': 'unavailable', 'score': None,
        'evaluation_date': today.isoformat(), 'components': components, 'reasons': reasons,
        'formula': 'round(compound_score * round(mean(lifecycle_decay), 3) * mean(family_prior), 1)',
        'observation_freshness_qualified': False, 'forecast_qualified': False,
        'portfolio_qualified': False, 'prior_calibration': 'unvalidated_declared_constants'}
    row.update(families=sorted(families), n_families=len(families), evidence_weight=round(weight, 2),
        evidence_basis='family_priors_v1 (uncalibrated heuristic)', freshness=None,
        freshness_basis='legacy_field_alias_for_first_seen_lifecycle_decay_not_observation_freshness',
        lifecycle_decay=None, desk_score=None, desk_score_calculation=calculation,
        family_pattern=row['n_systems'] >= 4 and len(families) == 3, prime_convergence=False)
    if reasons or not components:
        return
    average = sum(c['decay'] for c in components) / len(components)
    decay = round(average, 3)
    mean_prior = weight / len(components)
    value = row['compound_score'] * decay * mean_prior
    if not math.isfinite(value):
        calculation['reasons'].append({'reason': 'desk_score_overflow'})
        return
    row.update(freshness=decay, lifecycle_decay=decay, desk_score=round(value, 1),
               prime_convergence=row['family_pattern'])
    calculation.update(status='usable', score=row['desk_score'], compound_score=row['compound_score'],
        lifecycle_mean_unrounded=average, lifecycle_mean_rounded=decay,
        family_prior_sum=weight, family_prior_mean=mean_prior)


def reversal_population(doc):
    if not isinstance(doc, dict) or not isinstance(doc.get('rows'), list):
        return {}, {'status': 'unavailable', 'reason': 'reversal_rows_unavailable', 'occurrences': []}
    records = deepcopy(doc['rows'])
    names = [symbol(row.get('ticker')) if isinstance(row, dict) else None for row in records]
    counts = Counter(n for n in names if n is not None)
    selected, evidence = {}, []
    for i, (row, name) in enumerate(zip(records, names)):
        entry = {'pointer': '/rows/' + str(i), 'symbol': name, 'record': row, 'status': 'withheld'}
        evidence.append(entry)
        if name is None:
            entry['reason'] = 'reversal_symbol_unavailable'
        elif counts[name] != 1:
            entry['reason'] = 'duplicate_reversal_symbol'
        else:
            entry['status'] = 'unique_record'
            selected[name] = entry
    return selected, {'status': 'available', 'occurrences': evidence,
                       'selected_count': len(records), 'unique_count': len(selected)}


def reported_reversal(row, selected):
    row.update(archetype=None, reversal_context=None, entry_quality=None, chg5_proxy_pct=None,
               sparkline_change_pct=None)
    entry = selected.get(row['symbol'])
    evidence = {'status': 'unavailable', 'reason': 'no_unique_reversal_record',
        'calendar_window_days': None, 'direction_qualified': False,
        'entry_quality_qualified': False, 'five_day_change_qualified': False}
    row['reversal_evidence'] = evidence
    if entry is None:
        return
    source = entry['record']
    evidence.update(pointer=entry['pointer'], record=source)
    direction = source.get('direction')
    try:
        score = number(source.get('reversal_score'))
        if not 0 <= score <= 100:
            raise InvalidNumber('reversal_score_out_of_range')
        if 'direction' not in source or direction not in (None, 'TOP_FORMING', 'BOTTOM_FORMING'):
            raise InvalidNumber('reported_direction_unavailable')
        row['reversal_context'] = {'direction': direction, 'score': score}
        row['archetype'] = 'TOP_FORMING_REPORTED' if direction == 'TOP_FORMING' and score >= 25 else 'BOTTOM_FORMING_REPORTED' if direction == 'BOTTOM_FORMING' and score >= 25 else 'NO_THRESHOLD_REVERSAL_REPORTED'
        evidence.update(status='reported', reason=None)
    except InvalidNumber as exc:
        evidence['reason'] = str(exc)
    # The producer's spk values have no per-point dates. Report only the actual
    # two-position interval; neither its duration nor a five-day return is known.
    points = source.get('spk')
    if isinstance(points, list) and len(points) >= 3:
        try:
            start, end = number(points[-3]), number(points[-1])
            if start <= 0 or end <= 0:
                raise InvalidNumber('positive_sparkline_values_required')
            change = 100.0 * (end / start - 1)
            if not math.isfinite(change):
                raise InvalidNumber('sparkline_ratio_overflow')
            row['sparkline_change_pct'] = round(change, 1)
            evidence['sparkline_calculation'] = {'start': start, 'end': end,
                'start_pointer': entry['pointer'] + '/spk/' + str(len(points)-3),
                'end_pointer': entry['pointer'] + '/spk/' + str(len(points)-1),
                'interval_count': 2, 'calendar_days': None,
                'formula': 'round(100 * (end / start - 1), 1)', 'change_pct': row['sparkline_change_pct']}
        except InvalidNumber as exc:
            evidence['sparkline_reason'] = str(exc)


def regime(risk, reversal):
    posture = risk.get('posture') if isinstance(risk, dict) else None
    if not isinstance(posture, str) or not posture.strip():
        posture = None
    value, reason = None, 'breadth_unavailable'
    breadth = reversal.get('breadth') if isinstance(reversal, dict) else None
    if isinstance(breadth, dict):
        try:
            bottom, top = number(breadth.get('bottom_pct')), number(breadth.get('top_pct'))
            if not 0 <= bottom <= 100 or not 0 <= top <= 100:
                raise InvalidNumber('breadth_percent_out_of_range')
            value, reason = round(bottom - top, 1), None
        except InvalidNumber as exc:
            reason = str(exc)
    return {'posture': posture, 'turn_net': value}, {'reported_posture': posture,
        'reported_breadth': deepcopy(breadth), 'turn_net_reason': reason,
        'turn_net_formula': 'round(bottom_pct - top_pct, 1)', 'forecast_qualified': False}


def history_population(doc, today, contracts):
    evidence = {'status': 'unavailable', 'window_start': (today-timedelta(days=90)).isoformat(),
        'window_end_exclusive': today.isoformat(), 'cohorts': [], 'observations': 0,
        'population': 'retained_top_400_compound_scores_per_day', 'forecast_qualified': False}
    if not isinstance(doc, dict) or not isinstance(doc.get('days'), list):
        evidence['reason'] = 'history_days_unavailable'
        return [], {}, evidence
    records = deepcopy(doc['days'])
    dates = [day(r.get('d')) if isinstance(r, dict) else None for r in records]
    counts = Counter(d for d in dates if d is not None)
    all_scores, by_symbol = [], {}
    for i, (record, stamp) in enumerate(zip(records, dates)):
        entry = {'pointer': '/days/'+str(i), 'record': record, 'status': 'excluded'}
        evidence['cohorts'].append(entry)
        if stamp is None:
            entry['reason'] = 'cohort_date_invalid'
        elif not today-timedelta(days=90) <= stamp < today:
            entry['reason'] = 'outside_prior_90_calendar_days'
        elif counts[stamp] != 1:
            entry['reason'] = 'duplicate_cohort_date'
        elif any(record.get(k) != v for k, v in contracts.items()):
            entry['reason'] = 'calculation_contract_mismatch'
        elif not isinstance(record.get('scores'), dict) or not record['scores']:
            entry['reason'] = 'score_population_unavailable'
        else:
            try:
                scores = {}
                for name, raw in record['scores'].items():
                    normalized = symbol(name)
                    if normalized is None or normalized in scores:
                        raise InvalidNumber('ambiguous_historical_symbol')
                    scores[normalized] = number(raw)
                entry.update(status='included', scores=scores)
                all_scores.extend(scores.values())
                for name, value in scores.items():
                    by_symbol.setdefault(name, []).append(value)
            except InvalidNumber as exc:
                entry['reason'] = str(exc)
    evidence.update(status='available' if all_scores else 'no_eligible_history', observations=len(all_scores))
    return all_scores, by_symbol, evidence


def apply(ranked, contexts, now, contracts, input_coverage_complete):
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError('Aware evaluation timestamp required')
    if type(input_coverage_complete) is not bool:
        raise ValueError('Explicit declared-collection coverage required')
    today = now.astimezone(timezone.utc).date()
    rows = deepcopy(ranked)
    docs = {key: contexts[key]['document'] for key in CONTEXT_KEYS}
    first_seen = deepcopy(docs['data/compound-firstseen.json'])
    selected, reversal_evidence = reversal_population(docs['data/trend-reversal.json'])
    stamp, regime_evidence = regime(docs['data/risk-gate.json'], docs['data/trend-reversal.json'])
    all_scores, by_symbol, history_evidence = history_population(docs['data/compound-history.json'], today, contracts)
    for row in rows:
        lifecycle(row, first_seen, today)
        reported_reversal(row, selected)
        row['regime'] = deepcopy(stamp)
        row['history_comparison'] = {'population': history_evidence['population'],
            'observations_all': len(all_scores), 'observations_self': len(by_symbol.get(row['symbol'], [])),
            'window_start': history_evidence['window_start'], 'window_end_exclusive': today.isoformat(),
            'formula': 'round(100 * count(score <= current_compound_score) / observations, 1)',
            'forecast_qualified': False}
        for field, values in (('pctile_90d_all', all_scores), ('pctile_90d_self', by_symbol.get(row['symbol'], []))):
            row.pop(field, None)
            if values:
                row[field] = round(100.0 * sum(v <= row['compound_score'] for v in values) / len(values), 1)
    rows.sort(key=lambda r: (r['desk_score'] is None, -(r['desk_score'] if r['desk_score'] is not None else 0), -r['compound_score']))
    history = docs['data/compound-history.json']
    next_history = None
    # A failed or partial source population must not replace the day's score
    # cohort with a deceptively complete smaller population.
    if input_coverage_complete is True and isinstance(history, dict) and isinstance(history.get('days'), list):
        days = [deepcopy(d) for d in history['days'] if not isinstance(d, dict) or d.get('d') != today.isoformat()][-89:]
        days.append({'d': today.isoformat(), **contracts, 'overlay_contract': CONTRACT,
                     'scores': {r['symbol']: r['compound_score'] for r in rows[:400]}})
        next_history = {'days': days}
    evidence = {'contract': CONTRACT, 'evaluation_at': now.astimezone(timezone.utc).isoformat(),
        'sources': {key: contexts[key]['evidence'] for key in CONTEXT_KEYS},
        'reversal': reversal_evidence, 'regime': regime_evidence, 'history': history_evidence,
        'input_coverage_complete': input_coverage_complete,
        'coverage_basis': 'complete declared collections only; upstream universe and observation freshness unqualified',
        'history_write_planned': next_history is not None,
        'first_seen_write_planned': first_seen is not None,
        'ordering': 'usable desk scores descending, then unavailable desk scores; base score breaks ties',
        'independence_qualified': False, 'forecast_qualified': False, 'portfolio_qualified': False}
    return rows, first_seen, next_history, evidence
