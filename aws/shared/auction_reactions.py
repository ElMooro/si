"""Replayable arithmetic over supplied prices and declared auction-day groups.

This validates supplied observations, not provider authenticity, adjusted-price
definitions, exchange-session completeness or historical point-in-time access.
"""
import bisect
from datetime import date, datetime, timezone
from fractions import Fraction
import hashlib
import json
import math

CONTRACT = 'auction-reaction-inputs.v1'
HORIZONS = (('same_day', 0), ('d1', 1), ('d5', 5), ('d20', 20))
FLAGS = {key: False for key in ('original_source_verified', 'price_definition_verified',
                               'session_calendar_verified', 'historical_point_in_time_verified',
                               'forecast_eligible', 'calls_eligible', 'sizing_eligible', 'execution_eligible')}


def number(value):
    if type(value) not in (int, float):
        return None
    try:
        return float(value) if math.isfinite(value) else None
    except (ValueError, OverflowError):
        return None


def day(value):
    if type(value) is not str or len(value) != 10:
        return None
    try:
        parsed = date.fromisoformat(value)
        return value if parsed.isoformat() == value else None
    except ValueError:
        return None


def safe(value):
    """Keep malformed observations inspectable without nonstandard JSON numbers."""
    root, active = [None], set()
    stack = [('value', root, 0, value)]
    while stack:
        operation, parent, key, node = stack.pop()
        if operation == 'leave':
            active.remove(id(node))
        elif node is None or type(node) in (str, bool, int):
            parent[key] = node
        elif type(node) is float:
            parent[key] = node if math.isfinite(node) else {'invalid_numeric': repr(node)}
        elif isinstance(node, (list, dict)):
            if id(node) in active:
                parent[key] = {'invalid_cycle': True}
                continue
            active.add(id(node))
            target = [None] * len(node) if isinstance(node, list) else {}
            parent[key] = target
            stack.append(('leave', None, None, node))
            items = list(enumerate(node)) if isinstance(node, list) else [(str(k), v) for k, v in node.items()]
            stack.extend(('value', target, child_key, child) for child_key, child in reversed(items))
        else:
            parent[key] = {'invalid_type': type(node).__name__, 'representation': repr(node)}
    return root[0]


def bar_day(value):
    if day(value):
        return value
    if type(value) in (int, float) and number(value) is not None:
        try:
            return datetime.fromtimestamp(value, timezone.utc).date().isoformat()
        except (ValueError, OverflowError, OSError):
            return None
    # No string slicing: a malformed date or naive timestamp must not qualify.
    if type(value) is str:
        try:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if parsed.tzinfo is not None:
                return parsed.astimezone(timezone.utc).date().isoformat()
        except ValueError:
            pass
    return None


def retain_bars(bars):
    """No sorting, deduplication, date clipping or dropping missing closes."""
    observations, dates, closes = [], [], []
    if not isinstance(bars, list):
        return {'dates': None, 'closes': None, 'input_observations': safe(bars)}
    for ordinal, raw in enumerate(bars):
        time = raw.get('time') if isinstance(raw, dict) else None
        close = raw.get('close') if isinstance(raw, dict) else None
        observations.append({'ordinal': ordinal, 'time': safe(time), 'close': safe(close),
                             'row_type': type(raw).__name__})
        dates.append(bar_day(time))
        closes.append(number(close))
    return {'dates': dates, 'closes': closes, 'input_observations': observations}


class PriceFrame:
    def __init__(self, source, as_of, symbol=None):
        self.as_of = as_of
        self.source = source if isinstance(source, dict) else {}
        self.dates, self.closes = self.source.get('dates'), self.source.get('closes')
        problems = []
        if day(as_of) is None:
            problems.append({'reason': 'invalid_calculation_date'})
        if self.source.get('reaction_input_contract') == CONTRACT:
            expected_symbol = symbol or self.source.get('requested_symbol')
            if not expected_symbol or self.source.get('requested_symbol') != expected_symbol or self.source.get('response_symbol') != expected_symbol:
                problems.append({'reason': 'proxy_response_symbol_mismatch'})
            count = self.source.get('response_count')
            if type(count) is not int or not isinstance(self.dates, list) or count != len(self.dates):
                problems.append({'reason': 'proxy_response_population_count_mismatch'})
            observations = self.source.get('input_observations')
            if not isinstance(observations, list) or len(observations) != count or any(
                    not isinstance(row, dict) or type(row.get('ordinal')) is not int or row['ordinal'] != index
                    for index, row in enumerate(observations or [])):
                problems.append({'reason': 'complete_original_consumed_fields_required'})
            elif ([bar_day(row.get('time')) for row in observations] != self.dates or
                  [number(row.get('close')) for row in observations] != self.closes):
                problems.append({'reason': 'normalized_prices_do_not_match_retained_inputs'})
        if not isinstance(self.dates, list) or not isinstance(self.closes, list):
            problems.append({'reason': 'date_and_close_arrays_required'})
        else:
            if len(self.dates) != len(self.closes):
                problems.append({'reason': 'mismatched_array_lengths'})
            if not self.dates:
                problems.append({'reason': 'empty_price_population'})
            previous = None
            for index, value in enumerate(self.dates):
                observed = day(value)
                if observed is None:
                    problems.append({'index': index, 'reason': 'invalid_observation_date'})
                else:
                    if previous is not None and observed <= previous:
                        problems.append({'index': index, 'reason': 'duplicate_or_nonascending_date'})
                    if day(as_of) and observed > as_of:
                        problems.append({'index': index, 'reason': 'future_observation_date'})
                    previous = observed
            for index, value in enumerate(self.closes):
                numeric = number(value)
                if numeric is None or numeric <= 0:
                    problems.append({'index': index, 'reason': 'missing_or_nonpositive_finite_price'})
        self.valid = not problems
        self.document = {'contract': CONTRACT, 'symbol': symbol, 'as_of': as_of,
                         'status': 'complete_supplied_frame' if self.valid else 'unavailable',
                         'problems': problems, 'input': safe(source),
                         'date_count': len(self.dates) if isinstance(self.dates, list) else None,
                         'close_count': len(self.closes) if isinstance(self.closes, list) else None,
                         'method': 'Original supplied order; strict unique dates and positive finite prices; no dropped observations.',
                         **FLAGS}
        self.frame_id = hashlib.sha256(json.dumps(self.document, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
        self.document['frame_id'] = self.frame_id

    def returns(self, event_day):
        out = {label: None for label, _ in HORIZONS}
        out['partial'] = []
        trace = {'contract': CONTRACT, 'frame_id': self.frame_id, 'event_day': event_day,
                 'as_of': self.as_of, 'unit': 'decimal_return', 'horizons': {}, **FLAGS}
        out['calculation_inputs'] = trace
        i = bisect.bisect_left(self.dates, event_day) if self.valid and day(event_day) else -1
        event_present = i >= 0 and i < len(self.dates) and self.dates[i] == event_day
        for label, offset in HORIZONS:
            start, end = (i - 1, i) if label == 'same_day' else (i, i + offset)
            reason = ('invalid_supplied_frame' if not self.valid else 'invalid_event_date' if day(event_day) is None else
                      'event_date_absent' if not event_present else 'prior_observation_absent' if start < 0 else
                      'forward_observation_absent' if end >= len(self.dates) else None)
            item = {'status': 'unavailable', 'reason': reason, 'start_index': None, 'end_index': None,
                    'start_date': None, 'end_date': None, 'start_close': None, 'end_close': None,
                    'value': None, 'observed_bar_offset': offset,
                    'formula': 'end_close / start_close - 1; supplied observed bars, not verified exchange sessions'}
            if reason is None:
                value = None
                try:
                    value = float(Fraction(str(self.closes[end])) / Fraction(str(self.closes[start])) - 1)
                    if not math.isfinite(value) or not math.isfinite(value * 100):
                        value = None
                except (ValueError, OverflowError, ZeroDivisionError):
                    pass
                partial = self.dates[end] >= self.as_of
                item.update(start_index=start, end_index=end, start_date=self.dates[start], end_date=self.dates[end],
                            start_close=self.closes[start], end_close=self.closes[end], value=value,
                            status='unavailable' if value is None else 'partial' if partial else 'complete',
                            reason='nonfinite_return' if value is None else 'current_utc_date_may_be_in_progress' if partial else None)
                out[label] = value
                if partial and value is not None:
                    out['partial'].append(label)
            trace['horizons'][label] = item
        return out


def distribution(values):
    observed = [value for value in values if number(value) is not None]
    out = {'n': len(observed), 'median': None, 'mean': None, 'hit': None,
           'excluded_nonfinite_or_invalid': len(values) - len(observed), 'unit': 'percent_return'}
    if len(observed) < 3:
        return out
    ordered = sorted(Fraction(str(value)) for value in observed)
    n = len(ordered)
    median = ordered[n // 2] if n % 2 else (ordered[n // 2 - 1] + ordered[n // 2]) / 2
    try:
        out.update(median=round(float(median * 100), 2), mean=round(float(sum(ordered) * 100 / n), 2),
                   hit=round(100 * sum(value > 0 for value in ordered) / n))
        if any(number(out[key]) is None for key in ('median', 'mean', 'hit')):
            raise OverflowError
    except OverflowError:
        out.update(median=None, mean=None, hit=None, reason='nonfinite_summary')
    return out


def compile_reactions(events, assets, cutoff_day, as_of, labels):
    """Supplied event register + whole input frames reproduce every sample."""
    if day(cutoff_day) is None or day(as_of) is None:
        raise ValueError('Strict event cutoff and calculation dates required')
    if not isinstance(events, list) or not isinstance(assets, dict) or not isinstance(labels, dict):
        raise ValueError('Complete event register, assets and class labels required')
    if any(not isinstance(entry, dict) or day(entry.get('date')) is None or not isinstance(entry.get('classes'), list) or not entry['classes'] or
           any(type(c) is not str or c not in labels for c in entry['classes']) or len(set(entry['classes'])) != len(entry['classes']) for entry in events):
        raise ValueError('Complete declared event classes required')
    if len({entry['date'] for entry in events}) != len(events):
        raise ValueError('Unique dated event groups required')
    eligible = [entry for entry in events if entry['date'] < cutoff_day]
    classes = sorted({label for entry in eligible for label in entry['classes']})
    stats, baseline, frames, coverage = {}, {}, {}, {}
    for symbol, source in assets.items():
        frame = PriceFrame(source, as_of, symbol)
        frames[symbol] = frame.document
        returns = {entry['date']: frame.returns(entry['date']) for entry in eligible}
        def sample(days):
            return {h: distribution([returns[d][h] for d in days if returns[d][h] is not None and h not in returns[d]['partial']]) for h, _ in HORIZONS}
        dates = [entry['date'] for entry in eligible]
        baseline[symbol] = sample(dates)
        coverage[symbol] = {h: {'candidate_events': len(dates), 'complete': baseline[symbol][h]['n'],
                               'partial': sum(h in returns[d]['partial'] for d in dates),
                               'unavailable': sum(returns[d][h] is None for d in dates)} for h, _ in HORIZONS}
        for label in classes:
            stats.setdefault(label, {})[symbol] = sample([entry['date'] for entry in eligible if label in entry['classes']])
    inputs = {'contract': CONTRACT, 'as_of': as_of, 'event_cutoff_exclusive': cutoff_day,
              'events': safe(events), 'frames': frames, 'class_labels': labels,
              'horizons': [list(item) for item in HORIZONS], 'coverage': coverage,
              'unit': 'summary percentages; per-event arithmetic in decimal returns',
              'method': 'All supplied events and price rows retained. Same-day uses prior close; forward horizons count unique supplied observations. Partial UTC-date endpoints excluded from distributions.', **FLAGS}
    return {'stats': stats, 'baseline': baseline, 'n_events': {label: sum(label in e['classes'] for e in eligible) for label in classes},
            'classes': labels, 'horizons': [h for h, _ in HORIZONS], 'comparison_inputs': inputs}


def replay(document):
    if document.get('contract') != CONTRACT or document.get('horizons') != [list(item) for item in HORIZONS]:
        raise ValueError('Known complete reaction input contract required')
    if any(document.get(key) is not False for key in FLAGS):
        raise ValueError('Input replay cannot grant evidence or trading permissions')
    assets = {}
    for symbol, retained in document['frames'].items():
        fresh = PriceFrame(retained['input'], document['as_of'], symbol).document
        if fresh != retained:
            raise ValueError('Retained frame integrity or validation differs: '+symbol)
        assets[symbol] = retained['input']
    result = compile_reactions(document['events'], assets, document['event_cutoff_exclusive'], document['as_of'], document['class_labels'])
    if result['comparison_inputs'] != document:
        raise ValueError('Full input register or coverage does not replay')
    return result
