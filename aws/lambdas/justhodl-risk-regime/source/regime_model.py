"""Original-source cross-asset measurements. Pure compiler; no forecast authority."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

CONTRACT = 'risk-regime-research.v1'
PREFIX = 'data/risk-regime-research/'
CURRENT = 'data/risk-regime.json'
SERIES = {'VIXCLS': 'Index', 'VXVCLS': 'Index', 'BAMLH0A0HYM2': 'Percent'}
SYMBOLS = ('SPY', 'HYG')
PERMISSIONS = dict(calls_eligible=False, sizing_eligible=False, execution_eligible=False)
ET = ZoneInfo('America/New_York')


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value): return hashlib.sha256(encoded(value)).hexdigest()


def clock(value):
    at = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if at.tzinfo is None: raise ValueError('timezone required')
    return at.astimezone(timezone.utc)


def decimal(value):
    if value is None or isinstance(value, bool) or value in ('', '.'): return None
    try:
        n = Decimal(str(value))
        return n if n.is_finite() else None
    except (InvalidOperation, ValueError): return None


def number(value):
    n = decimal(value)
    return float(n) if n is not None and abs(n) < Decimal('1e100') else None


def safe_url(value, path, host='api.massive.com'):
    u = urlsplit(value)
    if u.scheme != 'https' or u.netloc != host or u.path != path or u.fragment:
        raise ValueError('provider request identity differs')
    q = parse_qs(u.query, keep_blank_values=True)
    if any(len(v) != 1 or k.lower() in ('apikey', 'api_key', 'token', 'authorization') for k, v in q.items()):
        raise ValueError('ambiguous or credential-bearing request')
    return {k: v[0] for k, v in q.items()}


def original(inputs, bodies, name):
    source = inputs['sources'].get(name) or {}
    if source.get('status') != 'captured': raise ValueError('source_unavailable: ' + name)
    raw = bodies[name]
    if len(raw) != source['bytes'] or hashlib.sha256(raw).hexdigest() != source['sha256']:
        raise ValueError('source bytes differ')
    at = clock(source['acquired_at']); now = clock(inputs['generated_at'])
    if not 0 <= (now-at).total_seconds() <= 3600: raise ValueError('source acquisition outside run')
    if source['request_sha256'] != hashlib.sha256(source['request_url'].encode()).hexdigest():
        raise ValueError('request binding differs')
    doc = json.loads(raw)
    if not isinstance(doc, dict): raise ValueError('provider document required')
    return doc, source


def fred(inputs, bodies, sid):
    definition, dr = original(inputs, bodies, 'definition:' + sid)
    document, ref = original(inputs, bodies, 'observations:' + sid)
    end = date.fromisoformat(inputs['evaluation_date']); start = end-timedelta(days=800)
    base = {'series_id': sid, 'file_type': 'json', 'realtime_start': str(end), 'realtime_end': str(end)}
    if safe_url(dr['request_url'], '/fred/series', 'api.stlouisfed.org') != base:
        raise ValueError('definition query differs')
    query = {**base, 'observation_start': str(start), 'observation_end': str(end), 'units': 'lin',
             'sort_order': 'asc', 'limit': '10000', 'offset': '0', 'output_type': '1'}
    if safe_url(ref['request_url'], '/fred/series/observations', 'api.stlouisfed.org') != query:
        raise ValueError('observation query differs')
    meta = definition.get('seriess') or []
    if len(meta) != 1 or meta[0].get('id') != sid or meta[0].get('units') != SERIES[sid] or meta[0].get('frequency_short') != 'D':
        raise ValueError('official definition differs')
    if not meta[0].get('seasonal_adjustment'): raise ValueError('seasonal adjustment unspecified')
    source_rows = document.get('observations')
    if not isinstance(source_rows, list) or not source_rows or type(document.get('count')) is not int or len(source_rows) != document['count']:
        raise ValueError('incomplete provider history')
    for key, value in [('units', 'lin'), ('sort_order', 'asc'), ('output_type', 1), ('offset', 0), ('limit', 10000),
                       ('realtime_start', str(end)), ('realtime_end', str(end))]:
        if document.get(key) != value: raise ValueError('provider response differs from query: ' + key)
    rows = []
    for i, r in enumerate(source_rows):
        day = date.fromisoformat(r['date'])
        if not start <= day <= end or (rows and r['date'] <= rows[-1]['date']): raise ValueError('invalid observation ordering/bounds')
        if r.get('realtime_start') != str(end) or r.get('realtime_end') != str(end): raise ValueError('mixed source vintages')
        val = decimal(r.get('value'))
        if val is not None and val < 0: raise ValueError('negative volatility or OAS')
        rows.append({'date': str(day), 'value': float(val) if val is not None else None,
                     'decimal': str(val) if val is not None else None, 'row_index': i})
    latest = rows[-1]; age = (end-date.fromisoformat(latest['date'])).days
    status = 'unavailable' if latest['value'] is None else 'stale' if age > 7 else 'fresh'
    prior = rows[-6] if len(rows) >= 6 else None
    contiguous = len(rows) >= 6 and all(r['value'] is not None for r in rows[-6:])
    difference = Decimal(latest['decimal'])-Decimal(prior['decimal']) if contiguous else None
    window_start = date.fromisoformat(latest['date'])-timedelta(days=730)
    window = [r for r in rows if r['date'] >= str(window_start)]
    finite = [r for r in window if r['decimal'] is not None]
    complete_span = rows[0]['date'] <= str(window_start+timedelta(days=7))
    rank = None
    if latest['decimal'] is not None and len(finite) >= 252 and complete_span:
        current = Decimal(latest['decimal']); values = [Decimal(r['decimal']) for r in finite]
        rank = 100*(sum(x < current for x in values)+0.5*sum(x == current for x in values))/len(values)
    return {'series_id': sid, 'title': meta[0].get('title'), 'value': latest['value'], 'value_decimal': latest['decimal'],
        'unit': SERIES[sid], 'frequency': 'daily', 'seasonal_adjustment': meta[0]['seasonal_adjustment'],
        'observation_date': latest['date'], 'row_index': latest['row_index'], 'acquired_at': ref['acquired_at'],
        'provider_updated_at': meta[0].get('last_updated'), 'first_publication_at': None, 'current_vintage_date': str(end),
        'quality': {'status': status, 'age_days': age, 'max_age_days': 7, 'basis': 'observation_date',
                    'note': 'Calendar age ceiling; no exchange holiday or release-calendar assertion.'},
        'change_5_provider_rows': {'value': float(difference) if difference is not None else None,
            'decimal': str(difference) if difference is not None else None,
            'unit': 'percentage_points' if SERIES[sid] == 'Percent' else 'index_points',
            'current_date': latest['date'], 'baseline_date': prior['date'] if prior else None,
            'baseline_row_index': prior['row_index'] if prior else None,
            'reason': None if contiguous else 'missing_comparison_row', 'formula': 'latest minus fifth preceding provider row; all six rows must be finite'},
        'percentile_2y': {'value': rank, 'unit': 'percentile_0_100', 'window_start': str(window_start),
            'window_end': latest['date'], 'n_finite': len(finite), 'n_missing': len(window)-len(finite),
            'complete_span': complete_span, 'formula': '100 * (count_below + 0.5 * count_equal) / n_finite; includes latest',
            'reason': None if rank is not None else 'missing_latest_or_insufficient_history', 'predictive_probability': False},
        'history': rows, 'originals': {'definition': dr, 'observations': ref}, **PERMISSIONS}


def interpolate(candidates):
    """Same expiry only; trace exact contracts and interpolation weights."""
    clean = sorted(candidates, key=lambda r: (r['abs_delta'], r['ticker']))
    exact = next((r for r in clean if r['abs_delta'] == 0.25), None)
    if exact: return {'iv': exact['iv'], 'contracts': [exact], 'weights': [1.0]}
    low = [r for r in clean if 0.15 <= r['abs_delta'] < 0.25]
    high = [r for r in clean if 0.25 < r['abs_delta'] <= 0.35]
    if not low or not high: return None
    a, b = low[-1], high[0]
    if b['abs_delta']-a['abs_delta'] > 0.15: return None
    w = (0.25-a['abs_delta'])/(b['abs_delta']-a['abs_delta'])
    return {'iv': a['iv']*(1-w)+b['iv']*w, 'contracts': [a, b], 'weights': [1-w, w]}


def options(inputs, bodies, symbol):
    previous, prev_ref = original(inputs, bodies, 'previous:' + symbol)
    if safe_url(prev_ref['request_url'], '/v2/aggs/ticker/'+symbol+'/prev') != {'adjusted': 'true'}:
        raise ValueError('underlying reference query differs')
    results = previous.get('results') or []
    if previous.get('ticker') != symbol or len(results) != 1 or previous.get('adjusted') is not True:
        raise ValueError('underlying reference identity differs')
    spot = decimal(results[0].get('c')); timestamp = number(results[0].get('t'))
    if spot is None or spot <= 0 or timestamp is None: raise ValueError('underlying reference missing')
    at = datetime.fromtimestamp(timestamp/1000, timezone.utc); day = at.astimezone(ET).date()
    end = date.fromisoformat(inputs['evaluation_date'])
    if not 0 <= (end-day).days <= 7: raise ValueError('underlying reference stale/future')
    lo, hi = f'{spot*Decimal("0.88"):.2f}', f'{spot*Decimal("1.12"):.2f}'
    min_exp, max_exp = str(end+timedelta(days=21)), str(end+timedelta(days=45))
    query = {'strike_price.gte': lo, 'strike_price.lte': hi, 'expiration_date.gte': min_exp,
        'expiration_date.lte': max_exp, 'sort': 'ticker', 'order': 'asc', 'limit': '250'}
    names = inputs.get('option_pages', {}).get(symbol) or []
    if not 1 <= len(names) <= 12: raise ValueError('no bounded options chain')
    rows = []; seen = set(); expected_url = None; refs = []
    for page, name in enumerate(names):
        if name != f'options:{symbol}:{page+1}': raise ValueError('noncontiguous options pages')
        doc, ref = original(inputs, bodies, name); refs.append(ref)
        q = safe_url(ref['request_url'], '/v3/snapshot/options/'+symbol)
        if page == 0 and q != query: raise ValueError('options universe query differs')
        if page > 0 and ref['request_url'] != expected_url: raise ValueError('options cursor chain broken')
        if page > 0 and set(q) != {'cursor'}: raise ValueError('unexpected continuation query')
        if doc.get('status') not in ('OK', 'DELAYED'): raise ValueError('options provider status unavailable')
        result = doc.get('results')
        if not isinstance(result, list) or len(result) > 250: raise ValueError('invalid options page')
        expected_url = doc.get('next_url')
        if expected_url: safe_url(expected_url, '/v3/snapshot/options/'+symbol)
        if page < len(names)-1 and not expected_url: raise ValueError('extra page after terminal cursor')
        for index, item in enumerate(result):
            d = item.get('details') or {}; ticker = d.get('ticker'); strike = decimal(d.get('strike_price'))
            expiry = d.get('expiration_date'); side = d.get('contract_type')
            if (not isinstance(ticker, str) or not ticker.startswith('O:'+symbol) or ticker in seen or
                side not in ('call', 'put') or not isinstance(expiry, str) or not min_exp <= expiry <= max_exp or
                strike is None or not Decimal(lo) <= strike <= Decimal(hi) or
                (item.get('underlying_asset') or {}).get('ticker') != symbol):
                raise ValueError('duplicate or out-of-universe option contract')
            date.fromisoformat(expiry); seen.add(ticker)
            bar = item.get('day') or {}; volume = number(bar.get('volume')); oi = number(item.get('open_interest'))
            if volume is not None and volume < 0 or oi is not None and oi < 0: raise ValueError('negative options activity')
            stamp = number(bar.get('last_updated')); session = None
            if stamp is not None:
                dt = datetime.fromtimestamp(stamp/1e9, timezone.utc)
                if dt > clock(ref['acquired_at'])+timedelta(seconds=1): raise ValueError('future options day timestamp')
                session = dt.astimezone(ET).date().isoformat()
            delta = number((item.get('greeks') or {}).get('delta')); iv = number(item.get('implied_volatility'))
            if delta is not None and not (0 <= delta <= 1 if side == 'call' else -1 <= delta <= 0): delta = None
            if iv is not None and not 0 < iv < 10: iv = None
            rows.append({'ticker': ticker, 'expiry': expiry, 'side': side, 'strike': float(strike),
                'shares_per_contract': d.get('shares_per_contract'), 'exercise_style': d.get('exercise_style'),
                'volume': volume, 'day_date': session, 'day_updated_at_ns': stamp, 'open_interest': oi,
                'delta': delta, 'iv': iv, 'page': page+1, 'row_index': index})
    complete = not expected_url
    expiries = []
    for expiry in sorted({r['expiry'] for r in rows}):
        group = [r for r in rows if r['expiry'] == expiry]; sessions = sorted({r['day_date'] for r in group if r['day_date']})
        volume_rows = [r for r in group if r['volume'] is not None and r['day_date'] == str(day)]
        volumes = {side: sum(r['volume'] for r in volume_rows if r['side'] == side) for side in ('call', 'put')}
        has_both = all(any(r['side'] == side for r in group) for side in ('call', 'put'))
        volume_complete = complete and has_both and len(volume_rows) == len(group)
        oi_complete = complete and has_both and all(r['open_interest'] is not None for r in group)
        oi = {side: sum(r['open_interest'] for r in group if r['side'] == side and r['open_interest'] is not None) for side in ('call', 'put')}
        interpolated = {}
        for side in ('call', 'put'):
            candidates = [{'ticker': r['ticker'], 'abs_delta': abs(r['delta']), 'iv': r['iv'], 'page': r['page'], 'row_index': r['row_index']}
                for r in group if r['side'] == side and r['delta'] is not None and r['iv'] is not None and r['shares_per_contract'] == 100]
            interpolated[side] = interpolate(candidates) if complete else None
        skew = ((interpolated['put']['iv']-interpolated['call']['iv'])*100
                if all(interpolated.values()) else None)
        expiries.append({'expiry': expiry, 'contracts': len(group), 'daily_volume_session': str(day), 'observed_sessions': sessions,
            'volume_rows_in_session': len(volume_rows), 'volume_coverage': len(volume_rows)/len(group),
            'reported_volume_in_session': volumes,
            'put_call_volume_ratio': volumes['put']/volumes['call'] if volume_complete and volumes['call'] > 0 else None,
            'volume_ratio_reason': None if volume_complete and volumes['call'] > 0 else 'incomplete_chain_or_missing_session_volume_or_zero_call_volume',
            'open_interest': oi if oi_complete else None,
            'put_call_open_interest_ratio': oi['put']/oi['call'] if oi_complete and oi['call'] > 0 else None,
            'oi_observation_date': None, 'oi_timing': 'provider describes end of last trading day; response has no observation timestamp',
            'skew_25delta_vol_points': skew, 'interpolation': interpolated,
            'iv_observed_at': None, 'iv_timing_verified': False,
            'skew_quality': 'undated_provider_snapshot' if skew is not None else 'unavailable',
            'note': 'Same-expiry put IV minus call IV at absolute delta 0.25; linear interpolation, no extrapolation. No timestamp-coherence claim.'})
    return {'symbol': symbol, 'quality': {'status': 'research_only' if complete else 'incomplete_pagination'},
        'universe': {'expiration_min': min_exp, 'expiration_max': max_exp, 'strike_min': lo, 'strike_max': hi,
            'underlying_reference_close': float(spot), 'underlying_reference_date': str(day), 'underlying_reference': prev_ref,
            'note': '21–45 calendar days to expiry; strikes within ±12% of previous reported close. Not the whole options market.'},
        'pagination_complete': complete, 'pages': len(names), 'contracts': len(rows), 'next_page_missing': bool(expected_url),
        'expiries': expiries, 'contract_rows': rows, 'originals': refs, **PERMISSIONS}


def fx(inputs, bodies):
    doc, ref = original(inputs, bodies, 'fx:AUDJPY'); end = date.fromisoformat(inputs['evaluation_date'])
    path = f'/v2/aggs/ticker/C:AUDJPY/range/1/day/{end-timedelta(days=65)}/{end-timedelta(days=1)}'
    if safe_url(ref['request_url'], path) != {'adjusted': 'true', 'sort': 'asc', 'limit': '50000'}:
        raise ValueError('FX query differs')
    source = doc.get('results') or []
    if doc.get('ticker') != 'C:AUDJPY' or doc.get('next_url') or doc.get('resultsCount') != len(source):
        raise ValueError('incomplete FX range')
    rows = []
    for i, r in enumerate(source):
        val, stamp = decimal(r.get('c')), number(r.get('t'))
        if val is None or val <= 0 or stamp is None: raise ValueError('FX bar invalid')
        dt = datetime.fromtimestamp(stamp/1000, timezone.utc); day = dt.date()
        if not end-timedelta(days=65) <= day < end or (rows and day.isoformat() <= rows[-1]['date']):
            raise ValueError('FX bar bounds/order differ')
        rows.append({'date': str(day), 'value': float(val), 'decimal': str(val), 'row_index': i, 'bar_timestamp_ms': stamp})
    if not rows: raise ValueError('FX bars missing')
    last = rows[-1]; target = date.fromisoformat(last['date'])-timedelta(days=7)
    baseline = next((r for r in reversed(rows) if r['date'] == str(target)), None)
    change = 100*(Decimal(last['decimal'])/Decimal(baseline['decimal'])-1) if baseline else None
    age = (end-date.fromisoformat(last['date'])).days
    return {'pair': 'AUDJPY', 'value': last['value'], 'unit': 'JPY_per_AUD', 'observation_date': last['date'],
        'acquired_at': ref['acquired_at'], 'history': rows, 'original': ref,
        'quality': {'status': 'fresh' if age <= 7 else 'stale', 'age_days': age, 'max_age_days': 7},
        'change_7_calendar_days_pct': float(change) if change is not None else None,
        'baseline_date': baseline['date'] if baseline else None, 'target_date': str(target),
        'note': 'Provider UTC daily aggregate close; missing exact weekly baseline stays null. One FX pair, not an FX regime.', **PERMISSIONS}


def build(inputs, bodies):
    if inputs.get('contract') != 'risk-regime-inputs.v1': raise ValueError('input contract differs')
    if date.fromisoformat(inputs['evaluation_date']) > clock(inputs['generated_at']).date(): raise ValueError('future evaluation date')
    measurements = {}; problems = {}
    for sid in SERIES:
        try: measurements[sid] = fred(inputs, bodies, sid)
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            measurements[sid] = {'series_id': sid, 'value': None, 'unit': SERIES[sid], 'quality': {'status': 'unavailable'}, **PERMISSIONS}
            problems[sid] = str(exc)
    option_rows = {}
    for symbol in SYMBOLS:
        try: option_rows[symbol] = options(inputs, bodies, symbol)
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            option_rows[symbol] = {'symbol': symbol, 'pagination_complete': False, 'expiries': [], 'quality': {'status': 'unavailable'}, **PERMISSIONS}
            problems['options:'+symbol] = str(exc)
    try: currency = fx(inputs, bodies)
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        currency = {'pair': 'AUDJPY', 'value': None, 'quality': {'status': 'unavailable'}, **PERMISSIONS}; problems['fx:AUDJPY'] = str(exc)
    vix, vxv, hy = (measurements[s] for s in SERIES)
    matched = (vix['quality']['status'] == vxv['quality']['status'] == 'fresh' and vix.get('observation_date') == vxv.get('observation_date'))
    spread = float(Decimal(vix['value_decimal'])-Decimal(vxv['value_decimal'])) if matched else None
    term = {'value': spread, 'unit': 'index_points', 'observation_date': vix.get('observation_date') if matched else None,
        'formula': 'VIXCLS - VXVCLS at the same observation date', 'reason': None if matched else 'dates_differ_or_source_unavailable',
        'interpretation': '30-day minus 3-month implied-volatility index; not a VIX futures curve', **PERMISSIONS}
    fresh = sum(r['quality']['status'] == 'fresh' for r in measurements.values()) + (currency['quality']['status'] == 'fresh')
    return {'contract': CONTRACT, 'engine': 'risk-regime', 'version': '2.0.0', 'generated_at': inputs['generated_at'],
        'evaluation_date': inputs['evaluation_date'], 'measurements': measurements, 'term_structure': term,
        'option_cohorts': option_rows, 'fx_measurement': currency, 'source_failures': problems,
        'quality': {'status': 'degraded' if problems or fresh < 4 or any(not r.get('pagination_complete') for r in option_rows.values()) else 'research_only', 'fresh_native_series': fresh,
            'native_series_expected': 4, 'basis': 'per-observation dates, complete queries and separately timed option fields',
            'note': 'Fresh source coverage is not predictive validation. Options IV and OI timestamps are not supplied.'},
        'call': None, 'decision': {'verb': 'WAIT', 'meaning': 'abstain', 'reason': 'No validated out-of-sample decision model or portfolio-specific risk budget.'},
        'risk_regime_score': None, 'risk_regime': 'UNQUALIFIED', 'scale': None,
        'posture': {'beta_tilt': None, 'size_mult': None, 'hedge': None, 'reason': 'position authority not qualified'},
        'components': {'vix': {'vix': vix['value'], 'vix_3m': vxv['value'], 'term_structure': spread},
            'credit': {'hy_oas': hy['value'], 'pctile': None, 'chg_5d_bp': None,
                'note': 'Deprecated ambiguous summaries withheld; dated calculations are in measurements.'},
            'options': {'SPY': {'pcr': None, 'skew_25d': None}, 'HYG': {'pcr': None, 'skew_25d': None}},
            'fx': {'fx_roro_score': None, 'regime': 'UNQUALIFIED'}},
        'blocks_used': [], 'tells': [], 'participation': {}, 'cross_border': {}, 'systemic_stress': {},
        'liquidity': {}, 'capital_inflows': {}, 'secondary_risk': {}, 'dollar_context': {}, 'wl_research': {},
        'context': inputs.get('context', {}),
        'dependency_roots': {'volatility': ['fred:VIXCLS', 'fred:VXVCLS'], 'credit': ['fred:BAMLH0A0HYM2'],
            'options': ['massive:options:SPY', 'massive:options:HYG'], 'fx': ['massive:C:AUDJPY']},
        'independent_vote_count': 0,
        'methodology': {'risk_score': 'No arbitrary weighted score or recommendation.',
            'history': 'FRED current-vintage history; unsuitable for point-in-time backtests without historical vintages.',
            'options': 'Complete bounded universe required. Volume and open interest never blended; IV skew grouped by expiry.',
            'portfolio_consequence': 'No size, hedge budget or execution change is authorized by these measurements.',
            'original_provider_verified': not bool(problems), 'context_original_provider_verified': False}, **PERMISSIONS}
