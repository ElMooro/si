"""Reproducible USD cash-equity holdings risk; never allocation authority.

Daily bars are split adjusted price returns, NOT total returns. Samples align
on both interval endpoints against observed SPY sessions, never array offsets.
Current holdings are repriced on historical returns; this is not account P&L.
"""
from datetime import datetime, timezone, timedelta
import base64
import hashlib
import json
import math
from pathlib import Path
import statistics

from capital_contract import capital_book_view, finite, timestamp
from instrument_identity import resolve_instrument

VERSION = '2.0.0'
MIN_RETURNS = 60
MAX_BAR_AGE_DAYS = 5
MAX_MARK_AGE_H = 96
ARCHIVE_PREFIX = 'history/archive/feed/portfolio/risk.json/'


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def rounded(value, digits=3):
    return round(value, digits) if finite(value) is not None else None


def read_bars(packet, symbol, now):
    errors, closes = [], {}
    if not isinstance(packet, dict) or packet.get('ticker') != symbol or packet.get('adjusted') is not True:
        return {}, ['BAR_IDENTITY_OR_ADJUSTMENT_UNVERIFIED']
    evidence = packet.get('_source_evidence') or {}
    try:
        raw = base64.b64decode(evidence['raw_body_base64'], validate=True)
        received = timestamp(evidence.get('received_at'))
        if hashlib.sha256(raw).hexdigest() != evidence.get('body_sha256') or json.loads(raw) != {k:v for k,v in packet.items() if k != '_source_evidence'}:
            raise ValueError('response mismatch')
        if received is None or not -300 <= (now-received).total_seconds() <= 3600:
            raise ValueError('receipt outside run window')
        if not str(evidence.get('request', '')).startswith('https://api.polygon.io/v2/aggs/ticker/'+symbol+'/range/1/day/'):
            raise ValueError('wrong source identity')
    except (KeyError, ValueError, TypeError):
        return {}, ['ORIGINAL_RESPONSE_EVIDENCE_INVALID']
    if packet.get('status') not in ('OK', 'DELAYED') or packet.get('next_url'):
        return {}, ['BAR_RESPONSE_INCOMPLETE']
    rows = packet.get('results')
    if not isinstance(rows, list):
        return {}, ['BAR_RESULTS_MISSING']
    for row in rows:
        if not isinstance(row, dict):
            errors.append('INVALID_BAR'); continue
        t, close = finite(row.get('t')), finite(row.get('c'))
        try:
            dt = datetime.fromtimestamp(t / 1000, timezone.utc) if t is not None else None
        except (ValueError, OverflowError, OSError):
            dt = None
        if dt is None or close is None or close <= 0:
            errors.append('INVALID_BAR'); continue
        # US daily windows start at midnight Eastern (04:00/05:00 UTC).
        if dt.hour not in (4, 5) or dt.minute or dt.second or dt.microsecond:
            errors.append('NOT_US_DAILY_BAR_START'); continue
        day = dt.date().isoformat()
        if dt.date() > now.date():
            errors.append('FUTURE_SESSION'); continue
        if dt.date() == now.date():
            # Exclude the processing date entirely, including unfinished bars.
            continue
        if day in closes:
            errors.append('DUPLICATE_SESSION'); continue
        closes[day] = close
    if errors:
        return {}, sorted(set(errors))
    if not closes or (now.date() - datetime.fromisoformat(max(closes)).date()).days > MAX_BAR_AGE_DAYS:
        return {}, ['PRICE_HISTORY_STALE_OR_MISSING']
    return dict(sorted(closes.items())), []


def interval_returns(closes, calendar):
    out = {}
    for left, right in zip(calendar, calendar[1:]):
        if left in closes and right in closes and (datetime.fromisoformat(right)-datetime.fromisoformat(left)).days <= 4:
            out[(left, right)] = closes[right] / closes[left] - 1
    return out


def pair_values(left, right):
    keys = sorted(set(left) & set(right))
    return [left[k] for k in keys], [right[k] for k in keys], keys


def correlation(left, right):
    a, b, keys = pair_values(left, right)
    if len(keys) < MIN_RETURNS or statistics.variance(a) == 0 or statistics.variance(b) == 0:
        return None
    return statistics.correlation(a, b)


def beta(left, right):
    a, b, keys = pair_values(left, right)
    if len(keys) < MIN_RETURNS or statistics.variance(b) == 0:
        return None
    return statistics.covariance(a, b) / statistics.variance(b)


def drawdown(closes, count):
    values = list(closes.values())
    if len(values) < count:
        return None
    peak, worst = values[-count], 0.0
    for value in values[-count:]:
        peak = max(peak, value)
        worst = min(worst, value / peak - 1)
    return worst * 100


def exposure_view(snapshot, now):
    positions = snapshot.get('positions')
    errors, signed, gross, sectors = [], {}, {}, {}
    if not isinstance(positions, list) or not positions:
        return {}, {}, {}, ['NO_POSITIONS']
    age = timestamp(snapshot.get('generated_at'))
    if age is None or not -300 <= (now-age).total_seconds() <= MAX_MARK_AGE_H*3600:
        errors.append('SNAPSHOT_STALE_OR_UNDATED')
    for p in positions:
        if not isinstance(p, dict):
            errors.append('INVALID_POSITION'); continue
        sym = p.get('symbol')
        identity = resolve_instrument(sym, p.get('asset_class'))
        if not identity or identity['asset_class'] != 'equity' or p.get('currency', 'USD') != 'USD' or p.get('multiplier', 1) != 1:
            errors.append('UNSUPPORTED_OR_AMBIGUOUS_INSTRUMENT'); continue
        sym = identity['symbol']
        value, qty, price = finite(p.get('market_value')), finite(p.get('qty')), finite(p.get('current_price'))
        mark = finite(p.get('price_asof_unix_ms'))
        mark_age = (now.timestamp() - mark/1000)/3600 if mark is not None else None
        if value is None or qty is None or price is None or price <= 0 or abs(qty*price-value) > 0.021 or p.get('valuation_status') != 'PRICED':
            errors.append('UNPRICED_OR_UNRECONCILED_POSITION'); continue
        if mark_age is None or not -5/60 <= mark_age <= MAX_MARK_AGE_H:
            errors.append('STALE_OR_UNDATED_MARK'); continue
        signed[sym] = signed.get(sym, 0) + value
        gross[sym] = gross.get(sym, 0) + abs(value)
        sector = str(p.get('sector') or 'Unknown')
        sectors[sector] = sectors.get(sector, 0) + abs(value)
    return signed, gross, sectors, sorted(set(errors))


def build(snapshot, packets, generated_at, scenarios):
    now = timestamp(generated_at)
    if now is None:
        raise ValueError('timezone-aware evaluation time required')
    signed, gross_by, sectors, exposure_errors = exposure_view(snapshot, now)
    errors = list(exposure_errors)
    gross, net = sum(gross_by.values()), sum(signed.values())
    if gross <= 0:
        errors.append('NO_GROSS_EXPOSURE')
    symbols = sorted(set(signed) | {'SPY'})
    closes, data_errors = {}, {}
    for sym in symbols:
        closes[sym], data_errors[sym] = read_bars(packets.get(sym), sym, now)
    calendar = sorted(closes['SPY'])
    returns = {sym: interval_returns(closes[sym], calendar) for sym in symbols}
    metrics = {str(p.get('symbol')): {'annual_vol_pct': None, 'beta_spy': None,
               '30d_max_drawdown_pct': None, '90d_max_drawdown_pct': None,
               'status': 'UNAVAILABLE', 'errors': ['EXPOSURE_NOT_VERIFIED']}
               for p in snapshot.get('positions') or [] if isinstance(p, dict)}
    for sym in sorted(signed):
        values = list(returns[sym].values())
        ready = len(values) >= MIN_RETURNS and bool(calendar) and max(closes[sym], default='') == calendar[-1]
        metrics[sym] = {'annual_vol_pct': rounded(statistics.stdev(values)*math.sqrt(252)*100, 2) if ready else None,
                        'beta_spy': rounded(beta(returns[sym], returns['SPY'])) if ready else None,
                        '30d_max_drawdown_pct': rounded(drawdown(closes[sym], 31), 2),
                        '90d_max_drawdown_pct': rounded(drawdown(closes[sym], 91), 2),
                        'drawdown_basis': '30/90 observed sessions; split-adjusted price, not account drawdown',
                        'n_bars': len(closes[sym]), 'n_returns': len(values),
                        'as_of': max(closes[sym], default=None), 'status': 'AVAILABLE' if ready else 'UNAVAILABLE',
                        'errors': data_errors[sym] + ([] if ready else ['INSUFFICIENT_OR_LAGGING_HISTORY'])}
    active = [sym for sym in sorted(signed) if signed[sym] != 0]
    # One common sample for every covariance/portfolio return: PSD by construction.
    common = set(returns['SPY'])
    for sym in active:
        common &= set(returns[sym])
    keys = sorted(common)
    if len(keys) < MIN_RETURNS:
        errors.append('INSUFFICIENT_COMMON_SESSIONS')
    if not keys or not calendar or keys[-1][1] != calendar[-1]:
        errors.append('COMMON_SAMPLE_NOT_CURRENT')
    if any(data_errors[sym] for sym in active + ['SPY']):
        errors.append('INVALID_OR_MISSING_MARKET_INPUT')
    rows = {sym: {key: returns[sym][key] for key in keys} for sym in active}
    corr = {a: {b: rounded(correlation(rows[a], rows[b])) for b in active} for a in active}
    modeled = not errors
    pnl = [sum(signed[sym]*returns[sym][key] for sym in active) for key in keys] if modeled else []
    daily = statistics.stdev(pnl) if pnl else None
    gross_returns = {key: value/gross for key, value in zip(keys, pnl)} if pnl and gross else {}
    gross_beta = beta(gross_returns, returns['SPY']) if gross_returns else None
    book = capital_book_view(snapshot, now)
    nav, nav_errors = None, list(book['errors'])
    bc = book['contract']
    if book['status'] == 'READY':
        cash, liabilities = finite(bc.get('cash')), finite(bc.get('liabilities'))
        if bc.get('currency') != 'USD' or cash is None or liabilities is None or abs(cash+net-liabilities-book['equity_nav']) > 0.021:
            nav_errors.append('NAV does not reconcile to USD cash + signed marked holdings - liabilities')
        if set(book['signed_weights']) != set(signed) or any(abs(book['signed_weights'].get(s, 0)*book['equity_nav']-v) > 0.021 for s,v in signed.items()):
            nav_errors.append('capital book and marked snapshot positions differ')
        if not nav_errors and not errors:
            nav = book['equity_nav']
    concentration = [{'sector': s, 'weight_pct': rounded(v/gross*100, 2)} for s,v in sorted(sectors.items(), key=lambda x:-x[1])] if gross and not exposure_errors else []
    hhi = sum((v/gross*100)**2 for v in sectors.values()) if concentration else None
    projections = {}
    for sid, scenario in scenarios.items():
        parts, unmodeled = [], []
        for p in snapshot.get('positions') or []:
            shock = finite(scenario.get('sector_returns', {}).get(p.get('sector')))
            mv = finite(p.get('market_value'))
            valid = shock is not None and mv is not None and not exposure_errors
            parts.append({'symbol': p.get('symbol'), 'sector': p.get('sector'), 'scenario_return': rounded(shock*100 if shock is not None else None),
                          'scenario_pnl': rounded(mv*shock, 2) if valid else None})
            if not valid: unmodeled.append(p.get('symbol'))
        complete = bool(parts) and not unmodeled
        total = sum(row['scenario_pnl'] for row in parts) if complete else None
        projections[sid] = {'name': scenario['name'], 'duration_days': scenario['duration_days'],
                            'basis': 'Hypothetical static sector shocks; historical calibration and classification not verified',
                            'historical_replay_verified': False, 'probability': None,
                            'spy_return_pct': rounded(scenario['spy_return']*100),
                            'projected_pnl_dollars': rounded(total, 2), 'projected_pnl_pct': rounded(total/nav*100, 2) if total is not None and nav else None,
                            'pct_of_gross_exposure': rounded(total/gross*100, 2) if total is not None and gross else None,
                            'unmodeled_symbols': unmodeled, 'per_position': parts}
    annual = daily*math.sqrt(252) if daily is not None else None
    report = {
        'engine': 'justhodl-portfolio-risk', 'schema_version': VERSION, 'generated_at': generated_at,
        'status': 'no_positions' if not snapshot.get('positions') else 'AVAILABLE_HOLDINGS_MODEL' if modeled else 'INCOMPLETE',
        'permissions': {'sizing_eligible': False, 'may_recommend_trades': False, 'reason': 'Price-risk observations and hypothetical shocks do not establish forecast edge or account suitability'},
        'quality': {'status': 'partial' if modeled else 'unavailable', 'reason_codes': sorted(set(errors)), 'data_errors': data_errors},
        'risk_contract': {'scope': 'USD cash equities/ETFs; current signed quantities, multiplier 1; no options/FX',
                          'currency_basis': 'Legacy snapshot Polygon US-stock marks are USD; explicit other currencies rejected',
                          'return_basis': 'Split-adjusted daily price returns; dividends, financing, fees and liquidity losses excluded',
                          'alignment': 'Matching interval endpoints on observed SPY sessions; one common sample',
                          'sample_count': len(keys), 'minimum_sample': MIN_RETURNS,
                          'sample_start': keys[0][0] if keys else None, 'sample_end': keys[-1][1] if keys else None,
                          'annualization_sessions': 252, 'model': 'Normal zero-mean 1-day delta-normal VaR; not maximum possible loss or a calibrated tail guarantee',
                          'missing_input_defaults': False, 'netting': 'Duplicate symbol lots summed; gross exposure sums absolute lots',
                          'freshness': {'bar_max_calendar_days': MAX_BAR_AGE_DAYS, 'mark_max_hours': MAX_MARK_AGE_H, 'release_calendar_verified': False}},
        'capital_basis': {'status': 'RECONCILED' if nav else 'UNAVAILABLE', 'nav': nav, 'currency': 'USD', 'errors': nav_errors},
        'total_market_value': rounded(net, 2) if not exposure_errors else None,
        'gross_market_value': rounded(gross, 2) if not exposure_errors else None,
        'n_positions': len(snapshot.get('positions') or []), 'n_instruments': len(signed),
        'holdings_risk': {'daily_pnl_std_dollars': rounded(daily, 2),
                          'annual_vol_pct_of_gross': rounded(annual/gross*100, 2) if annual is not None and gross else None,
                          'beta_spy_per_gross': rounded(gross_beta),
                          'var_1d_99_dollars': rounded(daily*2.326347874, 2) if daily is not None else None,
                          'var_1d_95_dollars': rounded(daily*1.644853627, 2) if daily is not None else None},
        'portfolio_vol_annual_pct': rounded(annual/nav*100, 2) if annual is not None and nav else None,
        'portfolio_vol_daily_pct': rounded(daily/nav*100) if daily is not None and nav else None,
        'portfolio_beta_spy': rounded(gross_beta*gross/nav) if gross_beta is not None and nav else None,
        'var_1d_99_dollars': rounded(daily*2.326347874, 2) if daily is not None and nav else None,
        'var_1d_95_dollars': rounded(daily*1.644853627, 2) if daily is not None and nav else None,
        'var_1d_99_pct': rounded(daily*2.326347874/nav*100, 2) if daily is not None and nav else None,
        'var_1d_95_pct': rounded(daily*1.644853627/nav*100, 2) if daily is not None and nav else None,
        'position_metrics': metrics, 'correlation_matrix': corr, 'correlation_clusters': [],
        'sector_concentration': concentration, 'concentration_basis': 'Absolute marked lot exposure / gross marked exposure; not NAV and not ETF look-through',
        'concentration_hhi': rounded(hhi, 1), 'concentration_label': 'Gross sector HHI (0..10000)' if hhi is not None else 'Unavailable',
        'max_sector_concentration_pct': concentration[0]['weight_pct'] if concentration else None,
        'historical_scenarios': projections,
        'stops_hit': [p for p in snapshot.get('positions') or [] if p.get('stop_hit') is True] if not exposure_errors else [],
        'etf_lookthrough': {'status': 'UNVERIFIED', 'sector_lookthrough': [], 'geo_lookthrough': [], 'note': 'ETF constituent vintages and coverage have not been reconciled to this book'},
    }
    seen = set()
    for anchor in active if modeled else []:
        members = [anchor]
        for candidate in active:
            if candidate not in members and all(corr[candidate][other] is not None and corr[candidate][other] >= 0.8 for other in members):
                members.append(candidate)
        identity = tuple(sorted(members))
        if len(members) >= 4 and identity not in seen:
            seen.add(identity)
            values = [corr[a][b] for i,a in enumerate(members) for b in members[i+1:]]
            report['correlation_clusters'].append({'symbols': sorted(members), 'avg_pairwise_correlation': rounded(statistics.mean(values)),
                'total_weight_pct': rounded(sum(gross_by[s] for s in members)/gross*100, 2), 'weight_basis': 'gross absolute lots'})
    report['alerts_summary'] = {'var_breach': report['var_1d_99_pct'] > 5 if report['var_1d_99_pct'] is not None else None,
        'stops_hit_count': len(report['stops_hit']), 'sector_concentration_breach': concentration[0]['weight_pct'] > 40 if concentration else None,
        'correlation_cluster_count': len(report['correlation_clusters'])}
    return report


def code_identity():
    import capital_contract, instrument_identity
    return {p.name: hashlib.sha256(p.read_text(encoding='utf-8').replace('\r\n','\n').encode()).hexdigest()
            for p in (Path(__file__), Path(capital_contract.__file__), Path(instrument_identity.__file__))}


def freeze(snapshot, packets, generated_at, scenarios):
    inputs = json.loads(canonical({'snapshot': snapshot, 'packets': packets, 'generated_at': generated_at, 'scenarios': scenarios}))
    output = build(**inputs)
    bundle = {'schema_version': VERSION, 'scope': 'private owner risk inputs; not a public research artifact',
              'code': code_identity(), 'inputs': inputs, 'output_sha256': digest(output)}
    return bundle, output


def replay(bundle):
    if bundle.get('schema_version') != VERSION or bundle.get('code') != code_identity():
        raise ValueError('risk replay code/schema mismatch')
    out = build(**bundle['inputs'])
    if digest(out) != bundle.get('output_sha256'):
        raise ValueError('risk replay output mismatch')
    return out
