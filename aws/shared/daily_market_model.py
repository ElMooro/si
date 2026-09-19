"""Deterministic, original-response market measurements. No forecast authority.

Polygon aggregates are ET daily windows, split adjusted, not dividend total
returns or executable closing-auction fills. CoinGecko changes are explicitly
provider reported. Neither ticker identity nor current adjusted history proves
historical point-in-time security-master or corporate-action availability.
"""
from calendar import monthrange
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import math
import re
from zoneinfo import ZoneInfo

CONTRACT = 'daily-market-sources.v1'
ET = ZoneInfo('America/New_York')


def clock(value):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('aware timestamp required')
    return stamp.astimezone(timezone.utc)


def number(value, positive=False, nonnegative=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('finite numeric source value required')
    if positive and value <= 0 or nonnegative and value < 0:
        raise ValueError('source value outside domain')
    return value


def optional(value, **kwargs):
    return None if value is None else number(value, **kwargs)


def receipt(source, provider, generated_at):
    evidence = source.get('evidence') or {}
    if evidence.get('contract') != 'source-evidence.v1' or evidence.get('provider') != provider or evidence.get('captured') is not True:
        raise ValueError('captured source evidence required')
    sha = evidence.get('sha256', '')
    if not re.fullmatch(r'[0-9a-f]{64}', sha) or not evidence.get('key', '').endswith('/' + sha + '.bin.gz'):
        raise ValueError('source identity differs')
    acquired, now = clock(source['acquired_at']), clock(generated_at)
    if clock(evidence['first_received_at']) > acquired or acquired > now:
        raise ValueError('invalid acquisition clock')
    return acquired, now, deepcopy(evidence)


def shift_month(day, count):
    n = day.year * 12 + day.month - 1 - count
    y, m = divmod(n, 12)
    return date(y, m + 1, min(day.day, monthrange(y, m + 1)[1]))


def comparison(rows, target, current, tolerance=7):
    eligible = [r for r in rows if r['date'] <= target.isoformat() and (target - date.fromisoformat(r['date'])).days <= tolerance]
    base = eligible[-1] if eligible else None
    return {'target_date': target.isoformat(), 'baseline_date': base['date'] if base else None,
            'baseline_close': base['c'] if base else None, 'current_date': current['date'],
            'current_close': current['c'], 'value': 100 * (current['c'] / base['c'] - 1) if base else None,
            'unit': 'percent', 'method': 'split-adjusted price return; no dividends or costs',
            'baseline_lookback_limit_days': tolerance, 'baseline_source_row': base['source_row'] if base else None}


def equity(source, generated_at):
    acquired, now, evidence = receipt(source, 'polygon', generated_at)
    request, payload = source['request'], source['response']
    symbol = request['symbol']
    if not re.fullmatch(r'[A-Z][A-Z.\-]{0,6}', symbol):
        raise ValueError('unsupported equity symbol')
    start, end = date.fromisoformat(request['start']), date.fromisoformat(request['end'])
    if request != {'symbol': symbol, 'start': start.isoformat(), 'end': end.isoformat(),
                   'multiplier': 1, 'timespan': 'day', 'adjusted': True, 'sort': 'desc', 'limit': 50000}:
        raise ValueError('unsupported aggregate request')
    if start > end or (end-start).days > 400 or end >= acquired.astimezone(ET).date():
        raise ValueError('request includes incomplete aggregate date')
    if payload.get('ticker') != symbol or payload.get('adjusted') is not True or payload.get('status') not in ('OK', 'DELAYED'):
        raise ValueError('response identity or adjustment differs')
    original = payload.get('results') or []
    if payload.get('next_url') or payload.get('resultsCount') != len(original) or not original:
        raise ValueError('incomplete or empty aggregate response')
    rows, seen = [], set()
    for i, raw in enumerate(original):
        t = number(raw['t'], nonnegative=True)
        if int(t) != t:
            raise ValueError('noninteger aggregate timestamp')
        stamp = datetime.fromtimestamp(t/1000, timezone.utc).astimezone(ET)
        day = stamp.date()
        if stamp.hour or stamp.minute or stamp.second or stamp.microsecond or day in seen or not start <= day <= end:
            raise ValueError('duplicate, non-daily or out-of-range aggregate')
        seen.add(day)
        row = {k: number(raw[k], positive=True) for k in ('o', 'h', 'l', 'c')}
        if not row['l'] <= min(row['o'], row['c']) <= max(row['o'], row['c']) <= row['h']:
            raise ValueError('invalid OHLC bounds')
        row.update(date=day.isoformat(), v=optional(raw.get('v'), nonnegative=True), source_row=i,
                   period_start=stamp.isoformat(), period_end=datetime.combine(day+timedelta(days=1), datetime.min.time(), ET).isoformat())
        rows.append(row)
    rows.sort(key=lambda r: r['date'])
    current = rows[-1]
    current_day = date.fromisoformat(current['date'])
    observation_age = (now - clock(current['period_end'])).total_seconds()
    acquisition_age = (now - acquired).total_seconds()
    if observation_age < 0:
        raise ValueError('incomplete current period')
    status = 'fresh' if observation_age <= 96*3600 and acquisition_age <= 26*3600 else 'stale'
    changes = {key: comparison(rows, target, current) for key, target in {
        'week': current_day-timedelta(days=7), 'month': shift_month(current_day, 1),
        'quarter': shift_month(current_day, 3), 'year': shift_month(current_day, 12),
        'ytd': date(current_day.year, 1, 1)-timedelta(days=1)}.items()}
    if len(rows) > 1:
        changes['day'] = comparison(rows, date.fromisoformat(rows[-2]['date']), current, 0)
        changes['day']['method'] += '; previous observed session, not 24-hour return'
    else:
        changes['day'] = comparison(rows, current_day-timedelta(days=1), current, 0)
    closes = [r['c'] for r in rows]
    out = {'instrument_id': 'equity:US:'+symbol, 'symbol': symbol, 'asset_class': 'equity',
           'name': source.get('name') or symbol, 'currency': 'USD', 'unit': 'USD_per_share',
           'price': current['c'] if status == 'fresh' else None, 'last_observed_price': current['c'],
           'date': current['date'], 'period_start': current['period_start'], 'observed_at': current['period_end'], 'acquired_at': source['acquired_at'],
           'identity_scope': 'US provider ticker in this response vintage; not a permanent historical security identifier',
           'source_published_at': None, 'price_kind': 'completed ET daily aggregate close',
           'adjustment': 'split_adjusted_current_retrieved_vintage', 'volume': current['v'], 'volume_unit': 'shares',
           'changes': changes, 'evidence': evidence, 'source_row': current['source_row'], 'request': deepcopy(request),
           'history': [{'d': r['date'], 'c': r['c'], 'source_row': r['source_row']} for r in reversed(rows)],
           'history_scope': {'from': rows[0]['date'], 'through': current['date'], 'observations': len(rows),
                             'session_completeness_verified': False, 'historical_point_in_time': False},
           'observed_window': {'high': max(r['h'] for r in rows), 'low': min(r['l'] for r in rows),
                               'start': rows[0]['date'], 'end': current['date'], 'all_time': False},
           'quality': {'status': status, 'original_source_verified': True, 'evaluated_at': generated_at,
                       'observation_age_seconds': observation_age, 'acquisition_age_seconds': acquisition_age,
                       'maximum_observation_age_seconds': 96*3600, 'maximum_acquisition_age_seconds': 26*3600,
                       'basis': 'completed daily period and acquisition age ceilings; exchange release calendar not verified'},
           'call': None, 'calls_eligible': False, 'sizing_eligible': False}
    for key, value in changes.items():
        out[key+'_pct'] = value['value'] if status == 'fresh' else None
    out.update(open=current['o'], high=current['h'], low=current['l'],
               day_change=current['c']-rows[-2]['c'] if len(rows)>1 and status=='fresh' else None,
               sparkline=closes[-30:], sparkline_dates=[r['date'] for r in rows[-30:]])
    for field in ('w52_high', 'w52_low', 'w52_position', 'rsi14', 'macd', 'macd_signal', 'macd_hist',
                  'support', 'resistance', 'ad_value'):
        out[field] = None
    out['unavailable_measurements'] = '52-week coverage and independent technical definitions are not established; observed-window extrema and dated SMA remain available.'
    for count in (20, 50, 200):
        out['sma'+str(count)] = sum(closes[-count:])/count if len(closes) >= count and status == 'fresh' else None
    out['technical_definitions'] = {'sma': 'arithmetic mean of the last N observed completed daily closes; no signal',
                                    'eligibility': 'descriptive measurements only'}
    return out


def crypto(source, generated_at):
    acquired, now, evidence = receipt(source, 'coingecko', generated_at)
    payload = source['response']
    if not isinstance(payload, list) or not payload or len(payload) > 25:
        raise ValueError('coin market response missing')
    if source.get('request') != {'vs_currency': 'usd', 'order': 'market_cap_desc', 'per_page': 25,
                                 'page': 1, 'sparkline': False, 'price_change_percentage': '1h,24h,7d,30d'}:
        raise ValueError('coin request differs')
    by_id, aliases = {}, {}
    for i, row in enumerate(payload):
        coin = row['id']
        symbol = str(row['symbol']).upper()
        if not re.fullmatch(r'[a-z0-9_-]{1,100}', coin) or coin in by_id or not symbol:
            raise ValueError('invalid or duplicate coin identity')
        observed = clock(row['last_updated'])
        if observed > acquired:
            raise ValueError('future coin observation')
        age = (now-observed).total_seconds()
        status = 'fresh' if age <= 2*3600 and (now-acquired).total_seconds() <= 2*3600 else 'stale'
        price = optional(row.get('current_price'), positive=True)
        if price is None: status = 'missing'
        out = {'instrument_id': 'coingecko:'+coin, 'provider_id': coin, 'symbol': symbol,
               'name': row.get('name'), 'asset_class': 'crypto', 'currency': 'USD', 'unit': 'USD_per_coin',
               'price': price if status == 'fresh' else None, 'last_observed_price': price,
               'observed_at': row['last_updated'], 'acquired_at': source['acquired_at'],
               'source_published_at': None, 'price_kind': 'provider aggregate market price',
               'evidence': deepcopy(evidence), 'source_row': i, 'sparkline': [],
               'quality': {'status': status, 'original_source_verified': True, 'evaluated_at': generated_at,
                           'observation_age_seconds': age, 'maximum_observation_age_seconds': 7200,
                           'maximum_acquisition_age_seconds': 7200},
               'call': None, 'calls_eligible': False, 'sizing_eligible': False}
        for key, raw_key in {'market_cap':'market_cap', 'volume_24h':'total_volume', 'rank':'market_cap_rank',
                              'circulating':'circulating_supply', 'total_supply':'total_supply', 'ath':'ath'}.items():
            out[key] = optional(row.get(raw_key), nonnegative=True)
        out['units'] = {'market_cap': 'USD', 'volume_24h': 'USD', 'circulating': 'coin', 'total_supply': 'coin', 'ath': 'USD_per_coin'}
        out['provider_reported_changes'] = {}
        for horizon in ('1h', '24h', '7d', '30d'):
            value = optional(row.get('price_change_percentage_'+horizon+'_in_currency'))
            if horizon == '24h' and value is None: value = optional(row.get('price_change_percentage_24h'))
            out['change_'+horizon] = value if status == 'fresh' else None
            out['provider_reported_changes'][horizon] = {'value': value, 'unit': 'percent', 'baseline_verified': False}
        out['ath_pct'] = optional(row.get('ath_change_percentage'))
        out['ath_date'] = row.get('ath_date')
        out['image'] = row.get('image')
        out['ath_definition'] = 'provider-reported historical maximum; not independently reconstructed'
        by_id[coin] = out
        aliases.setdefault(symbol, []).append(coin)
    return {'by_id': by_id, 'symbol_aliases': {symbol: ids[0] for symbol, ids in aliases.items() if len(ids) == 1},
            'symbol_collisions': {symbol: ids for symbol, ids in aliases.items() if len(ids) > 1},
            'scope': 'provider top 25 by market cap; not a complete crypto universe'}


def build(sources, generated_at):
    if sources.get('contract') != CONTRACT:
        raise ValueError('market source contract required')
    stocks, errors = {}, deepcopy(sources.get('errors', {}))
    universe = sources.get('universe', [])
    if not isinstance(universe, list) or len(universe) != len(set(universe)):
        raise ValueError('explicit unique equity universe required')
    for symbol in universe:
        if symbol not in sources.get('equities', {}): errors.setdefault(symbol, 'SOURCE_NOT_ACQUIRED')
    for symbol, source in sources.get('equities', {}).items():
        try:
            if symbol not in universe: raise ValueError('symbol outside requested universe')
            row = equity(source, generated_at)
            if row['symbol'] != symbol: raise ValueError('equity map identity differs')
            stocks[symbol] = row
        except (ValueError, KeyError, TypeError, OverflowError):
            errors[symbol] = 'INVALID_SOURCE_RESPONSE'
    coins = {'by_id': {}, 'symbol_aliases': {}, 'symbol_collisions': {}}
    if sources.get('crypto'):
        try: coins = crypto(sources['crypto'], generated_at)
        except (ValueError, KeyError, TypeError, OverflowError): errors['crypto'] = 'INVALID_SOURCE_RESPONSE'
    else:
        errors.setdefault('crypto', 'SOURCE_NOT_ACQUIRED')
    all_rows = list(stocks.values()) + list(coins['by_id'].values())
    fresh_count = sum(row['quality']['status'] == 'fresh' for row in all_rows)
    return {'stocks': stocks, 'crypto_by_id': coins['by_id'],
            'crypto': {symbol: deepcopy(coins['by_id'][coin]) for symbol, coin in coins['symbol_aliases'].items()},
            'crypto_symbol_collisions': coins['symbol_collisions'],
            'quality': {'status': 'degraded' if errors or fresh_count != len(all_rows) or not all_rows else 'measured', 'errors': errors,
                        'fresh_observations': fresh_count,
                        'equity_universe': deepcopy(sources.get('universe', [])), 'equities_compiled': len(stocks),
                        'crypto_compiled': len(coins['by_id']),
                        'source_originals_required': True, 'calls_eligible': False, 'sizing_eligible': False}}
