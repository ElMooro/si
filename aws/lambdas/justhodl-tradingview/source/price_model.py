"""Provider-native price observations. Descriptive, never portfolio instructions."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, localcontext
import hashlib
import json
import math
import re
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo
import daily_market_model as daily

CONTRACT = 'native-market-observation.v1'
PREFIX = 'data/price-observations/'
MAX_AGE = 96*3600


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')


def digest(raw): return hashlib.sha256(raw).hexdigest()


def clock(value): return daily.clock(value)


def number(value):
    if value is None: return None
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError('numeric source observation required')
    result = Decimal(str(value))
    if not result.is_finite() or not math.isfinite(float(result)) or (result != 0 and float(result) == 0):
        raise ValueError('nonfinite or unrepresentable observation')
    return result


def epoch(value):
    parsed = number(value)
    if parsed is None or parsed <= 0 or int(parsed) != parsed: raise ValueError('integer source timestamp required')
    return datetime.fromtimestamp(int(parsed), timezone.utc)


def source_url(provider, symbol, kind, request):
    if not re.fullmatch(r'[A-Za-z0-9.^=_!\-]{1,50}', symbol): raise ValueError('invalid provider symbol')
    if provider == 'yahoo' and kind == 'bars' and request == {'range':'5d','interval':'1d'}:
        return 'https://query1.finance.yahoo.com/v8/finance/chart/'+quote(symbol,safe='')+'?range=5d&interval=1d'
    if provider == 'fmp' and kind == 'quote':
        symbols = request.get('symbols')
        if set(request) != {'symbols'} or not isinstance(symbols,list) or not 1 <= len(symbols) <= 40 or len(set(symbols)) != len(symbols) or symbol not in symbols:
            raise ValueError('explicit unique quote batch required')
        if any(not re.fullmatch(r'[A-Za-z0-9.^=_!\-]{1,50}', s) for s in symbols): raise ValueError('invalid batch symbol')
        return 'https://financialmodelingprep.com/stable/batch-quote?'+urlencode({'symbols':','.join(symbols)})
    if provider == 'fmp' and kind == 'profile' and request == {'symbol':symbol}:
        return 'https://financialmodelingprep.com/stable/profile?'+urlencode(request)
    if provider == 'polygon' and kind == 'bars':
        return f'https://api.polygon.io/v2/aggs/ticker/{quote(symbol,safe="")}/range/1/day/{request["start"]}/{request["end"]}?sort=desc&limit=50000'
    if provider == 'polygon' and kind == 'previous' and request == {'adjusted':True}:
        return f'https://api.polygon.io/v2/aggs/ticker/{quote(symbol,safe="")}/prev'
    raise ValueError('unsupported source request')


def original(provider, symbol, kind, source, compiled_at):
    acquired, now, evidence = daily.receipt(source, provider, compiled_at)
    url = source_url(provider,symbol,kind,source['request'])
    if evidence.get('source_url') != url: raise ValueError('source request identity differs')
    if evidence['key'] != f'data/evidence/{provider}/{digest(url.encode())}/{evidence["sha256"]}.bin.gz':
        raise ValueError('source key identity differs')
    return source['response'], acquired


def pair(current, previous):
    current, previous = number(current), number(previous)
    with localcontext() as ctx:
        ctx.prec = 50
        delta = current-previous if current is not None and previous is not None else None
        percent = delta/previous*100 if delta is not None and previous > 0 else None
    for value in (delta,percent): number(value)
    return {'value':float(current) if current is not None else None,
            'prev':float(previous) if previous is not None else None,
            'change':float(delta) if delta is not None else None,
            'chg_pct':float(percent) if percent is not None else None,
            'exact':{k:str(v) if v is not None else None for k,v in
                     (('value',current),('previous',previous),('change',delta),('change_percent',percent))}}


def yahoo(symbol, sources, compiled_at):
    response, acquired = original('yahoo',symbol,'bars',sources['bars'],compiled_at)
    chart = response['chart']
    if chart.get('error') or not isinstance(chart.get('result'),list) or len(chart['result']) != 1:
        raise ValueError('one chart result required')
    result = chart['result'][0]; meta = result['meta']
    if meta.get('symbol') != symbol or meta.get('dataGranularity') != '1d' or meta.get('range') != '5d':
        raise ValueError('chart identity or interval differs')
    zone = ZoneInfo(meta['exchangeTimezoneName'])
    stamps = result.get('timestamp'); quotes = result['indicators']['quote']
    if not isinstance(stamps,list) or not 1 <= len(stamps) <= 10 or not isinstance(quotes,list) or len(quotes) != 1:
        raise ValueError('bounded daily chart required')
    closes = quotes[0]['close']
    if not isinstance(closes,list) or len(closes) != len(stamps): raise ValueError('unaligned price/timestamp arrays')
    dates = [epoch(t) for t in stamps]
    if dates != sorted(set(dates)) or any(t > acquired for t in dates): raise ValueError('duplicate or future bar start')
    for value in closes: number(value)
    kind = meta.get('instrumentType')
    currency = meta.get('currency')
    if kind not in ('INDEX','EQUITY','ETF','MUTUALFUND','FUTURE','CURRENCY','CRYPTOCURRENCY'):
        raise ValueError('unreviewed provider instrument type')
    if not isinstance(currency,str) or not re.fullmatch(r'[A-Za-z]{3,5}',currency): raise ValueError('native currency required')
    if kind in ('EQUITY','ETF','MUTUALFUND','CRYPTOCURRENCY') and any(v is not None and number(v) <= 0 for v in closes):
        raise ValueError('nonpositive security/crypto price')
    unit = 'index_points' if kind == 'INDEX' else currency+'_per_share' if kind in ('EQUITY','ETF') else 'provider_quoted_units'
    current = dates[-1]; prior = dates[-2] if len(dates)>1 else None
    regular = epoch(meta['regularMarketTime']) if meta.get('regularMarketTime') is not None else None
    if regular and regular > acquired: raise ValueError('future regular-market timestamp')
    name = meta.get('longName') or meta.get('shortName')
    # The provider currently labels ^MOVE as a different index. Preserve it, flag the conflict.
    conflict = symbol == '^MOVE' and not re.search(r'\bMOVE\b|Merrill.*Option.*Volatility',str(name),re.I)
    return {**pair(closes[-1],closes[-2] if len(closes)>1 else None),
        'definition':name, 'instrument_type':kind, 'exchange':meta.get('fullExchangeName') or meta.get('exchangeName'),
        'currency':currency, 'unit':unit, 'provider_exchange_timezone':meta['exchangeTimezoneName'],
        'asof':current.astimezone(zone).date().isoformat(), 'observation_date':current.astimezone(zone).date().isoformat(),
        'previous_observation_date':prior.astimezone(zone).date().isoformat() if prior else None,
        'observation_period_start':current.isoformat(), 'previous_period_start':prior.isoformat() if prior else None,
        'observed_at':None, 'provider_regular_market_at':regular.isoformat() if regular else None,
        'age_reference_at':current.isoformat(), 'age_basis':'provider daily bar START; final close time is unverified',
        'comparison_basis':'adjacent provider daily bars, including missing values; latest bar may be in progress',
        'price_kind':'provider daily bar close; finality unverified', 'period_completed_verified':False,
        'adjustment':'provider chart close; corporate-action and futures-roll conventions unverified',
        'identity_conflict':conflict,
        'source_window':[{'period_start':t.isoformat(),'close':float(number(v)) if v is not None else None} for t,v in zip(dates,closes)],
        'contract_specification_verified':False}


def fmp(symbol, sources, compiled_at):
    quotes, acquired = original('fmp',symbol,'quote',sources['quote'],compiled_at)
    profiles, _ = original('fmp',symbol,'profile',sources['profile'],compiled_at)
    if not isinstance(quotes,list) or len({q['symbol'] for q in quotes}) != len(quotes): raise ValueError('duplicate quote identity')
    if any(q['symbol'] not in sources['quote']['request']['symbols'] for q in quotes): raise ValueError('unrequested quote')
    matches = [q for q in quotes if q['symbol'] == symbol]
    if len(matches) != 1 or not isinstance(profiles,list) or len(profiles) != 1 or profiles[0].get('symbol') != symbol:
        raise ValueError('one exact quote and profile required')
    q, p = matches[0], profiles[0]
    if not isinstance(p.get('currency'),str) or not re.fullmatch(r'[A-Za-z]{3,5}',p['currency']): raise ValueError('currency definition unavailable')
    if not q.get('exchange') or q['exchange'] != p.get('exchange'): raise ValueError('quote/profile exchange differs')
    observed = epoch(q['timestamp'])
    if observed > acquired: raise ValueError('future quote timestamp')
    for key in ('price','previousClose'):
        if number(q.get(key)) is not None and number(q[key]) <= 0: raise ValueError('nonpositive security price')
    return {**pair(q.get('price'),q.get('previousClose')), 'definition':p.get('companyName') or q.get('name'),
        'instrument_type':'ETF' if p.get('isEtf') is True else 'FUND' if p.get('isFund') is True else 'EQUITY',
        'exchange':q['exchange'], 'currency':p['currency'], 'unit':p['currency']+'_per_share',
        'provider_identifiers':{k:p.get(k) for k in ('isin','cusip','cik')},
        'asof':observed.isoformat(), 'observation_date':observed.date().isoformat(), 'observation_date_timezone':'UTC',
        'observed_at':observed.isoformat(), 'previous_observation_date':None,
        'age_reference_at':observed.isoformat(), 'age_basis':'provider quote timestamp',
        'comparison_basis':'provider previousClose; baseline date not supplied or inferred',
        'provider_reported_change_percent':float(number(q['changePercentage'])) if q.get('changePercentage') is not None else None,
        'price_kind':'provider quote, not an executable fill', 'period_completed_verified':False,
        'adjustment':'provider previousClose; corporate-action convention unverified', 'identity_conflict':False}


def polygon(symbol, sources, compiled_at):
    payload, acquired = original('polygon',symbol,'bars',sources['bars'],compiled_at)
    request = sources['bars']['request']
    start_day,end_day = date.fromisoformat(request['start']),date.fromisoformat(request['end'])
    if request != {'symbol':symbol,'start':start_day.isoformat(),'end':end_day.isoformat(),
                   'multiplier':1,'timespan':'day','adjusted':True,'sort':'desc','limit':50000} or not re.fullmatch(r'[A-Z][A-Z.\-]{0,6}',symbol):
        raise ValueError('reviewed US daily aggregate request required')
    if start_day>end_day or (end_day-start_day).days>400 or end_day>=acquired.astimezone(daily.ET).date():
        raise ValueError('invalid or incomplete current aggregate window')
    if 'previous' in sources:
        if payload.get('ticker') != symbol or payload.get('adjusted') is not True or payload.get('resultsCount') != 0 or payload.get('results') or payload.get('next_url'):
            raise ValueError('previous fallback requires an explicitly empty current window')
        old, acquired = original('polygon',symbol,'previous',sources['previous'],compiled_at)
        rows = old.get('results')
        if old.get('ticker') != symbol or old.get('adjusted') is not True or old.get('status') not in ('OK','DELAYED') or old.get('resultsCount') != 1 or not isinstance(rows,list) or len(rows) != 1:
            raise ValueError('one previous aggregate required')
        bar = rows[0]
        millis = number(bar['t'])
        if millis is None or millis <= 0 or int(millis) != millis: raise ValueError('integer aggregate timestamp required')
        start = datetime.fromtimestamp(int(millis)/1000,timezone.utc).astimezone(daily.ET)
        if start > acquired: raise ValueError('future previous aggregate timestamp')
        values = {k:number(bar[k]) for k in ('o','h','l','c')}
        if any(v is None or v<=0 for v in values.values()) or not values['l']<=min(values['o'],values['c'])<=max(values['o'],values['c'])<=values['h']:
            raise ValueError('invalid previous OHLC')
        return {**pair(bar['c'],None),'definition':symbol,'instrument_type':'US_CASH_SECURITY','exchange':None,
            'currency':'USD','unit':'USD_per_share','asof':start.date().isoformat(),'observation_date':start.date().isoformat(),
            'previous_observation_date':None,'observed_at':None,'observation_period_start':start.isoformat(),
            'age_reference_at':start.isoformat(),'age_basis':'provider previous-aggregate timestamp; historical period boundary unverified',
            'comparison_basis':'no previous close available; session open is not a comparison baseline',
            'price_kind':'last available previous aggregate; requested current window was empty',
            'adjustment':'split_adjusted_current_retrieved_vintage','period_completed_verified':False,
            'previous_aggregate_scope_unverified':True,
            'source_window':[{'date':start.date().isoformat(),'c':bar['c']}],'identity_conflict':False}
    # Reuse the reviewed completed-ET-aggregate compiler; its bytes are pinned with this wrapper.
    native = daily.equity(sources['bars'],compiled_at)
    baseline = native['changes']['day']
    return {**pair(native['last_observed_price'],baseline['baseline_close']),
        'definition':symbol, 'instrument_type':'US_CASH_SECURITY', 'exchange':None,
        'currency':'USD', 'unit':native['unit'], 'asof':native['date'], 'observation_date':native['date'],
        'previous_observation_date':baseline['baseline_date'], 'observed_at':native['observed_at'],
        'age_reference_at':native['observed_at'], 'age_basis':'completed ET daily aggregate period end',
        'observation_period_start':native['period_start'],
        'comparison_basis':'previous observed completed ET daily close, not session open or 24-hour return',
        'price_kind':native['price_kind'], 'adjustment':native['adjustment'], 'period_completed_verified':True,
        'source_window':native['history'], 'identity_conflict':False}


def compile_observation(provider, symbol, sources, compiled_at):
    expected = {'yahoo':{'bars'},'fmp':{'quote','profile'},'polygon':{'bars'}}
    if provider not in expected or (set(sources) != expected[provider] and not (provider=='polygon' and set(sources)=={'bars','previous'})):
        raise ValueError('exact source set required')
    out = {'yahoo':yahoo,'fmp':fmp,'polygon':polygon}[provider](symbol,sources,compiled_at)
    age = (clock(compiled_at)-clock(out['age_reference_at'])).total_seconds()
    if age < 0: raise ValueError('future source observation')
    status = 'identity_conflict' if out['identity_conflict'] else 'missing_latest' if out['value'] is None else 'stale' if age > MAX_AGE else 'period_scope_unverified' if out.get('previous_aggregate_scope_unverified') else 'within_age_ceiling'
    acquired = max(clock(s['acquired_at']) for s in sources.values()).isoformat()
    return {**out, 'contract_version':CONTRACT, 'provider':provider, 'provider_symbol':symbol,
        'instrument_id':provider+':'+symbol, 'identity_scope':'provider symbol in this response vintage; no permanent historical identifier qualification',
        'source':provider+':'+symbol, 'resolved_via':('poly' if provider=='polygon' else provider)+':'+symbol,
        'fetched_at':acquired, 'calculated_at':compiled_at, 'published_at':None, 'frequency':'provider market observation',
        'seasonal_adjustment':'not applicable', 'change_unit':out['unit'],
        'quality':{'status':status,'observation_age_seconds':age,'max_observation_age_seconds':MAX_AGE,
                   'release_calendar_verified':False,'historical_availability_verified':False},
        'status':'LIVE' if status=='within_age_ceiling' else 'MAPPING_REVIEW' if status=='identity_conflict' else 'STALE' if out['value'] is not None else 'PENDING_RESOLUTION',
        'comparison_eligible':False, 'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}


def identity_for(row, aliases):
    symbol = row.get('symbol')
    alias = aliases.get(symbol)
    candidate = alias if alias is not None else row.get('resolved_via') or row.get('source') or ''
    if candidate == 'fmp': return 'fmp',symbol
    match = re.fullmatch(r'(yahoo|poly|polygon|fmp):([A-Za-z0-9.^=_!\-]{1,50})',candidate)
    if match: return ('polygon' if match[1]=='poly' else match[1]),match[2]
    return None


def merge_observation(row, observation, aliases):
    if row.get('contract_version') != CONTRACT and row.get('value') is not None:
        row['superseded_legacy_measurement'] = {k:row.get(k) for k in ('value','prev','source','asof','fetched_at')}
    row.update(observation); row['cached'] = False
    curated = aliases.get(row.get('symbol'))
    mismatch = ('ECONOMICS' in (row.get('exchanges') or []) or row.get('category')=='macro') and not curated
    row['mapping'] = {'basis':'curated provider mapping' if curated else 'inherited provider mapping; semantic review incomplete',
                      'display_symbol':row.get('symbol'),'provider_symbol':observation['provider_symbol'],
                      'proxy_possible':row.get('symbol')!=observation['provider_symbol'], 'semantic_match_verified':False}
    if mismatch:
        row.update(status='MAPPING_REVIEW',resolution_note='Economics/macro identifier resolved to a market instrument; excluded pending semantic mapping review.')
        row['quality'] = {**row['quality'],'status':'mapping_conflict'}
    else: row['resolution_note']='Original market response and compiler retained. Price observations are not trade recommendations.'


def mark_unavailable(row, identity):
    row.update(status='STALE' if row.get('value') is not None else 'PENDING_RESOLUTION',cached=True,
        expected_price_identity=':'.join(identity),calls_eligible=False,sizing_eligible=False,execution_eligible=False,comparison_eligible=False,
        resolution_note='Native price refresh unavailable; prior values and source clocks retained, not a current quote.')
    row['quality']={'status':'refresh_unavailable','release_calendar_verified':False}
