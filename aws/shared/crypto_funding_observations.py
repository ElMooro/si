"""Traceable, descriptive funding observations; no annual yield or investment vote.

The existing producer owns collection. Consumer projection never performs transport.
"""
from base64 import b64encode
from datetime import datetime,timedelta,timezone
from decimal import Decimal,InvalidOperation,localcontext
import hashlib,json,math,re

CONTRACT='crypto-reported-funding-point.v1'
DENIED={'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'forecast_qualified':False,'independent_investment_votes':0}

def number(token):
    if not isinstance(token,str) or len(token)>128 or not re.fullmatch(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?',token):return None
    try:
        decimal=Decimal(token);value=float(decimal)
        if not math.isfinite(value) or Decimal(str(value))!=decimal:return None
        return decimal
    except (InvalidOperation,ValueError,OverflowError):return None

def stamp(token):
    if not isinstance(token,str) or not re.fullmatch('[0-9]{1,15}',token):return None
    try:
        return (datetime(1970,1,1,tzinfo=timezone.utc)+timedelta(milliseconds=int(token))).isoformat()
    except (OverflowError,OSError,ValueError):return None

def pairs(items):
    result={}
    for key,value in items:
        if key in result:raise ValueError('Duplicate response member')
        result[key]=value
    return result

def point(provider,instrument,body):
    if provider not in ('okx','bybit') or not isinstance(instrument,str) or not isinstance(body,bytes):raise ValueError('Exact source identity and received bytes required')
    result={'contract':CONTRACT,'provider':provider,'instrument':instrument,'status':'unavailable','funding_rate':None,'funding_rate_decimal':None,'funding_rate_pct':None,'funding_rate_pct_decimal':None,
            'unit':'ratio_per_reported_funding_event','reported_settlement_at':None,'reported_next_settlement_at':None,'reported_source_timestamp':None,'reported_schedule_difference_ms':None,
            'annualized_pct':None,'annualization_status':'unavailable_unqualified_settlement_basis','reason':'unvalidated_response','rate_basis':'current_endpoint_rate' if provider=='okx' else 'last_settled_history_rate',
            'original_response_base64':b64encode(body).decode(),'original_response_sha256':hashlib.sha256(body).hexdigest(),'original_response_bytes':len(body),'source_timing_qualified':False,'cross_contract_comparability_verified':False,**DENIED}
    def unavailable(reason):result['reason']=reason;return result
    if len(body)>1_000_000:return unavailable('oversized_complete_response')
    try:
        doc=json.loads(body.decode('utf-8'),object_pairs_hook=pairs,parse_constant=lambda token:(_ for _ in ()).throw(ValueError('Nonfinite JSON token')))
    except (ValueError,UnicodeError,RecursionError):return unavailable('invalid_whole_response_json')
    if not isinstance(doc,dict):return unavailable('response_not_object')
    if provider=='okx':
        if doc.get('code')!='0':return unavailable('reported_source_failure')
        rows=doc.get('data');key='instId';event='fundingTime';clock='ts'
    else:
        if type(doc.get('retCode')) is not int or doc['retCode']!=0:return unavailable('reported_source_failure')
        wrapper=doc.get('result')
        if not isinstance(wrapper,dict) or wrapper.get('category')!='linear':return unavailable('wrong_contract_category')
        rows=wrapper.get('list');key='symbol';event='fundingRateTimestamp';clock=None
    if not isinstance(rows,list) or len(rows)!=1 or not isinstance(rows[0],dict):return unavailable('expected_exactly_one_requested_observation')
    row=rows[0]
    if row.get(key)!=instrument:return unavailable('returned_instrument_mismatch')
    rate=number(row.get('fundingRate'))
    if rate is None:return unavailable('missing_invalid_or_lossy_funding_rate')
    with localcontext() as ctx:
        ctx.prec=160;percent=rate*100
    projected=float(percent)
    if not math.isfinite(projected) or Decimal(str(projected))!=percent:return unavailable('unrepresentable_percent_projection')
    if stamp(row.get(event)) is None:return unavailable('missing_or_invalid_settlement_clock')
    result.update(status='descriptive',reason='reported_event_only_not_an_annual_yield',funding_rate=float(rate),funding_rate_decimal=str(rate),funding_rate_pct=projected,funding_rate_pct_decimal=str(percent),reported_settlement_at=stamp(row[event]),
                  funding_payment_direction='longs_pay_shorts' if rate>0 else 'shorts_pay_longs' if rate<0 else 'zero_reported_rate')
    if provider=='okx':
        result['reported_source_timestamp']=stamp(row.get(clock))
        result['reported_next_settlement_at']=stamp(row.get('nextFundingTime'))
        if result['reported_next_settlement_at'] is not None and int(row['nextFundingTime'])>int(row[event]):result['reported_schedule_difference_ms']=int(row['nextFundingTime'])-int(row[event])
        result['reported_method']=row.get('method') if isinstance(row.get('method'),str) else None
    elif type(doc.get('time')) is int:result['reported_source_timestamp']=stamp(str(doc['time']))
    return result


PACKET_CONTRACT = 'crypto-reported-funding.v1'
MAX_RESPONSE_BYTES = 1_000_000
SYMBOLS = ('BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'ADA', 'AVAX', 'LINK', 'DOT', 'BNB')
POINT_FIELDS = ('contract', 'provider', 'instrument', 'status', 'reason', 'unit',
    'funding_rate', 'funding_rate_decimal', 'funding_rate_pct', 'funding_rate_pct_decimal',
    'funding_payment_direction', 'reported_settlement_at', 'reported_next_settlement_at',
    'reported_source_timestamp', 'reported_schedule_difference_ms', 'rate_basis',
    'annualized_pct', 'annualization_status', 'source_timing_qualified',
    'cross_contract_comparability_verified', 'original_response_sha256') + tuple(DENIED)


def read_complete_body(response):
    """Bound memory; a too-large prefix is never called a complete original."""
    chunks, total = [], 0
    while total <= MAX_RESPONSE_BYTES:
        try:
            chunk = response.read(min(65536, MAX_RESPONSE_BYTES + 1 - total))
        except Exception as exc:
            partial = getattr(exc, 'partial', b'')
            if isinstance(partial, bytes):
                chunks.append(partial[:MAX_RESPONSE_BYTES + 1 - total])
            return b''.join(chunks), False, type(exc).__name__
        if not isinstance(chunk, bytes):
            raise ValueError('Response body must be bytes')
        if not chunk:
            return b''.join(chunks), True, None
        chunks.append(chunk); total += len(chunk)
    return b''.join(chunks), False, 'response_exceeds_capture_limit'


def acquire(provider, instrument, opener, now):
    """Only called by the existing producer; no retries or extra endpoints."""
    from urllib.request import Request
    from urllib.error import HTTPError
    url = ('https://www.okx.com/api/v5/public/funding-rate?instId=' + instrument
           if provider == 'okx' else
           'https://api.bybit.com/v5/market/funding/history?category=linear&symbol=' + instrument + '&limit=1')
    received, complete, status, failure = b'', False, None, None
    started = now().isoformat()
    try:
        try:
            response = opener(Request(url, headers={'User-Agent': 'JustHodl/2.0', 'Accept': 'application/json'}), timeout=8)
        except HTTPError as exc:
            response = exc
        with response:
            status = response.status if hasattr(response, 'status') else response.code
            received, complete, failure = read_complete_body(response)
            length = response.headers.get('Content-Length') if response.headers else None
            if complete and length is not None:
                if not isinstance(length, str) or not re.fullmatch('[0-9]+', length) or int(length) != len(received):
                    complete = False; failure = 'declared_content_length_mismatch'
    except Exception as exc:
        failure = type(exc).__name__
    finished = now().isoformat()
    out = point(provider, instrument, received if complete else b'')
    out.update(request_url=url, acquired_started_at=started, acquired_completed_at=finished,
               http_status=status if type(status) is int else None,
               response_complete=complete, transport_error=failure)
    if not complete:
        for key in ('original_response_base64', 'original_response_sha256', 'original_response_bytes'):
            out[key] = None
        out.update(status='unavailable', reason=failure or 'response_exceeds_capture_limit',
                   received_prefix_base64=b64encode(received).decode(), received_prefix_bytes=len(received),
                   received_prefix_sha256=hashlib.sha256(received).hexdigest())
    elif status != 200 or type(status) is not int:
        out.update(status='unavailable', reason='http_status_not_200', funding_rate=None,
                   funding_rate_decimal=None, funding_rate_pct=None, funding_rate_pct_decimal=None)
    return out


def collect_funding(opener, now=None):
    """Publish a fixed-universe observation ledger, never an unequal-period mean."""
    now = now or (lambda: datetime.now(timezone.utc))
    attempts = []
    selected = []
    for provider in ('okx', 'bybit'):
        selected = []
        for symbol in SYMBOLS:
            instrument = symbol + ('-USDT-SWAP' if provider == 'okx' else 'USDT')
            evidence = acquire(provider, instrument, opener, now)
            index = len(attempts); attempts.append(evidence)
            row = {key: evidence.get(key) for key in POINT_FIELDS}
            row.update(symbol=symbol, evidence_index=index, sentiment='UNAVAILABLE')
            selected.append(row)
        if any(row['status'] == 'descriptive' for row in selected):
            break  # Preserve the existing all-primary-unavailable fallback policy.
    observed = sum(row['status'] == 'descriptive' for row in selected)
    return {'contract': PACKET_CONTRACT, 'status': 'descriptive' if observed else 'unavailable',
            'rates': selected, 'source_attempts': attempts, 'selected_provider': provider,
            'expected_instruments': len(SYMBOLS), 'observed_instruments': observed,
            'unavailable_instruments': len(SYMBOLS)-observed,
            'coverage_basis': 'requested_fixed_universe_not_global_market',
            'avg_rate_pct': None, 'avg_funding': None, 'leverage_sentiment': 'UNAVAILABLE',
            'bias': 'unknown', 'long_count': None, 'short_count': None,
            'positive_count': None, 'negative_count': None,
            'most_longed': [], 'most_shorted': [],
            'reason': 'settlement_basis_and_cross_contract_comparability_unqualified',
            'source_timing_qualified': False, 'cross_contract_comparability_verified': False,
            'annualization_qualified': False, **DENIED}


def denied_contract(value):
    return isinstance(value, dict) and all(type(value.get(k)) is type(v) and value.get(k) == v for k, v in DENIED.items())


def reported_point(row):
    """Project only typed, internally consistent observations; no legacy fallback.

    This is a consumer schema check, not proof of authentic or current source data.
    """
    if not denied_contract(row) or row.get('contract') != CONTRACT or row.get('status') != 'descriptive':
        return None
    provider, symbol = row.get('provider'), row.get('symbol')
    if provider not in ('okx', 'bybit') or symbol not in SYMBOLS:
        return None
    if row.get('instrument') != symbol + ('-USDT-SWAP' if provider == 'okx' else 'USDT'):
        return None
    if row.get('unit') != 'ratio_per_reported_funding_event' or row.get('annualized_pct') is not None:
        return None
    basis = 'current_endpoint_rate' if provider == 'okx' else 'last_settled_history_rate'
    if row.get('rate_basis') != basis or row.get('source_timing_qualified') is not False or row.get('cross_contract_comparability_verified') is not False:
        return None
    rate, percent = number(row.get('funding_rate_decimal')), number(row.get('funding_rate_pct_decimal'))
    if rate is None or percent is None:
        return None
    with localcontext() as ctx:
        ctx.prec = 160
        if rate * 100 != percent:
            return None
    for key, decimal in [('funding_rate', rate), ('funding_rate_pct', percent)]:
        value = row.get(key)
        if type(value) not in (int, float) or not math.isfinite(value) or Decimal(str(value)) != decimal:
            return None
    try:
        clock = datetime.fromisoformat(row['reported_settlement_at'])
        if clock.tzinfo is None or clock.utcoffset() != timedelta(0):
            return None
    except (KeyError, TypeError, ValueError, OverflowError):
        return None
    direction = 'longs_pay_shorts' if rate > 0 else 'shorts_pay_longs' if rate < 0 else 'zero_reported_rate'
    if row.get('funding_payment_direction') != direction:
        return None
    return {key: row.get(key) for key in POINT_FIELDS + ('symbol', 'evidence_index')}


def funding_context(packet):
    """Keep context concise without promoting provider rates into investment votes."""
    rows = []
    if denied_contract(packet) and packet.get('contract') == PACKET_CONTRACT and isinstance(packet.get('rates'), list):
        projected = [reported_point(row) for row in packet['rates']]
        rows = [row for row in projected if row is not None]
        identities = [(row['provider'], row['instrument']) for row in rows]
        if len(set(identities)) != len(identities):
            rows = []
    return {'contract': PACKET_CONTRACT, 'rates': rows,
            'status': 'descriptive' if rows else 'unavailable',
            'note': 'Per reported event only; no comparable market mean, annual yield or positioning conclusion.',
            'annualized_pct': None, 'leverage_sentiment': 'UNAVAILABLE', **DENIED}


def funding_line(row):
    value = reported_point(row)
    if value is None:
        return 'Funding observation unavailable; no annual yield or investment vote.'
    return (value['symbol'] + ' ' + value['provider'].upper() + ': ' + value['funding_rate_pct_decimal'] +
            '% per reported event; ' + value['funding_payment_direction'].replace('_', ' ') +
            '; ' + value['rate_basis'].replace('_', ' ') + ' at ' + value['reported_settlement_at'] +
            '; annualization and freshness unqualified.')


def apply_funding_risk_boundary(output, packet):
    """No aggregate risk action when a required funding input lacks a valid basis."""
    from copy import deepcopy
    out = deepcopy(output)
    out['unqualified_legacy_risk'] = deepcopy(output)
    out.update(score=None, regime='UNAVAILABLE', action='WAIT', signals=[],
               decision={'verb': 'WAIT', 'meaning': 'abstain'},
               quality={'status': 'unqualified_required_input', 'source': 'crypto-reported-funding',
                        'preceding_quality': output.get('quality')},
               funding_context=funding_context(packet),
               reason='Comparable funding periods and independent risk calibration are unqualified.', **DENIED)
    return out
