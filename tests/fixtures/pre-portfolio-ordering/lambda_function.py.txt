"""Private, reproducible portfolio holdings risk. No invented risk or sizing defaults."""
from private_artifact import publish_private, private_http_denied
from instrument_identity import resolve_instrument
import base64
import hashlib
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from portfolio_risk_model import VERSION, ARCHIVE_PREFIX, canonical, freeze, replay, read_bars, snapshot_value_identity, MAX_SOURCE_BYTES, source_document

S3_BUCKET = "justhodl-dashboard-live"
SNAPSHOT_KEY = "portfolio/snapshot.json"
RISK_KEY = "portfolio/risk.json"
ALERT_HISTORY_KEY = "portfolio/risk-alert-history.json"
POLY_KEY = os.environ.get("POLY_KEY", "")
s3 = boto3.client("s3", region_name="us-east-1")

# Retained legacy assumptions, now explicitly hypothetical and uncalibrated.
# No historical empirical claim is made by these constants.
SCENARIOS = {
    "covid_2020": {
        "name": "COVID-2020 (Feb 19 → Mar 23, 2020)",
        "duration_days": 33,
        "spy_return": -0.339,
        "sector_returns": {
            "Technology":            -0.265,
            "Healthcare":            -0.235,
            "Consumer Defensive":    -0.176,
            "Utilities":             -0.339,
            "Consumer Cyclical":     -0.367,
            "Communication Services": -0.249,
            "Industrials":           -0.402,
            "Basic Materials":       -0.348,
            "Real Estate":           -0.422,
            "Financial Services":    -0.434,
            "Energy":                 -0.564,
        },
    },
    "inflation_2022": {
        "name": "2022 Inflation Bear (Jan 4 → Oct 12, 2022)",
        "duration_days": 281,
        "spy_return": -0.252,
        "sector_returns": {
            "Technology":            -0.342,
            "Communication Services": -0.401,
            "Consumer Cyclical":     -0.385,
            "Real Estate":           -0.305,
            "Healthcare":            -0.057,
            "Financial Services":    -0.180,
            "Industrials":           -0.160,
            "Consumer Defensive":     -0.061,
            "Utilities":              0.020,
            "Basic Materials":       -0.187,
            "Energy":                  0.426,
        },
    },
    "q4_2018_volcrisis": {
        "name": "Q4 2018 Vol Crisis (Oct 3 → Dec 24, 2018)",
        "duration_days": 82,
        "spy_return": -0.195,
        "sector_returns": {
            "Technology":            -0.236,
            "Energy":                 -0.276,
            "Industrials":           -0.225,
            "Consumer Cyclical":     -0.234,
            "Financial Services":    -0.189,
            "Healthcare":            -0.108,
            "Communication Services": -0.169,
            "Utilities":              0.019,
            "Consumer Defensive":     -0.041,
            "Real Estate":           -0.064,
            "Basic Materials":       -0.193,
        },
    },
    "gfc_2008": {
        "name": "GFC 2008 (Oct 9, 2007 → Mar 9, 2009)",
        "duration_days": 517,
        "spy_return": -0.566,
        "sector_returns": {
            "Technology":            -0.530,
            "Financial Services":    -0.823,
            "Real Estate":           -0.710,
            "Industrials":           -0.626,
            "Consumer Cyclical":     -0.555,
            "Energy":                 -0.524,
            "Basic Materials":       -0.594,
            "Communication Services": -0.487,
            "Healthcare":            -0.379,
            "Consumer Defensive":     -0.288,
            "Utilities":             -0.452,
        },
    },
    "dotcom_2000": {
        "name": "Dot-Com Bust (Mar 24, 2000 → Oct 9, 2002)",
        "duration_days": 929,
        "spy_return": -0.491,
        "sector_returns": {
            "Technology":            -0.778,
            "Communication Services": -0.658,
            "Consumer Cyclical":     -0.376,
            "Healthcare":            -0.158,
            "Financial Services":    -0.301,
            "Industrials":           -0.297,
            "Energy":                 -0.149,
            "Basic Materials":       -0.157,
            "Real Estate":            0.149,
            "Consumer Defensive":      0.099,
            "Utilities":             -0.279,
        },
    },
}


def read_provider_response(response, deadline):
    """Complete HTTP message with a byte bound and elapsed-time acceptance gate.

    read1 returns available socket data instead of waiting to fill a large read.
    The existing twenty-second socket timeout still bounds a blocked read; the
    elapsed-time gate never accepts a body that finishes after the deadline.
    """
    if response.status != 200:
        raise ValueError('Complete provider HTTP response required')
    lengths = response.headers.get_all('Content-Length') or []
    if len(lengths) > 1:
        raise ValueError('Ambiguous provider response length')
    declared = None
    if lengths:
        text = lengths[0]
        if type(text) is not str:
            raise ValueError('Invalid provider response length')
        text = text.strip(' \t')
        if not text or any(c not in '0123456789' for c in text):
            raise ValueError('Invalid provider response length')
        declared = int(text)
        if declared > MAX_SOURCE_BYTES:
            raise ValueError('Provider response exceeds complete byte bound')
    parts, size = [], 0
    while True:
        if time.monotonic() >= deadline:
            raise TimeoutError('Provider response acceptance deadline exceeded')
        part = response.read1(min(65536, MAX_SOURCE_BYTES + 1 - size))
        if time.monotonic() >= deadline:
            raise TimeoutError('Provider response acceptance deadline exceeded')
        if type(part) is not bytes:
            raise ValueError('Provider response must contain bytes')
        if not part: break
        size += len(part)
        if size > MAX_SOURCE_BYTES:
            raise ValueError('Provider response exceeds complete byte bound')
        parts.append(part)
    if declared is not None and declared != size:
        raise ValueError('Incomplete provider response body')
    return b''.join(parts)


def fetch_polygon_bars(symbol, lookback_days=180):
    if not POLY_KEY:
        return {"error": "PROVIDER_CREDENTIAL_UNAVAILABLE"}
    end = datetime.now(timezone.utc).date() - timedelta(days=1)
    start = end - timedelta(days=lookback_days)
    path = f"/v2/aggs/ticker/{urllib.parse.quote(symbol, safe='')}/range/1/day/{start}/{end}"
    query = "adjusted=true&sort=asc&limit=500"
    url = "https://api.polygon.io" + path + "?" + query + "&apiKey=" + urllib.parse.quote(POLY_KEY, safe='')
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-PortfolioRisk/2.0"})
        deadline = time.monotonic() + 20
        with urllib.request.urlopen(req, timeout=20) as response:
            raw = read_provider_response(response, deadline)
        if POLY_KEY.encode() in raw:
            raise ValueError("provider echoed a credential")
        packet = source_document(raw)
        if '_source_evidence' in packet:
            raise ValueError("provider response contains reserved evidence field")
        packet['_source_evidence'] = {"request": "https://api.polygon.io"+path+"?"+query,
            "received_at": datetime.now(timezone.utc).isoformat(), "body_sha256": hashlib.sha256(raw).hexdigest(),
            "raw_body_base64": base64.b64encode(raw).decode(),
            "definition": "https://www.massive.com/docs/rest/stocks/aggregates/custom-bars"}
        return packet
    except Exception as exc:
        # URLs and exception bodies may contain credentials; retain type only.
        return {"error": "SOURCE_REQUEST_FAILED", "error_type": type(exc).__name__}


def batch_fetch_bars(symbols, lookback_days=180, max_workers=6):
    out = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        jobs = {executor.submit(fetch_polygon_bars, s, lookback_days): s for s in symbols}
        for future in as_completed(jobs):
            try: out[jobs[future]] = future.result()
            except Exception: out[jobs[future]] = {"error": "SOURCE_REQUEST_FAILED"}
    return out


def verify_archive_bytes(response, expected, deadline):
    """Accept an immutable collision only after exact complete byte comparison."""
    body = response['Body']
    size = 0
    try:
        declared = response.get('ContentLength')
        if declared is not None and (type(declared) is not int or declared != len(expected)):
            raise ValueError('Invalid immutable archive length')
        if response.get('ContentEncoding', 'identity') not in ('', 'identity'):
            raise ValueError('Unexpected immutable archive encoding')
        while True:
            if time.monotonic() >= deadline:
                raise ValueError('Immutable archive read deadline exceeded')
            part = body.read(min(65536, len(expected) + 1 - size))
            if time.monotonic() >= deadline:
                raise ValueError('Immutable archive read deadline exceeded')
            if type(part) is not bytes:
                raise ValueError('Invalid immutable archive body')
            if not part:
                break
            if part != expected[size:size + len(part)]:
                raise ValueError('Immutable archive content differs')
            size += len(part)
        if size != len(expected):
            raise ValueError('Incomplete immutable archive body')
    finally:
        body.close()


def retain_bundle(bundle):
    raw = canonical(bundle)
    sha = hashlib.sha256(raw).hexdigest()
    key = ARCHIVE_PREFIX + 'risk-v2-' + sha + '.json'
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=raw, ContentType='application/json',
                      CacheControl='private, no-store', IfNoneMatch='*')
    except Exception as exc:
        code = str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
        if code not in ('PreconditionFailed', '412'):
            raise
        deadline = time.monotonic() + 20
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        verify_archive_bytes(response, raw, deadline)
    return {'schema_version': VERSION, 'bundle_key': key, 'bundle_sha256': sha,
            'output_sha256': bundle['output_sha256'], 'scope': 'private owner risk',
            'independent_runner_verified': False}


def snapshot_document(response):
    """Read the existing private snapshot completely before any provider work."""
    body = response['Body']
    limit, parts, size = 4 * 1024 * 1024, [], 0
    try:
        declared = response.get('ContentLength')
        if declared is not None and (type(declared) is not int or declared < 0 or declared > limit):
            raise ValueError('Invalid complete snapshot length')
        while True:
            part = body.read(min(65536, limit + 1 - size))
            if not isinstance(part, bytes): raise ValueError('Invalid snapshot body bytes')
            if not part: break
            size += len(part)
            if size > limit: raise ValueError('Complete snapshot exceeds byte bound')
            parts.append(part)
        if declared is not None and size != declared: raise ValueError('Incomplete snapshot body')
    finally:
        body.close()
    def pairs(rows):
        out = {}
        for key, value in rows:
            if key in out: raise ValueError('Duplicate snapshot JSON field')
            out[key] = value
        return out
    def constant(_): raise ValueError('Nonfinite snapshot JSON value')
    document = json.loads(b''.join(parts).decode('utf-8', errors='strict'), object_pairs_hook=pairs, parse_constant=constant)
    if not isinstance(document, dict): raise ValueError('Invalid private snapshot document')
    snapshot_value_identity(document)
    return document


def _run_private(event, context):
    if (event or {}).get('validation_only') is True:
        # Exercise the deployed provider adapter and pure math without reading
        # an account or publishing anything. The one-share holding is synthetic.
        now = datetime.now(timezone.utc)
        packet = fetch_polygon_bars('SPY', 180)
        closes, errors = read_bars(packet, 'SPY', now)
        if errors:
            return {'statusCode': 503, 'body': json.dumps({'validation_only': True, 'ok': False, 'reason_codes': errors, 'writes': 0})}
        row = max((r for r in packet['results'] if datetime.fromtimestamp(r['t']/1000, timezone.utc).date() < now.date()), key=lambda r:r['t'])
        synthetic = {'generated_at': now.isoformat(), 'positions': [{'symbol':'SPY', 'asset_class':'etf', 'currency':'USD',
            'qty':1, 'current_price':row['c'], 'market_value':row['c'], 'price_asof_unix_ms':row['t'], 'valuation_status':'PRICED'}]}
        bundle, output = freeze(synthetic, {'SPY':packet}, now.isoformat(), {})
        replay(bundle)
        ok = output['status'] == 'AVAILABLE_HOLDINGS_MODEL' and output['holdings_risk']['beta_spy_per_gross'] == 1
        return {'statusCode': 200 if ok else 503, 'body': json.dumps({'validation_only':True, 'ok':ok,
            'schema_version':VERSION, 'writes':0, 'private_account_reads':0, 'source':'Polygon SPY daily bars',
            'original_response_sha256':packet['_source_evidence']['body_sha256'],
            'sample_count':output['risk_contract']['sample_count'], 'as_of':output['risk_contract']['sample_end'],
            'replay':'reproduced', 'sizing_eligible':False})}
    snapshot = snapshot_document(s3.get_object(Bucket=S3_BUCKET, Key=SNAPSHOT_KEY))
    if not isinstance(snapshot, dict):
        raise ValueError('invalid private snapshot')
    symbols = {'SPY'}
    for row in snapshot.get('positions') or []:
        identity = resolve_instrument(row.get('symbol'), row.get('asset_class'))
        if identity and identity['asset_class'] == 'equity':
            symbols.add(identity['provider_symbols']['polygon'])
    packets = batch_fetch_bars(sorted(symbols), 180) if snapshot.get('positions') else {}
    bundle, payload = freeze(snapshot, packets, datetime.now(timezone.utc).isoformat(), SCENARIOS)
    replay(bundle)
    payload['replay'] = retain_bundle(bundle)
    payload['alerts_sent'] = 0
    payload['notification_policy'] = 'No automatic messages from this research risk model'
    publish_risk(payload)
    # Invoke response contains no holdings, account balances or model values.
    return {'statusCode': 200, 'body': json.dumps({'success': True, 'schema_version': VERSION,
        'status': payload['status'], 'generated_at': payload['generated_at'], 'alerts_sent': 0})}


def load_alert_history():
    try:
        history = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=ALERT_HISTORY_KEY)['Body'].read())
        if not isinstance(history, dict): raise ValueError('invalid owner history')
        return history
    except Exception as error:
        if str(getattr(error, 'response', {}).get('Error', {}).get('Code', '')) in ('404', 'NoSuchKey', 'NotFound'):
            return {}
        raise


def save_alert_history(history):
    s3.put_object(Bucket=S3_BUCKET, Key=ALERT_HISTORY_KEY, Body=canonical(history),
                  ContentType='application/json', CacheControl='private, no-store')
    publish_private('portfolio-risk-history', history)


def publish_risk(payload):
    s3.put_object(Bucket=S3_BUCKET, Key=RISK_KEY, Body=canonical(payload),
                  ContentType='application/json', CacheControl='private, no-store')
    publish_private('portfolio-risk', payload)


def lambda_handler(event, context):
    denied = private_http_denied(event)
    if denied is not None: return denied
    response = _run_private(event, context)
    response['headers'] = {'Cache-Control': 'private, no-store', 'Vary': 'Authorization'}
    return response
