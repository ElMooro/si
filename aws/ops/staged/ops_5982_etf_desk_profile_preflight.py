"""Bounded source audit for the existing ETF desk; no producer invocation."""
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
from decimal import Decimal, localcontext
import ast, json, re, subprocess, sys, urllib.request, urllib.error, urllib.parse
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/ops/staged'), str(ROOT/'aws/ops/checks'), str(ROOT/'aws/shared')]
from ops_report import report
import ops_5975_etf_constituent_source_preflight as source_audit
import etf_holdings_native as native
import provider_flow_catalog as catalog

BUCKET = 'justhodl-dashboard-live'
PREFIX = 'audit-private/20260909-originals/etf-desk-research/'
FUNCTION = 'justhodl-etf-global-desk'
ENDPOINT = 'https://api.polygon.io/etf-global/v1/profiles'
PROBE = ('SPY', 'VOO', 'TLT', 'BND', 'SOXL', 'SQQQ', 'EFA', 'FXE', 'PPLT', 'IBIT')
PACKETS = ('data/etf-desk.json', 'data/etf-derived.json', 'data/etf-global.json',
    'data/etf-global-desk-meta.json', 'data/etf-holdings-complete.json', 'data/etf-holdings-index.json',
    'data/provider-fund-flow-research.json', 'data/etf-holdings-research.json', 'data/flow-lookthrough.json')
EXPOSURES = ('sector_exposure', 'industry_exposure', 'industry_group_exposure', 'subindustry_exposure',
    'geographic_exposure', 'currency_exposure', 'maturity_exposure', 'coupon_exposure')
NUMERIC = ('aum', 'net_expenses', 'total_expenses', 'management_fee', 'fee_waivers', 'other_expenses',
    'bid_ask_spread', 'discount_premium', 'avg_daily_trading_volume', 'num_holdings',
    'creation_unit_size', 'creation_fee', 'levered_amount')
bounded, encoded, code, denied = source_audit.bounded, source_audit.encoded, source_audit.code, source_audit.denied


def retain(s3, raw):
    digest = native.sha(raw); key = PREFIX + digest + '.bin'
    try: s3.put_object(Bucket=BUCKET, Key=key, Body=raw, ContentType='application/octet-stream', CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if code(exc) not in ('PreconditionFailed', 'ConditionalRequestConflict', '409', '412'): raise
    assert bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body']) == raw
    return {'key': key, 'sha256': digest, 'bytes': len(raw)}


def checked_url(url):
    assert isinstance(url, str) and len(url) <= 8192
    u = urllib.parse.urlsplit(url)
    assert u.scheme == 'https' and u.hostname in ('api.polygon.io', 'api.massive.com')
    assert u.path == '/etf-global/v1/profiles' and not u.username and not u.password and u.port in (None, 443) and not u.fragment
    query = urllib.parse.parse_qsl(u.query, keep_blank_values=True)
    allowed = {'composite_ticker', 'processed_date', 'processed_date.lte', 'sort', 'limit', 'cursor'}
    assert query and len({k for k, v in query}) == len(query) and all(k in allowed and v for k, v in query)
    return url


def request_original(s3, credential, url, budget):
    checked_url(url); budget['requests'] += 1
    assert budget['requests'] <= 120
    request = urllib.request.Request(url, headers={'User-Agent': 'JustHodl-ProfileSourceAudit/1.0', 'Authorization': 'Bearer '+credential})
    try:
        response = urllib.request.build_opener(source_audit.NoRedirect).open(request, timeout=25)
        raw = bounded(response, 8*1024*1024)
    except urllib.error.HTTPError as exc:
        status = exc.code; exc.close(); return None, {'status': 'provider_http_error', 'http_status': status}
    except Exception: return None, {'status': 'provider_request_failed'}
    assert credential.encode() not in raw
    budget['bytes'] += len(raw); assert budget['bytes'] <= 128*1024*1024
    ref = retain(s3, raw)
    meta = {'status': 'retained', 'original': ref, 'url': url, 'acquired_at': datetime.now(timezone.utc).isoformat()}
    try:
        doc = native.strict(raw)
        assert isinstance(doc, dict) and doc.get('status') == 'OK' and isinstance(doc.get('results'), list)
        assert len(doc['results']) <= 5000
        if 'count' in doc: assert type(doc['count']) is int and doc['count'] == len(doc['results'])
    except Exception: return None, {**meta, 'status': 'provider_shape_rejected'}
    return doc, meta


def describe(rows):
    """Summarize actual source structure without guessing financial units."""
    output = []
    for row in rows:
        rec = {k: row.get(k) for k in ('composite_ticker', 'effective_date', 'processed_date', 'asset_class',
            'product_type', 'leverage_style', 'description', 'issuer', 'listing_exchange')}
        rec['numeric_fields'] = {k: {'present': k in row, 'value': str(row[k]) if isinstance(row.get(k), (Decimal, int)) and not isinstance(row[k], bool) else row.get(k),
            'type': type(row.get(k)).__name__, 'unit_certified': False} for k in NUMERIC}
        exposures = {}
        for key in EXPOSURES:
            value = row.get(key); item = {'present': key in row, 'type': type(value).__name__, 'unit_certified': False}
            if isinstance(value, dict):
                values = [v for v in value.values() if isinstance(v, (Decimal, int)) and not isinstance(v, bool)]
                try:
                    with localcontext() as ctx:
                        ctx.prec=180
                        total=str(sum((native.decimal(v) for v in values), Decimal(0)))
                except (ValueError, ArithmeticError): total=None
                item.update(entries=len(value), keys=list(value), numeric_entries=len(values),
                    raw_numeric_sum=total, sum_numeric_bounds_valid=total is not None,
                    non_numeric_entries=len(value)-len(values))
            elif isinstance(value, list):
                item.update(entries=len(value), child_field_counts=dict(Counter(k for child in value if isinstance(child, dict) for k in child)))
            exposures[key] = item
        rec['exposures'] = exposures; rec['all_field_types'] = {k: type(v).__name__ for k, v in row.items()}
        output.append(rec)
    return output


def probe(s3, credential, ticker, cutoff, budget):
    selection_url = ENDPOINT+'?'+urllib.parse.urlencode({'composite_ticker': ticker, 'processed_date.lte': cutoff, 'sort': 'processed_date.desc', 'limit': 1})
    first, selection = request_original(s3, credential, selection_url, budget)
    result = {'ticker': ticker, 'cutoff': cutoff, 'selection': selection, 'pages': [], 'status': 'unavailable'}
    if not first or not first['results']: return result
    assert len(first['results']) == 1
    selected = first['results'][0]; processed = selected.get('processed_date')
    assert selected.get('composite_ticker') == ticker and isinstance(processed, str) and native.day(processed).isoformat() == processed and processed <= cutoff
    url = ENDPOINT+'?'+urllib.parse.urlencode({'composite_ticker': ticker, 'processed_date': processed, 'limit': 5000})
    rows = []; seen = set()
    for _ in range(8):
        assert url not in seen; seen.add(url)
        doc, meta = request_original(s3, credential, url, budget)
        if not doc: return {**result, 'status': 'incomplete', 'failure': meta}
        assert all(isinstance(r, dict) and r.get('composite_ticker') == ticker and r.get('processed_date') == processed for r in doc['results'])
        rows.extend(doc['results']); result['pages'].append(meta)
        if not doc.get('next_url'): break
        url = checked_url(doc['next_url'])
    else: return {**result, 'status': 'pagination_bound'}
    result.update(status='complete_returned_profile_snapshot', processed_date=processed, rows=len(rows),
        effective_dates=dict(Counter(str(r.get('effective_date')) for r in rows)),
        source_rows=describe(rows), single_profile_unambiguous=len(rows)==1)
    return result


def main():
    s3 = boto3.client('s3', region_name='us-east-1'); lam = boto3.client('lambda', region_name='us-east-1')
    with report('ops_5982_etf_desk_profile_preflight') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_etf_profile_preflight.py')],cwd=ROOT,check=True)
        # Actual package, schedule and receipt evidence; never invoke the predecessor.
        runtime = source_audit.runtime(lam, s3, boto3.client('events', region_name='us-east-1'), boto3.client('scheduler', region_name='us-east-1'), FUNCTION)
        tree = ast.parse((ROOT/'aws/lambdas'/FUNCTION/'source/lambda_function.py').read_text())
        desk = next(ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id=='DESK' for t in n.targets))
        assert len(desk) == len(set(desk)) == 116
        inventory = {}; refs = {}; keys = set(PACKETS); retained_context_bytes = 0
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix='data/etf-flow-hist/'):
            for obj in page.get('Contents', []):
                key = obj['Key']; assert re.fullmatch(r'data/etf-flow-hist/(?:_index|[A-Z][A-Z0-9.\-]{0,14})\.json', key)
                keys.add(key)
        assert len(keys) <= 1100
        for key in sorted(keys):
            try: raw = bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body'])
            except Exception as exc:
                if code(exc) not in ('NoSuchKey', '404'): raise
                inventory[key] = {'status': 'missing'}; continue
            retained_context_bytes += len(raw); assert retained_context_bytes <= 256*1024*1024
            refs[key] = retain(s3, raw); doc = json.loads(raw)
            inventory[key] = {'status': 'retained', 'bytes': len(raw), 'root_fields': list(doc),
                'generated_at': doc.get('generated_at'), 'contract': doc.get('contract'), 'funds': len(doc.get('by_etf', {}))}
        env = lam.get_function_configuration(FunctionName=FUNCTION).get('Environment', {}).get('Variables', {})
        credential = env.get('POLYGON_KEY') or env.get('POLYGON_API_KEY') or env.get('MASSIVE_API_KEY'); assert credential
        del env
        today = datetime.now(timezone.utc).date(); budget = {'requests': 0, 'bytes': 0}
        attempt_key = PREFIX+'ops-5982-profile-attempt.json'
        attempt = {'contract': 'etf-profile-source-attempt.v1', 'started_at': datetime.now(timezone.utc).isoformat(),
            'status': 'collecting', 'completed_probes': {}}
        try: s3.put_object(Bucket=BUCKET, Key=attempt_key, Body=encoded(attempt), ContentType='application/json', CacheControl='no-store', IfNoneMatch='*')
        except Exception as exc:
            if code(exc) in ('PreconditionFailed', 'ConditionalRequestConflict', '409', '412'):
                raise RuntimeError('This source audit was already attempted; inspect retained evidence without recollecting') from None
            raise
        current = {}; prior = {}
        for role, tickers, cutoff, target in [('current', PROBE, today.isoformat(), current),
                ('prior', ('SPY', 'VOO'), (today-timedelta(days=30)).isoformat(), prior)]:
            for ticker in tickers:
                target[ticker] = probe(s3, credential, ticker, cutoff, budget)
                attempt['completed_probes'][role+'_'+ticker] = retain(s3, encoded(target[ticker]))
                attempt['provider_requests'] = budget['requests']
                s3.put_object(Bucket=BUCKET, Key=attempt_key, Body=encoded(attempt), ContentType='application/json', CacheControl='no-store')
        del credential
        manifest = {'contract': 'etf-profile-source-preflight.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
            'runtime': runtime, 'desk_funds': desk, 'funds_outside_canonical': sorted(set(desk)-set(catalog.ETF_UNIVERSE)),
            'packets': refs, 'packet_inventory': inventory, 'current_probes': current, 'prior_probes': prior,
            'provider_requests': budget['requests'], 'original_provider_bytes': budget['bytes'],
            'scope': 'Exact existing desk contexts and profile originals. Numeric scale and currency are not inferred from magnitude.'}
        ref = retain(s3, encoded(manifest)); protected = {attempt_key, ref['key'], *(v['key'] for v in refs.values()), *(v['key'] for v in attempt['completed_probes'].values())}
        attempt.update(status='retained', manifest=ref, completed_at=datetime.now(timezone.utc).isoformat())
        s3.put_object(Bucket=BUCKET, Key=attempt_key, Body=encoded(attempt), ContentType='application/json', CacheControl='no-store')
        for item in [*current.values(), *prior.values()]:
            for page in [item['selection'], *item['pages'], item.get('failure', {})]:
                if page.get('original'): protected.add(page['original']['key'])
        def check(key): assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=8) as pool: list(pool.map(check, sorted(protected)))
        r.kv(retained_manifest=ref, runtime=runtime, packet_inventory=inventory, current_probes=current, prior_probes=prior,
            funds_outside_canonical=manifest['funds_outside_canonical'], originals_anonymously_denied=True,
            protected_artifacts_checked=len(protected), provider_requests=budget['requests'], original_provider_bytes=budget['bytes'],
            producer_invocations=0, private_account_reads=0, paid_ai_calls=0, signals_emitted=0, notifications_sent=0, portfolio_writes=0)


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
