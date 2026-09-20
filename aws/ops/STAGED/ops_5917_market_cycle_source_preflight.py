"""Audit cycle/breadth/survey runtimes and preserve public predecessors; invoke nothing."""
from pathlib import Path
from datetime import datetime, timezone
import base64, hashlib, io, json, re, sys, urllib.request, urllib.error, zipfile
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/ops/checks'), str(ROOT/'scripts')]
from ops_report import report
from release_package_evidence import shared_imports

BUCKET = 'justhodl-dashboard-live'
PRIVATE = 'audit-private/20260909-originals/market-cycle/'
MAX = 24*1024*1024
FUNCTIONS = ('justhodl-market-extremes', 'justhodl-capitulation',
             'justhodl-aaii-sentiment', 'justhodl-market-internals')
PRODUCTS = ('data/market-extremes.json', 'data/market-extremes-history.json',
            'data/capitulation.json', 'data/capitulation-history.json',
            'data/aaii-sentiment.json', 'data/market-internals.json')
INPUTS = ('valuations-data.json', 'data/market-internals.json', 'data/aaii-sentiment.json',
          'data/credit-stress.json', 'data/insider-aggregate.json', 'data/retail-sentiment.json',
          'data/vrp.json', 'data/vol-surface.json', 'data/eurodollar-stress.json')


def bounded(stream):
    try: raw = stream.read(MAX+1)
    finally: stream.close()
    if len(raw) > MAX: raise ValueError('Explicit response bound exceeded')
    return raw


def fetch(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers={
            'User-Agent': 'JustHodl-research-audit/1.0 (+https://justhodl.ai)'}), timeout=35) as response:
        return bounded(response)


def public(key):
    return fetch('https://justhodl.ai/'+key)


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method='HEAD'), timeout=25): return False
    except urllib.error.HTTPError as exc: return exc.code in (401, 403, 404)


def preserve(s3, raw):
    sha = hashlib.sha256(raw).hexdigest(); key = PRIVATE+sha+'.bin'
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=raw, ContentType='application/octet-stream',
                      CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in (
                'PreconditionFailed', '412', 'ConditionalRequestConflict', '409'): raise
    assert bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body']) == raw
    assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
    return {'sha256': sha, 'bytes': len(raw), 'anonymous_denied': True}


def shape(raw):
    d = json.loads(raw)
    assert isinstance(d, dict)
    result = {'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(), 'keys': sorted(d),
              'clocks': {k: d[k] for k in ('generated_at', 'updated_at', 'as_of', 'data_date') if k in d},
              'contract': d.get('contract')}
    # Only explicitly selected public market fields; no embedded watchlist/AI context.
    for key in ('schema_version', 'version', 'posture', 'cycle_position', 'capitulation_score',
                'signal', 'stabilising', 'days_covered', 'days_added', 'source', 'backfilled_rows'):
        if key in d: result[key] = d[key]
    if isinstance(d.get('latest'), dict):
        result['latest'] = {k: v for k, v in d['latest'].items() if k in (
            'week_ending', 'bullish', 'neutral', 'bearish', 'bull_bear_spread',
            'ADVANCERS', 'DECLINERS', 'UNCHANGED', 'UP_VOLUME', 'DOWN_VOLUME', 'TRIN',
            'NEW_HIGHS', 'NEW_LOWS', 'PCT_ABOVE_50DMA', 'PCT_ABOVE_200DMA')}
    return result


def main():
    lam = boto3.client('lambda', region_name='us-east-1')
    s3 = boto3.client('s3', region_name='us-east-1')
    events = boto3.client('events', region_name='us-east-1')
    scheduler = boto3.client('scheduler', region_name='us-east-1')
    with report('ops_5917_market_cycle_source_preflight') as r:
        r.kv(engine_invocations=0, private_account_reads=0, notifications_sent=0,
             portfolio_writes=0, paid_ai_calls=0)
        configs = {}
        for fn in FUNCTIONS:
            live = lam.get_function(FunctionName=fn); cfg = live['Configuration']; configs[fn] = cfg
            archive = fetch(live['Code']['Location'])
            assert base64.b64encode(hashlib.sha256(archive).digest()).decode() == cfg['CodeSha256']
            directory = ROOT/'aws/lambdas'/fn/'source'
            sources = list(directory.glob('*.py'))
            expected = {p.name: p for p in sources}
            expected.update({p.name: p for p in shared_imports(ROOT, sources) if not (directory/p.name).exists()})
            with zipfile.ZipFile(io.BytesIO(archive)) as z:
                for name, path in expected.items():
                    assert z.read(name) == path.read_bytes(), 'Runtime differs; review before rewriting: '+fn+'/'+name
            try:
                receipt = json.loads(public('data/ops/releases/'+fn+'.json'))
                assert receipt['code_sha256'] == cfg['CodeSha256']
                prior = {'commit': receipt['commit'], 'matches_runtime': True}
            except urllib.error.HTTPError as exc:
                if exc.code not in (403, 404): raise
                prior = {'http_status': exc.code, 'matches_runtime': None}
            rules = []
            for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):
                for name in page.get('RuleNames', []):
                    rule = events.describe_rule(Name=name)
                    rules.append({k: rule.get(k) for k in ('Name', 'State', 'ScheduleExpression')})
            r.kv(function=fn, runtime={'code_sha256': cfg['CodeSha256'], 'packaged_sources_match': True,
                'packaged_files_checked': len(expected), 'source_bytes': len((directory/'lambda_function.py').read_bytes()),
                'memory_mb': cfg['MemorySize'], 'timeout_s': cfg['Timeout'], 'prior_receipt': prior}, classic_rules=rules)
        sched = scheduler.get_schedule(Name='justhodl-market-extremes-daily')
        r.kv(market_extremes_scheduler={k: sched.get(k) for k in ('Name', 'State', 'ScheduleExpression', 'ScheduleExpressionTimezone')},
             scheduler_target_matches=sched['Target']['Arn'] == configs['justhodl-market-extremes']['FunctionArn'])
        for key in PRODUCTS:
            raw = bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body'])
            saved = preserve(s3, raw)
            assert json.loads(public(key)) == json.loads(raw), 'Public/S3 product disagreement: '+key
            r.kv(product=key, protected_complete_predecessor=saved, public_shape=shape(raw))
        for key in INPUTS:
            try: r.kv(input=key, public_shape=shape(public(key)))
            except urllib.error.HTTPError as exc: r.kv(input=key, public_http_status=exc.code)
        # Two ordinary public survey pages: no login, paywall workaround or producer execution.
        for url in ('https://www.aaii.com/sentimentsurvey', 'https://www.aaii.com/sentimentsurvey/sent_results'):
            try:
                raw = fetch(url); saved = preserve(s3, raw)
                r.kv(public_survey_probe={'url': url, 'acquired_at': datetime.now(timezone.utc).isoformat(),
                     'original': saved, 'html_table_rows': len(re.findall(rb'<tr\b', raw, re.I))})
            except urllib.error.HTTPError as exc: r.kv(public_survey_probe={'url': url, 'http_status': exc.code})
        r.kv(next_work='Reconstruct native breadth denominators and dated survey observations before cycle inference. Preserve previous products, inspect all direct consumers, and do not grant forecasting or portfolio authority to unvalidated scores.')


if __name__ == '__main__':
    try: main()
    except Exception:
        print('Cycle source preflight failed; inspect its committed report before retrying. No producer was invoked.')
        sys.exit(1)
