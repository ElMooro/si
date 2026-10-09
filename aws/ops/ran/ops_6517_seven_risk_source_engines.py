"""ops 6517 - seven more free sovereign / systemic risk source engines, each with its own page and provider-catalog entry.

Second pass of Khalid's list (first pass = ops 6516: FDIC BankFind, NY Fed CMDI, ESMA ratings, EBA risk dashboard):

  * justhodl-epu                     -> data/epu.json                     (page /epu.html)                     Baker-Bloom-Davis EPU, global + national + US daily
  * justhodl-world-uncertainty-index -> data/world-uncertainty-index.json (page /world-uncertainty-index.html) Ahir-Bloom-Furceri WUI, 143 countries + WTU
  * justhodl-fao-food-price-index    -> data/fao-food-price-index.json    (page /fao-food-price-index.html)    FAO FFPI + five sub-indices since 1990
  * justhodl-fhfa-hpi                -> data/fhfa-hpi.json                (page /fhfa-hpi.html)                FHFA HPI: US, divisions, states, 100 metros
  * justhodl-fragile-states-index    -> data/fragile-states-index.json    (page /fragile-states-index.html)    Fund for Peace FSI, every edition 2006->
  * justhodl-nyfed-hhdc              -> data/nyfed-hhdc.json              (page /nyfed-hhdc.html)              NY Fed Household Debt & Credit report workbook
  * justhodl-worldbank-wgi           -> data/worldbank-wgi.json           (page /worldbank-wgi.html)           World Bank WGI, six dimensions x 214 economies
  * aws/shared/risk_sources.py 1.1.0 -> absolute-path xlsx rels, ordinal() captions, per-packet hot tail; nyfed-cmdi + eba
                                        re-deployed for the "18.1th pct" -> "18th pct" caption fix.
  * justhodl-provider-catalog        -> seven new providers registered (data.html / provider.html).

Each source engine is invoked once (they are idempotent), its hot packet / warm state / first warm series are checked, then
the provider catalog is rebuilt and the seven entries verified.  Waits until deploy-lambdas has published a release receipt
covering THIS commit for every function.  Descriptive engines only (decision.call None, sizing_eligible False).
"""

from datetime import date, datetime, timezone
from pathlib import Path
import json
import os
import subprocess
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'scripts'))
from apply_direct_scheduler import payload as scheduler_payload  # noqa: E402

REGION = 'us-east-1'
BUCKET = 'justhodl-dashboard-live'
REPORT = ROOT / 'aws/ops/reports/latest/ops_6517_seven_risk_source_engines.md'
SLUGS = ('epu', 'world-uncertainty-index', 'fao-food-price-index', 'fhfa-hpi', 'fragile-states-index', 'nyfed-hhdc', 'worldbank-wgi')
SOURCE_ENGINES = tuple('justhodl-' + s for s in SLUGS)
REDEPLOYED = ('justhodl-nyfed-cmdi', 'justhodl-eba-risk-dashboard')      # caption fix only; not re-invoked (their 03:xx crons will)
CATALOG_FN = 'justhodl-provider-catalog'
ALL_FNS = SOURCE_ENGINES + REDEPLOYED + (CATALOG_FN,)
SLUG = {'justhodl-' + s: s for s in SLUGS}
MIN_SERIES = {'epu': 20, 'world-uncertainty-index': 300, 'fao-food-price-index': 6, 'fhfa-hpi': 120, 'fragile-states-index': 150, 'nyfed-hhdc': 100, 'worldbank-wgi': 1000}
EXPECT = {  # (kpi label substring, minimum table rows) sanity per engine, from the local dry runs of 2026-10-09
    'epu': ('Global EPU', 20), 'world-uncertainty-index': ('World Uncertainty Index', 140), 'fao-food-price-index': ('FAO Food Price Index', 6),
    'fhfa-hpi': ('US purchase-only HPI', 51), 'fragile-states-index': ('Most fragile', 170), 'nyfed-hhdc': ('Total household debt', 20),
    'worldbank-wgi': ('Economies covered', 200)}
THIS_COMMIT = os.environ.get('GITHUB_SHA') or subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True, cwd=ROOT).stdout.strip()
CFG = Config(retries={'max_attempts': 0}, read_timeout=660, connect_timeout=30)
lam = boto3.client('lambda', region_name=REGION, config=CFG)
sch = boto3.client('scheduler', region_name=REGION, config=Config(retries={'max_attempts': 2}))
s3 = boto3.client('s3', region_name=REGION)
out = {'ran_at': datetime.now(timezone.utc).isoformat(), 'commit': THIS_COMMIT}
failed = []
ENGINE_PATHS = tuple('aws/lambdas/%s/' % fn for fn in ALL_FNS) + ('aws/shared/',)


def receipt_covers_this_commit(receipt_commit):
    """Accept the receipt when it is THIS commit, or an ancestor with no engine-path change between it and HEAD."""
    if not receipt_commit:
        return False
    if receipt_commit == THIS_COMMIT:
        return True
    try:
        subprocess.run(['git', 'fetch', '-q', '--depth=50', 'origin', receipt_commit], cwd=ROOT, capture_output=True, text=True, timeout=120)
        anc = subprocess.run(['git', 'merge-base', '--is-ancestor', receipt_commit, THIS_COMMIT], cwd=ROOT, capture_output=True, text=True)
        if anc.returncode != 0:
            return False
        diff = subprocess.run(['git', 'diff', '--name-only', receipt_commit, THIS_COMMIT, '--'] + list(ENGINE_PATHS), cwd=ROOT, capture_output=True, text=True)
        return diff.returncode == 0 and not diff.stdout.strip()
    except Exception:  # noqa: BLE001
        return False


def wait_for_function(fn, max_wait_s=2400):
    """deploy-lambdas runs in a parallel workflow on the same push.  New functions do not exist until it has run, existing
    ones hold the OLD code: wait until the release receipt names a commit covering THIS one and the code sha matches."""
    t0 = time.time()
    last = None
    key = 'data/ops/releases/%s.json' % fn
    while time.time() - t0 < max_wait_s:
        try:
            receipt = json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())
            cfg = lam.get_function_configuration(FunctionName=fn)
            last = (receipt.get('commit', '')[:8], cfg.get('State'), cfg.get('LastUpdateStatus'), cfg.get('CodeSha256'))
            if receipt_covers_this_commit(receipt.get('commit', '')) and cfg.get('CodeSha256') == receipt.get('code_sha256') \
                    and cfg.get('State') == 'Active' and cfg.get('LastUpdateStatus') in (None, 'Successful'):
                return cfg
        except Exception as exc:  # noqa: BLE001 - receipt or function not there yet
            last = ('waiting', type(exc).__name__)
        time.sleep(20)
    raise RuntimeError('%s: new code (%s) not deployed after %ds (last=%s)' % (fn, THIS_COMMIT[:8], max_wait_s, last))


def ensure_schedule(fn, function_arn):
    config = json.loads((ROOT / 'aws/lambdas' / fn / 'config.json').read_text())
    spec = config.get('eventbridge_scheduler')
    if not spec:
        return {'status': 'no schedule in config'}
    args = {'Name': spec['schedule_name'], 'GroupName': spec.get('group_name', 'default')}
    try:
        current = sch.get_schedule(**args)
        return {'status': 'exists', 'expression': current.get('ScheduleExpression'), 'state': current.get('State')}
    except sch.exceptions.ResourceNotFoundException:
        pass
    request = scheduler_payload(config, {}, function_arn)
    try:
        sch.create_schedule(**request)
        status = 'created'
    except sch.exceptions.ConflictException:
        status = 'created_concurrently'
    back = sch.get_schedule(**args)
    return {'status': status, 'expression': back.get('ScheduleExpression'), 'state': back.get('State')}


def invoke(fn, payload):
    r = lam.invoke(FunctionName=fn, InvocationType='RequestResponse', Payload=json.dumps(payload).encode())
    body = json.loads(r['Payload'].read() or b'{}')
    if r.get('FunctionError'):
        raise RuntimeError('%s invoke failed: %s' % (fn, str(body)[:600]))
    if isinstance(body, dict) and 'body' in body and 'statusCode' in body:  # risk_sources.run envelope
        body = json.loads(body['body']) if isinstance(body['body'], str) else body['body']
    if isinstance(body, dict) and body.get('ok') is False:
        raise RuntimeError('%s returned not ok: %s' % (fn, str(body)[:600]))
    return body


def get_json(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())


def check_source_engine(fn, today):
    slug = SLUG[fn]
    rec = {}
    hot = get_json('data/%s.json' % slug)
    state = get_json('data/warm/%s/state.json' % slug)
    rec['hot'] = {'version': hot.get('version'), 'as_of': hot.get('as_of'), 'generated_at': hot.get('generated_at'), 'n_series': hot.get('n_series'),
                  'n_points': hot.get('n_points'), 'kpis': hot.get('kpis'), 'tables': {k: len(v.get('rows') or []) for k, v in (hot.get('tables') or {}).items()},
                  'source_files': hot.get('source_files'), 'notes': hot.get('notes'), 'decision': hot.get('decision')}
    rec['state'] = {'n_series': state.get('n_series'), 'catalog_len': len(state.get('catalog') or []), 'updated_at': state.get('updated_at') or state.get('generated_at')}
    if (hot.get('decision') or {}).get('call') is not None or (hot.get('decision') or {}).get('sizing_eligible'):
        failed.append('%s: doctrine breach, decision block is not descriptive' % slug)
    if (hot.get('n_series') or 0) < MIN_SERIES[slug]:
        failed.append('%s: only %s series published (expected >= %d)' % (slug, hot.get('n_series'), MIN_SERIES[slug]))
    if len(state.get('catalog') or []) != (hot.get('n_series') or 0):
        failed.append('%s: warm state catalog (%d) != hot n_series (%s)' % (slug, len(state.get('catalog') or []), hot.get('n_series')))
    gen = hot.get('generated_at') or ''
    if not gen.startswith(today.isoformat()):
        failed.append('%s: packet not generated today (%s)' % (slug, gen))
    txt = json.dumps({k: hot.get(k) for k in ('title', 'description', 'kpis', 'notes', 'tables')}).lower()
    for banned in (' buy ', ' sell ', 'target price', 'forecast', 'predict'):
        if banned in txt:
            failed.append('%s: doctrine, packet text contains "%s"' % (slug, banned.strip()))
    # one warm series object must be readable for the page chart
    first = (state.get('catalog') or [None])[0]
    if first:
        try:
            s3.head_object(Bucket=BUCKET, Key='data/warm/%s/series/%s.json.gz' % (slug, first))
            rec['first_series_gz'] = first
        except Exception as exc:  # noqa: BLE001
            failed.append('%s: warm series %s missing (%s)' % (slug, first, type(exc).__name__))
    return rec, hot


try:
    today = datetime.now(timezone.utc).date()
    out['functions'] = {}
    for fn in ALL_FNS:
        cfg = wait_for_function(fn)
        out['functions'][fn] = {'arn': cfg['FunctionArn'], 'runtime': cfg.get('Runtime'), 'timeout': cfg.get('Timeout'), 'memory': cfg.get('MemorySize'),
                                'code_sha': cfg.get('CodeSha256'), 'schedule': ensure_schedule(fn, cfg['FunctionArn'])}

    # 1. the seven source engines
    out['sources'] = {}
    for fn in SOURCE_ENGINES:
        slug = SLUG[fn]
        t0 = time.time()
        res = invoke(fn, {})
        res['elapsed_s'] = round(time.time() - t0, 1)
        out['sources'][fn] = {'invoke': res}
        if not res.get('ok'):
            failed.append('%s: invoke not ok: %s' % (slug, json.dumps(res)[:300]))
            continue
        rec, hot = check_source_engine(fn, today)
        out['sources'][fn]['check'] = rec
        label, min_rows = EXPECT[slug]
        if not any(label.lower() in (k.get('label') or '').lower() for k in hot.get('kpis') or []):
            failed.append('%s: expected KPI "%s" missing' % (slug, label))
        biggest = max([len(v.get('rows') or []) for v in (hot.get('tables') or {}).values()] or [0])
        if biggest < min_rows:
            failed.append('%s: largest table has %d rows (expected >= %d)' % (slug, biggest, min_rows))
        for k in hot.get('kpis') or []:
            txt = '%s %s %s' % (k.get('label'), k.get('value'), k.get('sub'))
            if 'None' in txt or 'nan' in txt.lower().split() or 'undefined' in txt:
                failed.append('%s: KPI renders a null: %r' % (slug, txt))
        bad = [f for f in hot.get('source_files') or [] if not str(f.get('status', '')).startswith('live')]
        if bad:
            out['sources'][fn]['non_live_files'] = bad

    # 2. engine-specific sanity
    e = get_json('data/epu.json')
    out['epu_global'] = (e.get('series') or {}).get('GEPU_CURRENT', {}).get('latest')
    if not out['epu_global'] or out['epu_global'] < 50:
        failed.append('epu: Global EPU latest implausible %r' % out['epu_global'])
    f = get_json('data/fao-food-price-index.json')
    out['ffpi'] = (f.get('series') or {}).get('FFPI', {}).get('latest')
    if not out['ffpi'] or not 60 < out['ffpi'] < 250:
        failed.append('fao: FFPI latest implausible %r' % out['ffpi'])
    h = get_json('data/fhfa-hpi.json')
    out['fhfa_us'] = (h.get('series') or {}).get('US_SA', {}).get('latest')
    if not out['fhfa_us'] or out['fhfa_us'] < 300:
        failed.append('fhfa: US_SA latest implausible %r' % out['fhfa_us'])
    hh = get_json('data/nyfed-hhdc.json')
    out['hhdc_quarter'] = hh.get('report_quarter')
    if not out['hhdc_quarter'] or out['hhdc_quarter'] < '2026Q1':
        failed.append('hhdc: report quarter %r is stale' % out['hhdc_quarter'])
    fs = get_json('data/fragile-states-index.json')
    out['fsi_edition'] = fs.get('latest_edition')
    if not fs.get('latest_edition') or fs['latest_edition'] < 2023:
        failed.append('fsi: latest edition %r' % fs.get('latest_edition'))
    w = get_json('data/worldbank-wgi.json')
    out['wgi_usa_rl'] = (w.get('series') or {}).get('RL_USA', {}).get('latest')
    if out['wgi_usa_rl'] is None:
        failed.append('wgi: RL_USA series missing')
    u = get_json('data/world-uncertainty-index.json')
    out['wui_global'] = (u.get('series') or {}).get('GLOBAL_GLOBAL_GDP_WEIGHTED_AVERAGE', {}).get('latest')
    if not out['wui_global']:
        failed.append('wui: GDP-weighted global series missing')

    # 3. provider catalog
    t0 = time.time()
    out['catalog_invoke'] = invoke(CATALOG_FN, {})
    out['catalog_invoke'] = {'elapsed_s': round(time.time() - t0, 1), 'summary': str(out['catalog_invoke'])[:400]}
    cat = get_json('data/provider-catalog.json')
    provs = {p.get('slug'): p for p in cat.get('providers') or []}
    out['catalog'] = {}
    for slug in SLUGS:
        p = provs.get(slug)
        if not p:
            failed.append('catalog: provider %s missing from data/provider-catalog.json' % slug)
            continue
        out['catalog'][slug] = {k: p.get(k) for k in ('n_keys', 'total_mb', 'series_count', 'hot_feeds', 'freshest_h')}
        if (p.get('n_keys') or 0) < 3:
            failed.append('catalog: provider %s has %s keys' % (slug, p.get('n_keys')))
        try:
            s3.head_object(Bucket=BUCKET, Key='data/providers/%s.json' % slug)
        except Exception as exc:  # noqa: BLE001
            failed.append('catalog: data/providers/%s.json missing (%s)' % (slug, type(exc).__name__))
except Exception as exc:  # noqa: BLE001
    failed.append('exception: %s: %s' % (type(exc).__name__, exc))
    out['exception'] = repr(exc)

out['ok'] = not failed
lines = ['# ops 6517 - seven free sovereign / systemic risk source engines (EPU, WUI, FAO FFPI, FHFA HPI, FSI, NY Fed HHDC, World Bank WGI)', '',
         '- ran_at: %s' % out['ran_at'], '- commit: %s' % out['commit'], '']
for fn in SOURCE_ENGINES:
    s = out.get('sources', {}).get(fn, {})
    inv = s.get('invoke') or {}
    chk = (s.get('check') or {}).get('hot') or {}
    lines.append('- %s: ok=%s series=%s points=%s as_of=%s elapsed=%ss' % (SLUG[fn], inv.get('ok'), inv.get('n_series'), inv.get('n_points'), inv.get('as_of'), inv.get('elapsed_s')))
    for k in chk.get('kpis') or []:
        lines.append('    - %s: %s (%s)' % (k.get('label'), k.get('value'), k.get('sub')))
lines += ['', '- catalog: %s' % json.dumps(out.get('catalog')), '']
lines += ['## FAILED', ''] + ['- %s' % f for f in failed] if failed else ['## FAILED', '', '- none']
lines += ['', '## Full data', '', '```json', json.dumps(out, default=str), '```', '', 'VERDICT: %s' % ('PASS' if out['ok'] else 'FAIL')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:60]))
print('VERDICT:', 'PASS' if out['ok'] else 'FAIL')
if failed:
    sys.exit(1)
