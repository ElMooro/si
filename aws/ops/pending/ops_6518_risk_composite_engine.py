"""ops 6518 - justhodl-risk: one composite risk engine over every long-history risk gauge the system banks.

  * justhodl-risk -> data/risk.json + data/warm/risk/ (page /risk.html; the old Risk Sizer moved to /risk-sizer.html)
      STRESS (10 pillars, 64 gauges) and FROTH (3 pillars, 18 gauges) composites, point-in-time expanding percentiles
      since 1990, 3-year relative readings, breadth, Bitcoin froth/stress, episode scorecard (14 equity, 7 Bitcoin
      episodes), train (1990-2012) vs held-out (2013->) threshold statistics, system risk-engine board.
      Inputs are read from the bucket (fred-scoped warm store, cds long-src mirrors, risk-source engine warm series)
      plus Coin Metrics community data.  Weights frozen in risk_model.py; the engine never retunes itself.
  * justhodl-provider-catalog -> "risk" provider registered (data.html / provider.html?p=risk).

Invokes the engine once (idempotent), checks hot packet / warm state / warm STRESS & FROTH series / scorecard tables /
doctrine, rebuilds the provider catalog and verifies the entry.  Waits for deploy-lambdas release receipts covering THIS
commit.  Descriptive engine only (decision.call None, sizing_eligible False).
"""

from datetime import datetime, timezone
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
REPORT = ROOT / 'aws/ops/reports/latest/ops_6518_risk_composite_engine.md'
FN = 'justhodl-risk'
SLUG = 'risk'
CATALOG_FN = 'justhodl-provider-catalog'
ALL_FNS = (FN, CATALOG_FN)
THIS_COMMIT = os.environ.get('GITHUB_SHA') or subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True, cwd=ROOT).stdout.strip()
CFG = Config(retries={'max_attempts': 0}, read_timeout=660, connect_timeout=30)
lam = boto3.client('lambda', region_name=REGION, config=CFG)
sch = boto3.client('scheduler', region_name=REGION, config=Config(retries={'max_attempts': 2}))
s3 = boto3.client('s3', region_name=REGION)
out = {'ran_at': datetime.now(timezone.utc).isoformat(), 'commit': THIS_COMMIT}
failed = []
ENGINE_PATHS = tuple('aws/lambdas/%s/' % fn for fn in ALL_FNS) + ('aws/shared/',)


def receipt_covers_this_commit(receipt_commit):
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
        except Exception as exc:  # noqa: BLE001
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
    if isinstance(body, dict) and 'body' in body and 'statusCode' in body:
        body = json.loads(body['body']) if isinstance(body['body'], str) else body['body']
    if isinstance(body, dict) and body.get('ok') is False:
        raise RuntimeError('%s returned not ok: %s' % (fn, str(body)[:600]))
    return body


def get_json(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())


try:
    today = datetime.now(timezone.utc).date()
    out['functions'] = {}
    for fn in ALL_FNS:
        cfg = wait_for_function(fn)
        out['functions'][fn] = {'arn': cfg['FunctionArn'], 'runtime': cfg.get('Runtime'), 'timeout': cfg.get('Timeout'), 'memory': cfg.get('MemorySize'),
                                'code_sha': cfg.get('CodeSha256'), 'schedule': ensure_schedule(fn, cfg['FunctionArn'])}

    # 1. run the composite engine
    t0 = time.time()
    res = invoke(FN, {})
    res['elapsed_s'] = round(time.time() - t0, 1)
    out['invoke'] = res
    if not res.get('ok'):
        failed.append('risk: invoke not ok: %s' % json.dumps(res)[:300])

    # 2. hot packet + warm state
    hot = get_json('data/%s.json' % SLUG)
    state = get_json('data/warm/%s/state.json' % SLUG)
    latest = hot.get('latest') or {}
    model = hot.get('model') or {}
    out['hot'] = {'version': hot.get('version'), 'as_of': hot.get('as_of'), 'generated_at': hot.get('generated_at'), 'n_series': hot.get('n_series'),
                  'n_points': hot.get('n_points'), 'kpis': hot.get('kpis'), 'tables': {k: len(v.get('rows') or []) for k, v in (hot.get('tables') or {}).items()},
                  'latest': latest, 'decision': hot.get('decision'), 'inputs_ok': hot.get('inputs_ok'), 'inputs_failed': hot.get('inputs_failed'),
                  'build_seconds': hot.get('build_seconds'), 'scorecard_summary': model.get('scorecard_summary'), 'best_threshold': model.get('best_threshold')}
    out['state'] = {'n_series': state.get('n_series'), 'catalog_len': len(state.get('catalog') or [])}
    dec = hot.get('decision') or {}
    if dec.get('call') is not None or dec.get('sizing_eligible'):
        failed.append('risk: doctrine breach, decision block is not descriptive')
    if (hot.get('n_series') or 0) < 80:
        failed.append('risk: only %s series published (expected >= 80)' % hot.get('n_series'))
    if len(state.get('catalog') or []) != (hot.get('n_series') or 0):
        failed.append('risk: warm state catalog (%d) != hot n_series (%s)' % (len(state.get('catalog') or []), hot.get('n_series')))
    if not (hot.get('generated_at') or '').startswith(today.isoformat()):
        failed.append('risk: packet not generated today (%s)' % hot.get('generated_at'))
    txt = json.dumps({k: hot.get(k) for k in ('title', 'description', 'kpis', 'notes', 'tables')}).lower()
    for banned in (' buy ', ' sell ', 'target price', 'forecast', 'predict'):
        if banned in txt:
            failed.append('risk: doctrine, packet text contains "%s"' % banned.strip())
    for k in hot.get('kpis') or []:
        t = '%s %s %s' % (k.get('label'), k.get('value'), k.get('sub'))
        if 'None' in t or 'undefined' in t or 'nan' in t.lower().split():
            failed.append('risk: KPI renders a null: %r' % t)
    # composite sanity
    for key in ('stress', 'froth', 'stress_rel3y', 'froth_rel3y', 'breadth80', 'btc_froth', 'btc_stress'):
        v = latest.get(key)
        if not isinstance(v, (int, float)) or not 0 <= v <= 100:
            failed.append('risk: latest.%s = %r not in 0..100' % (key, v))
    if (latest.get('gauges_live') or 0) < 70:
        failed.append('risk: only %s gauges live (expected >= 70 of %s)' % (latest.get('gauges_live'), latest.get('gauges_total')))
    if (hot.get('inputs_failed') or 0) > 3:
        failed.append('risk: %s input reads failed' % hot.get('inputs_failed'))
    # as-of must be fresh: the FRED-scoped store and Coin Metrics tail should put the calendar within 4 days of today
    as_of = latest.get('date') or hot.get('as_of') or ''
    if not as_of or (today - datetime.strptime(as_of[:10], '%Y-%m-%d').date()).days > 4:
        failed.append('risk: as_of %r is stale' % as_of)
    tables = hot.get('tables') or {}
    for name, min_rows in (('pillars', 10), ('froth_pillars', 3), ('components', 80), ('scorecard_equity', 13), ('scorecard_bitcoin', 6),
                           ('thresholds', 20), ('drivers', 10), ('system_board', 15), ('episodes_vs_today', 13), ('inputs', 60)):
        n = len((tables.get(name) or {}).get('rows') or [])
        if n < min_rows:
            failed.append('risk: table %s has %d rows (expected >= %d)' % (name, n, min_rows))
    # scorecard must reproduce the known shape of history: GFC and COVID troughs in the extreme zone
    sc_rows = {r[0]: r for r in (tables.get('scorecard_equity') or {}).get('rows') or []}
    cols = (tables.get('scorecard_equity') or {}).get('columns') or []
    try:
        i_max = cols.index('stress_max')
        for ep in ('Global financial crisis', 'COVID crash'):
            v = sc_rows[ep][i_max]
            if not isinstance(v, (int, float)) or v < 80:
                failed.append('risk: scorecard %s stress_max %r < 80' % (ep, v))
        out['scorecard_gfc_covid'] = {ep: sc_rows[ep][i_max] for ep in ('Global financial crisis', 'COVID crash')}
    except (ValueError, KeyError) as exc:
        failed.append('risk: scorecard_equity missing expected episode/column (%r)' % exc)
    # warm series for the page's 36-year chart
    for sid in ('STRESS', 'FROTH', 'STRESS_REL3Y', 'FROTH_REL3Y', 'P_CREDIT', 'BTC_FROTH'):
        try:
            obj = s3.get_object(Bucket=BUCKET, Key='data/warm/%s/series/%s.json.gz' % (SLUG, sid))
            import gzip  # noqa: PLC0415
            body = gzip.decompress(obj['Body'].read())
            pts = json.loads(body).get('points') or []
            out.setdefault('warm_series', {})[sid] = {'n': len(pts), 'first': pts[0][0] if pts else None, 'last': pts[-1][0] if pts else None}
            if sid in ('STRESS', 'FROTH') and (len(pts) < 8000 or pts[0][0] > '1991-01-01'):
                failed.append('risk: warm %s has %d points from %s (expected ~9000 from 1990)' % (sid, len(pts), pts[0][0] if pts else None))
        except Exception as exc:  # noqa: BLE001
            failed.append('risk: warm series %s unreadable (%s)' % (sid, type(exc).__name__))
    # Bitcoin mirror written
    try:
        s3.head_object(Bucket=BUCKET, Key='data/warm/%s/src/btc_coinmetrics.json' % SLUG)
    except Exception as exc:  # noqa: BLE001
        failed.append('risk: btc_coinmetrics.json mirror missing (%s)' % type(exc).__name__)

    # 3. provider catalog
    t0 = time.time()
    cat_res = invoke(CATALOG_FN, {})
    out['catalog_invoke'] = {'elapsed_s': round(time.time() - t0, 1), 'summary': str(cat_res)[:400]}
    cat = get_json('data/provider-catalog.json')
    provs = {p.get('slug'): p for p in cat.get('providers') or []}
    p = provs.get(SLUG)
    if not p:
        failed.append('catalog: provider %s missing from data/provider-catalog.json' % SLUG)
    else:
        out['catalog'] = {k: p.get(k) for k in ('n_keys', 'total_mb', 'series_count', 'hot_feeds', 'freshest_h')}
        if (p.get('n_keys') or 0) < 80:
            failed.append('catalog: provider %s has %s keys' % (SLUG, p.get('n_keys')))
        try:
            s3.head_object(Bucket=BUCKET, Key='data/providers/%s.json' % SLUG)
        except Exception as exc:  # noqa: BLE001
            failed.append('catalog: data/providers/%s.json missing (%s)' % (SLUG, type(exc).__name__))
except Exception as exc:  # noqa: BLE001
    failed.append('exception: %s: %s' % (type(exc).__name__, exc))
    out['exception'] = repr(exc)

out['ok'] = not failed
lines = ['# ops 6518 - justhodl-risk composite risk engine (STRESS + FROTH, 80+ gauges, 1990->)', '',
         '- ran_at: %s' % out['ran_at'], '- commit: %s' % out['commit'], '']
inv = out.get('invoke') or {}
lines.append('- risk: ok=%s series=%s points=%s as_of=%s elapsed=%ss' % (inv.get('ok'), inv.get('n_series'), inv.get('n_points'), inv.get('as_of'), inv.get('elapsed_s')))
for k in (out.get('hot') or {}).get('kpis') or []:
    lines.append('    - %s: %s (%s)' % (k.get('label'), k.get('value'), k.get('sub')))
lines += ['', '- scorecard summary: %s' % json.dumps((out.get('hot') or {}).get('scorecard_summary')),
          '- best thresholds: %s' % json.dumps((out.get('hot') or {}).get('best_threshold')),
          '- warm series: %s' % json.dumps(out.get('warm_series')), '- catalog: %s' % json.dumps(out.get('catalog')), '']
lines += ['## FAILED', ''] + ['- %s' % f for f in failed] if failed else ['## FAILED', '', '- none']
lines += ['', '## Full data', '', '```json', json.dumps(out, default=str), '```', '', 'VERDICT: %s' % ('PASS' if out['ok'] else 'FAIL')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:60]))
print('VERDICT:', 'PASS' if out['ok'] else 'FAIL')
if failed:
    sys.exit(1)
sys.exit(0)
