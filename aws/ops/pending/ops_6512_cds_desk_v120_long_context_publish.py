"""ops 6512 - first publish of justhodl-cds-desk v1.2.0: long-context history file + expansion blocks.

v1.2.0 adds (a) data/cds-desk-history.json: every liquid name's full daily series plus a "since 2006" block built
from free government records that still carry 2008 (Moody's Baa-10Y via FRED, OFR Financial Stress Index, Fed
GZ spread / excess bond premium) with the live CDX IG mapped onto Baa-10Y by a disclosed fit, and the CDS-bond
basis against the three-year ICE BofA OAS; (b) packet blocks: term (1Y-5Y slope, inverted curves), wides_1y /
tights_1y, activity (prints per day), and "active but unpriced" records that now carry both candidate branches of
the unsigned upfront.  Off-the-run tenors only enter the curve when their own print fixes the sign.

Waits until deploy-lambdas (parallel workflow, same push) has published the release receipt for THIS commit, runs
one daily pass on the new code (the bank is unchanged; no rebuild needed) and verifies packet + history file.

Descriptive engine only (decision.call is None, sizing_eligible False).  Retries disabled on the Lambda client;
long invocations use RequestResponse with a 600 s read timeout, one chunk at a time.
"""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'scripts'))
from apply_direct_scheduler import payload as scheduler_payload  # noqa: E402

FN = 'justhodl-cds-desk'
REGION = 'us-east-1'
BUCKET = 'justhodl-dashboard-live'
PACKET_KEY = 'data/cds-desk.json'
BANK_KEY = 'data/warm/cds/bank.json.gz'
FIRST_DAY = '2024-09-03'
CHUNK_DAYS = 45
REPORT = ROOT / 'aws/ops/reports/latest/ops_6512_cds_desk_v120_long_context_publish.md'
HISTORY_KEY = 'data/cds-desk-history.json'
RECEIPT_KEY = 'data/ops/releases/%s.json' % FN
import os
import subprocess
THIS_COMMIT = os.environ.get('GITHUB_SHA') or subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True, cwd=ROOT).stdout.strip()
CFG = Config(retries={'max_attempts': 0}, read_timeout=600, connect_timeout=30)
lam = boto3.client('lambda', region_name=REGION, config=CFG)
sch = boto3.client('scheduler', region_name=REGION, config=Config(retries={'max_attempts': 2}))
s3 = boto3.client('s3', region_name=REGION)
out = {'ran_at': datetime.now(timezone.utc).isoformat()}
failed = []


def wait_for_function(max_wait_s=1800):
    """deploy-lambdas runs in a parallel workflow on the same push: wait until its release receipt names THIS commit
    (the function already exists and is Active with the OLD code, so State alone is not enough), then for Active."""
    t0 = time.time()
    last = None
    while time.time() - t0 < max_wait_s:
        try:
            receipt = json.loads(s3.get_object(Bucket=BUCKET, Key=RECEIPT_KEY)['Body'].read())
            cfg = lam.get_function_configuration(FunctionName=FN)
            last = (receipt.get('commit', '')[:8], cfg.get('State'), cfg.get('LastUpdateStatus'), cfg.get('CodeSha256'))
            if receipt.get('commit') == THIS_COMMIT and cfg.get('CodeSha256') == receipt.get('code_sha256') \
                    and cfg.get('State') == 'Active' and cfg.get('LastUpdateStatus') in (None, 'Successful'):
                return cfg
        except Exception as exc:  # noqa: BLE001 - receipt or function not there yet
            last = ('waiting', type(exc).__name__)
        time.sleep(20)
    raise RuntimeError('new code (%s) not deployed after %ds (last=%s)' % (THIS_COMMIT[:8], max_wait_s, last))


def ensure_schedule(function_arn):
    config = json.loads((ROOT / 'aws/lambdas' / FN / 'config.json').read_text())
    spec = config['eventbridge_scheduler']
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


def invoke(payload):
    r = lam.invoke(FunctionName=FN, InvocationType='RequestResponse', Payload=json.dumps(payload).encode())
    body = json.loads(r['Payload'].read() or b'{}')
    if r.get('FunctionError'):
        raise RuntimeError('invoke failed: %s' % str(body)[:600])
    if not body.get('ok'):
        raise RuntimeError('engine returned not ok: %s' % str(body)[:600])
    return body


try:
    cfg = wait_for_function()
    out['function'] = {'arn': cfg['FunctionArn'], 'runtime': cfg.get('Runtime'), 'timeout': cfg.get('Timeout'), 'memory': cfg.get('MemorySize'),
                       'code_sha': cfg.get('CodeSha256')}
    out['schedule'] = ensure_schedule(cfg['FunctionArn'])
    today = datetime.now(timezone.utc).date()
    t0 = time.time()
    out['daily'] = invoke({'action': 'daily'})
    out['daily']['elapsed_s'] = round(time.time() - t0, 1)

    packet = json.loads(s3.get_object(Bucket=BUCKET, Key=PACKET_KEY)['Body'].read())
    groups = packet.get('groups') or {}
    hist_meta = packet.get('history') or {}
    out['packet'] = {'version': packet.get('version'), 'as_of': packet.get('as_of'), 'generated_at': packet.get('generated_at'),
                     'n_days': (packet.get('source') or {}).get('n_days'),
                     'n_liquid': {g: v.get('n_liquid') for g, v in groups.items()},
                     'n_unpriced': {g: len(v.get('unpriced') or []) for g, v in groups.items()},
                     'indices': [(i.get('name'), i.get('spread_bp'), i.get('n_last')) for i in packet.get('indices') or []],
                     'decision': packet.get('decision'), 'breadth': packet.get('breadth'), 'term': {k: v for k, v in (packet.get('term') or {}).items() if k != 'inverted'},
                     'inverted': [(r['name'], r['spread_bp'], r['slope_1y5y_bp']) for r in (packet.get('term') or {}).get('inverted', [])][:8],
                     'n_wides': len(packet.get('wides_1y') or []), 'n_tights': len(packet.get('tights_1y') or []),
                     'wides': [(r['name'], r['spread_bp'], r['pct_rank_1y']) for r in (packet.get('wides_1y') or [])[:8]],
                     'activity_rows': len((packet.get('activity') or {}).get('rows') or []),
                     'activity_last': ((packet.get('activity') or {}).get('rows') or [None])[-1],
                     'history_meta': hist_meta}
    if packet.get('decision', {}).get('call') is not None or packet.get('decision', {}).get('sizing_eligible'):
        failed.append('doctrine breach: decision block is not descriptive')
    if packet.get('version', '0') < '1.2.0':
        failed.append('packet built by old code: version %s' % packet.get('version'))
    if (today - date.fromisoformat(packet.get('as_of') or FIRST_DAY)).days > 6:
        failed.append('packet as_of is stale: %s' % packet.get('as_of'))
    if (out['packet']['n_liquid'].get('sovereign') or 0) < 15 or (out['packet']['n_liquid'].get('us_corp') or 0) < 60:
        failed.append('liquid universe thinner than expected: %s' % out['packet']['n_liquid'])
    for k in ('term', 'wides_1y', 'tights_1y', 'activity'):
        if k not in packet:
            failed.append('packet missing block %s' % k)
    if out['packet']['activity_rows'] < 60:
        failed.append('activity block too short: %s rows' % out['packet']['activity_rows'])
    if hist_meta.get('error'):
        failed.append('history publish error: %s' % hist_meta['error'])
    unp = [u for g in groups.values() for u in g.get('unpriced', [])]
    if unp and not all('candidates' in u and 'last_priced_date' in u for u in unp):
        failed.append('unpriced records missing candidates/last_priced_date')
    out['packet']['unpriced_sample'] = [(u['name'], u['trades_30d'], u.get('candidates'), u.get('last_spread_bp'), u.get('last_priced_date')) for u in sorted(unp, key=lambda u: -u['trades_30d'])[:5]]

    # history file
    head = s3.head_object(Bucket=BUCKET, Key=HISTORY_KEY)
    hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HISTORY_KEY)['Body'].read())
    long = hist.get('long') or {}
    ser = long.get('series') or {}
    baa = ser.get('baa10y') or {}
    out['history'] = {'bytes': head.get('ContentLength'), 'as_of': hist.get('as_of'), 'n_names': len(hist.get('names') or {}),
                      'longest_name': max(((len(v['points']), k) for k, v in (hist.get('names') or {}).items()), default=None),
                      'long_series': {k: {'n_points': len(v.get('points') or []), 'last': v.get('last'), 'pct_rank_since_2006': v.get('pct_rank_since_2006'),
                                          'peaks': v.get('peaks')} for k, v in ser.items()},
                      'cdx_ig_map': baa.get('cdx_ig_map'), 'basis': {k: {'last': v.get('last'), 'mean_bp': v.get('mean_bp'), 'n': len(v.get('points') or [])} for k, v in (long.get('basis') or {}).items()},
                      'sources': long.get('sources'), 'limits': long.get('limits')}
    if out['history']['n_names'] < 150:
        failed.append('history file carries only %s names' % out['history']['n_names'])
    if not baa:
        failed.append('Baa-10Y long record missing (FRED BAA10Y unreachable and no cache)')
    else:
        gfc = next((p for p in baa.get('peaks', []) if p['episode'].startswith('GFC')), None)
        if not gfc or not (500 <= gfc['value'] <= 700) or not gfc['date'].startswith('2008-1'):
            failed.append('Baa-10Y GFC peak implausible: %s (expect ~616bp Dec-2008)' % gfc)
        if len(baa.get('points') or []) < 1000:
            failed.append('Baa-10Y weekly record too short: %s points' % len(baa.get('points') or []))
        m = baa.get('cdx_ig_map') or {}
        if not m or (m.get('fit') or {}).get('n', 0) < 150:
            failed.append('CDX IG -> Baa-10Y mapping missing or thin: %s' % json.dumps(m.get('fit')))
    for k in ('ofr_credit', 'ofr_em', 'gz_spread', 'ebp'):
        if k not in ser:
            failed.append('long series missing: %s' % k)
    if 'ofr_credit' in ser:
        gfc = next((p for p in ser['ofr_credit'].get('peaks', []) if p['episode'].startswith('GFC')), None)
        if not gfc or not gfc['date'].startswith('2008-1'):
            failed.append('OFR credit GFC peak implausible: %s' % gfc)
    for k in ('hy', 'ig'):
        if k not in (long.get('basis') or {}):
            failed.append('basis missing: %s' % k)
    not_live = {k: v.get('status') for k, v in (long.get('sources') or {}).items() if v.get('status') != 'live'}
    if not_live:
        out['history']['sources_not_live'] = not_live  # warning only: cache fallback is by design
    # regressions carried from ops 6511
    as_of = date.fromisoformat(packet['as_of'])
    rows = [r for g in groups.values() for r in g.get('rows', [])]
    stale = [(r['name'], r['last_date']) for r in rows if (as_of - date.fromisoformat(r['last_date'])).days > 10]
    tiny = [(r['name'], r['spread_bp']) for r in rows if (r.get('spread_bp') or 99) < 5]
    sov_names = [r['name'] for r in (groups.get('sovereign') or {}).get('rows', [])]
    dupes = sorted({n for n in sov_names if sov_names.count(n) > 1} | {n for n in sov_names if '(' in n})
    no_sign = [r['name'] for r in rows if r.get('sign_evidence') not in ('firm', 'inferred')]
    out['regressions'] = {'stale_rows': stale[:10], 'sub_5bp_rows': tiny[:10], 'sovereign_dupes': dupes, 'rows_without_sign_evidence': no_sign[:10]}
    for label, bad in (('stale levels shown as current', stale), ('sub-5bp "quotes" survived', tiny), ('duplicate / unmerged sovereigns', dupes),
                       ('rows without sign evidence', no_sign)):
        if bad:
            failed.append('%s: %s' % (label, bad[:6]))
except Exception as exc:  # noqa: BLE001
    failed.append('%s: %s' % (type(exc).__name__, str(exc)[:800]))

lines = ['# ops 6512 - justhodl-cds-desk v1.2.0 first publish: history file, long context, expansion blocks', '',
         '- commit: %s' % THIS_COMMIT,
         '- function: %s' % json.dumps(out.get('function'), default=str),
         '- schedule: %s' % json.dumps(out.get('schedule'), default=str),
         '- daily: %s' % json.dumps(out.get('daily'), default=str),
         '- packet: %s' % json.dumps(out.get('packet'), default=str),
         '- history: %s' % json.dumps(out.get('history'), default=str),
         '- regressions: %s' % json.dumps(out.get('regressions'), default=str), '']
if failed:
    lines += ['## FAILED', ''] + ['- ' + f for f in failed] + ['']
lines += ['## Full data', '', '```json', json.dumps(out, default=str, separators=(',', ':')), '```', '', 'VERDICT: %s' % ('FAIL' if failed else 'PASS')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:9]))
if failed:
    sys.exit(1)
