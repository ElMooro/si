"""ops 6513 - justhodl-cds-desk v1.3.0: full-universe republish (sovereign taxonomy, AI/software sectors, alias merge, new sign evidence).

v1.3.0 (a) lists the whole DTCC single-name universe (priced, active-but-unpriced, dormant) with a sovereign taxonomy
(region / tier / ISO3, coverage against the known sovereign CDS universe) and US sectors that split out "AI & semis" and
"Software & internet"; (b) folds reporter short codes and split spellings into one series per legal entity
(merge_aliases, run on every load); (c) adds data-driven sign evidence for the unsigned upfront: trailing 90-day two-coupon
intersection, the name's own firm level within 180 days (far-apart branches only) and index co-movement of the two
branches.  (c) only changes stored rows when a day is re-aggregated, so this run re-walks every file date
with action=backfill from 2024-09-03 (one 540 s chunk at a time, continuing from next_start) and then runs one daily pass;
the bank prune is lifted from 400 to 2000 days so the whole tape stays in every name's history.

Waits until deploy-lambdas (parallel workflow, same push) has published the release receipt for THIS commit.
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
REPORT = ROOT / 'aws/ops/reports/latest/ops_6513_cds_desk_v130_universe_republish.md'
MAX_CHUNKS = 24   # whole tape 2024-09-03 -> today is ~540 file days; one 540 s Lambda chunk covers ~45-120 of them
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
    # 1) re-aggregate the WHOLE public tape under the v1.3.0 sign rules and the lifted 400-day prune (v1.2 kept only ~13 months;
    #    v1.3 keeps every file day since 2024-09-03 so each name's history runs from its first public print)
    start = FIRST_DAY
    chunks = []
    for _ in range(MAX_CHUNKS):
        t0 = time.time()
        r = invoke({'action': 'backfill', 'start': start, 'end': today.isoformat()})
        chunks.append({'start': start, 'files': r.get('files'), 'last_file': r.get('last_file'), 'exhausted': r.get('budget_exhausted'),
                       'aliases_merged': r.get('aliases_merged'), 'elapsed_s': round(time.time() - t0, 1)})
        if not r.get('budget_exhausted') or not r.get('next_start'):
            break
        start = r['next_start']
    else:
        failed.append('re-aggregation did not finish within %d chunks (last=%s)' % (MAX_CHUNKS, chunks[-1]))
    out['reaggregate'] = chunks
    # 2) one daily pass so the newest posted file is in and the publish summary comes from the daily path
    t0 = time.time()
    out['daily'] = invoke({'action': 'daily'})
    out['daily']['elapsed_s'] = round(time.time() - t0, 1)

    packet = json.loads(s3.get_object(Bucket=BUCKET, Key=PACKET_KEY)['Body'].read())
    groups = packet.get('groups') or {}
    hist_meta = packet.get('history') or {}
    sov = groups.get('sovereign') or {}
    us = groups.get('us_corp') or {}
    sectors = {s['sector']: s['n'] for s in us.get('sectors') or [] if isinstance(s, dict) and 'sector' in s}
    rows = [r for g in groups.values() for r in g.get('rows', [])]
    out['packet'] = {'version': packet.get('version'), 'as_of': packet.get('as_of'), 'generated_at': packet.get('generated_at'),
                     'n_days': (packet.get('source') or {}).get('n_days'),
                     'n_liquid': {g: v.get('n_liquid') for g, v in groups.items()},
                     'n_unpriced': {g: len(v.get('unpriced') or []) for g, v in groups.items()},
                     'n_dormant': {g: len(v.get('dormant') or []) for g, v in groups.items()},
                     'n_tracked': {g: v.get('n_tracked') for g, v in groups.items()},
                     'sovereign_coverage': sov.get('coverage'),
                     'sovereign_regions': sov.get('regions'), 'sovereign_tiers': sov.get('tiers'),
                     'sovereign_universe_n': len(sov.get('universe') or []),
                     'us_sectors': sectors,
                     'sign_sources': dict(sorted(__import__('collections').Counter(r.get('anchor_source') for r in rows).items(), key=lambda kv: -kv[1])),
                     'indices': [(i.get('name'), i.get('spread_bp'), i.get('n_last')) for i in packet.get('indices') or []],
                     'decision': packet.get('decision'), 'breadth': packet.get('breadth'),
                     'history_meta': hist_meta}
    if packet.get('decision', {}).get('call') is not None or packet.get('decision', {}).get('sizing_eligible'):
        failed.append('doctrine breach: decision block is not descriptive')
    if tuple(int(x) for x in str(packet.get('version', '0')).split('.')[:3]) < (1, 3, 0):
        failed.append('packet built by old code: version %s' % packet.get('version'))
    if (today - date.fromisoformat(packet.get('as_of') or FIRST_DAY)).days > 6:
        failed.append('packet as_of is stale: %s' % packet.get('as_of'))
    first_day = (packet.get('source') or {}).get('first_day')
    if not first_day or first_day > '2024-09-10':
        failed.append('bank history does not start at the first public file: first_day=%s' % first_day)
    if (out['packet']['n_liquid'].get('sovereign') or 0) < 15 or (out['packet']['n_liquid'].get('us_corp') or 0) < 60:
        failed.append('liquid universe thinner than expected: %s' % out['packet']['n_liquid'])
    cov = sov.get('coverage') or {}
    if not cov or (cov.get('known') or 0) < 100 or (cov.get('in_tape') or 0) < 45:
        failed.append('sovereign coverage block missing or thin: %s' % cov)
    if not sov.get('universe') or not all('status' in e and 'region' in e and 'tier' in e for e in sov['universe']):
        failed.append('sovereign universe block missing status/region/tier')
    for s in ('AI & semis', 'Software & internet'):
        if not sectors.get(s):
            failed.append('US sector missing from packet: %s (have %s)' % (s, sorted(sectors)))
    if (out['packet']['n_tracked'].get('us_corp') or 0) < 200:
        failed.append('US corporate tracked universe thinner than expected: %s' % out['packet']['n_tracked'])
    for k in ('term', 'wides_1y', 'tights_1y', 'activity', 'method'):
        if k not in packet:
            failed.append('packet missing block %s' % k)
    if hist_meta.get('error'):
        failed.append('history publish error: %s' % hist_meta['error'])
    unp = [u for g in groups.values() for u in g.get('unpriced', [])]
    if unp and not all('candidates' in u and 'last_priced_date' in u for u in unp):
        failed.append('unpriced records missing candidates/last_priced_date')
    out['packet']['unpriced_sample'] = [(u['name'], u['trades_30d'], u.get('candidates'), u.get('last_spread_bp'), u.get('last_priced_date')) for u in sorted(unp, key=lambda u: -u['trades_30d'])[:8]]
    # alias merge must leave no reporter short codes as separate names
    names = [r['name'] for g in groups.values() for b in ('rows', 'unpriced', 'dormant') for r in g.get(b, [])]
    shortcodes = [n for n in names if n.upper() in ('ORACLECORP', 'INTELSC', 'APACORP', 'GECUS', 'TWDIC', 'WELLS FARGO&COMPANY')]
    if shortcodes:
        failed.append('alias merge did not fold reporter short codes: %s' % shortcodes)

    # history file: every listed name (priced + unpriced + dormant) plus branches for the unpriced
    head = s3.head_object(Bucket=BUCKET, Key=HISTORY_KEY)
    hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HISTORY_KEY)['Body'].read())
    long = hist.get('long') or {}
    ser = long.get('series') or {}
    hn = hist.get('names') or {}
    out['history'] = {'bytes': head.get('ContentLength'), 'as_of': hist.get('as_of'), 'n_names': len(hn),
                      'n_with_branches': sum(1 for v in hn.values() if v.get('branches')),
                      'longest_name': max(((len(v['points']), k) for k, v in hn.items()), default=None),
                      'long_series': {k: {'n_points': len(v.get('points') or []), 'last': v.get('last'), 'pct_rank_since_2006': v.get('pct_rank_since_2006')} for k, v in ser.items()},
                      'sources': long.get('sources'), 'limits': long.get('limits')}
    if out['history']['n_names'] < 300:
        failed.append('history file carries only %s names (v1.3.0 lists the whole universe)' % out['history']['n_names'])
    if unp and out['history']['n_with_branches'] < min(20, len(unp) // 2):
        failed.append('history file carries branches for only %s unpriced names' % out['history']['n_with_branches'])
    for k in ('baa10y', 'ofr_credit', 'ofr_em', 'gz_spread', 'ebp'):
        if k not in ser:
            failed.append('long series missing: %s' % k)
    not_live = {k: v.get('status') for k, v in (long.get('sources') or {}).items() if v.get('status') != 'live'}
    if not_live:
        out['history']['sources_not_live'] = not_live  # warning only: cache fallback is by design
    # regressions carried from ops 6511/6512
    as_of = date.fromisoformat(packet['as_of'])
    stale = [(r['name'], r['last_date']) for r in rows if (as_of - date.fromisoformat(r['last_date'])).days > 10]
    tiny = [(r['name'], r['spread_bp']) for r in rows if (r.get('spread_bp') or 99) < 5]
    sov_names = [r['name'] for r in sov.get('rows', [])]
    dupes = sorted({n for n in sov_names if sov_names.count(n) > 1} | {n for n in sov_names if '(' in n})
    no_sign = [r['name'] for r in rows if r.get('sign_evidence') not in ('firm', 'inferred')]
    out['regressions'] = {'stale_rows': stale[:10], 'sub_5bp_rows': tiny[:10], 'sovereign_dupes': dupes, 'rows_without_sign_evidence': no_sign[:10]}
    for label, bad in (('stale levels shown as current', stale), ('sub-5bp "quotes" survived', tiny), ('duplicate / unmerged sovereigns', dupes),
                       ('rows without sign evidence', no_sign)):
        if bad:
            failed.append('%s: %s' % (label, bad[:6]))
except Exception as exc:  # noqa: BLE001
    failed.append('%s: %s' % (type(exc).__name__, str(exc)[:800]))

lines = ['# ops 6513 - justhodl-cds-desk v1.3.0 universe republish (taxonomy, AI/software sectors, alias merge, sign evidence)', '',
         '- commit: %s' % THIS_COMMIT,
         '- function: %s' % json.dumps(out.get('function'), default=str),
         '- schedule: %s' % json.dumps(out.get('schedule'), default=str),
         '- reaggregate: %s' % json.dumps(out.get('reaggregate'), default=str),
         '- daily: %s' % json.dumps(out.get('daily'), default=str),
         '- packet: %s' % json.dumps(out.get('packet'), default=str),
         '- history: %s' % json.dumps(out.get('history'), default=str),
         '- regressions: %s' % json.dumps(out.get('regressions'), default=str), '']
if failed:
    lines += ['## FAILED', ''] + ['- ' + f for f in failed] + ['']
lines += ['## Full data', '', '```json', json.dumps(out, default=str, separators=(',', ':')), '```', '', 'VERDICT: %s' % ('FAIL' if failed else 'PASS')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:10]))
if failed:
    sys.exit(1)
