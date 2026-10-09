"""ops 6511 - rebuild justhodl-cds-desk after the v1.1.0 fixes found on the first live packet:
mis-filed "quoted" spreads (CoreWeave 1.25bp, Sabre 18bp, OneMain 100bp) now validated against the print's own
upfront / the name's trailing anchor; levels older than 10 days are no longer shown as current (Clear Channel at
a 2025-11 level); 1d/1w/1m/3m changes are dated; sovereign postfix aliases merged (TURKEY REPUBLIC OF, AFRISUD).

Waits until deploy-lambdas (parallel workflow, same push) has published the release receipt for THIS commit, so the
rebuild runs on the new code, then rebuilds the bank from 2024-09-03 in bounded chunks, runs one daily pass and
verifies the packet with regression checks for every defect above.

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
REPORT = ROOT / 'aws/ops/reports/latest/ops_6511_cds_desk_rebuild_after_quote_stale_fixes.md'
RECEIPT_KEY = 'data/ops/releases/%s.json' % FN
import os
import subprocess
THIS_COMMIT = os.environ.get('GITHUB_SHA') or subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True, cwd=ROOT).stdout.strip()
CFG = Config(retries={'max_attempts': 0}, read_timeout=600, connect_timeout=30)
lam = boto3.client('lambda', region_name=REGION, config=CFG)
sch = boto3.client('scheduler', region_name=REGION, config=Config(retries={'max_attempts': 2}))
s3 = boto3.client('s3', region_name=REGION)
out = {'ran_at': datetime.now(timezone.utc).isoformat(), 'chunks': []}
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
    start = date.fromisoformat(FIRST_DAY)
    action = 'rebuild'
    while start <= today:
        end = min(start + timedelta(days=CHUNK_DAYS - 1), today)
        t0 = time.time()
        body = invoke({'action': action, 'start': start.isoformat(), 'end': end.isoformat()})
        action = 'backfill'
        rec = {'start': start.isoformat(), 'end': end.isoformat(), 'files': body.get('files'), 'last_file': body.get('last_file'),
               'as_of': body.get('as_of'), 'n_liquid': body.get('n_liquid'), 'budget_exhausted': body.get('budget_exhausted'),
               'elapsed_s': round(time.time() - t0, 1)}
        out['chunks'].append(rec)
        print('chunk', json.dumps(rec))
        if body.get('budget_exhausted') and body.get('next_start'):
            start = date.fromisoformat(body['next_start'])
        else:
            start = end + timedelta(days=1)
    out['daily'] = invoke({'action': 'daily'})

    packet = json.loads(s3.get_object(Bucket=BUCKET, Key=PACKET_KEY)['Body'].read())
    head = s3.head_object(Bucket=BUCKET, Key=BANK_KEY)
    groups = packet.get('groups') or {}
    out['packet'] = {'version': packet.get('version'), 'as_of': packet.get('as_of'), 'generated_at': packet.get('generated_at'),
                     'n_days': (packet.get('source') or {}).get('n_days'), 'first_day': (packet.get('source') or {}).get('first_day'),
                     'n_liquid': {g: v.get('n_liquid') for g, v in groups.items()},
                     'n_unpriced': {g: len(v.get('unpriced') or []) for g, v in groups.items()},
                     'indices': [(i.get('name'), i.get('spread_bp'), i.get('n_last')) for i in packet.get('indices') or []],
                     'sov_top': [(r.get('name'), r.get('spread_bp'), r.get('spread_basis')) for r in (groups.get('sovereign') or {}).get('rows', [])[:5]],
                     'decision': packet.get('decision'), 'breadth': packet.get('breadth'), 'bank_bytes': head.get('ContentLength')}
    if packet.get('decision', {}).get('call') is not None or packet.get('decision', {}).get('sizing_eligible'):
        failed.append('doctrine breach: decision block is not descriptive')
    if (out['packet']['n_days'] or 0) < 200:
        failed.append('bank too short: %s days' % out['packet']['n_days'])
    if (out['packet']['n_liquid'].get('sovereign') or 0) < 15 or (out['packet']['n_liquid'].get('us_corp') or 0) < 60:
        failed.append('liquid universe thinner than expected: %s' % out['packet']['n_liquid'])
    if len(out['packet']['indices']) < 5:
        failed.append('index strip incomplete: %s' % out['packet']['indices'])
    if (today - date.fromisoformat(packet.get('as_of') or FIRST_DAY)).days > 6:
        failed.append('packet as_of is stale: %s' % packet.get('as_of'))
    if packet.get('version', '0') < '1.1.0':
        failed.append('packet built by old code: version %s' % packet.get('version'))
    # regression checks for the defects this ops exists to fix
    as_of = date.fromisoformat(packet['as_of'])
    rows = [r for g in groups.values() for r in g.get('rows', [])]
    stale = [(r['name'], r['last_date']) for r in rows if (as_of - date.fromisoformat(r['last_date'])).days > 10]
    tiny = [(r['name'], r['spread_bp']) for r in rows if (r.get('spread_bp') or 99) < 5]
    sov_names = [r['name'] for r in (groups.get('sovereign') or {}).get('rows', [])]
    dupes = sorted({n for n in sov_names if sov_names.count(n) > 1} | {n for n in sov_names if '(' in n})
    wild = [(m['name'], m['chg_1d_bp']) for g in groups.values() for m in g.get('movers_1d', []) if abs(m.get('chg_1d_bp') or 0) > 500]
    no_sign = [r['name'] for r in rows if r.get('sign_evidence') not in ('firm', 'inferred')]
    out['regressions'] = {'stale_rows': stale[:10], 'sub_5bp_rows': tiny[:10], 'sovereign_dupes': dupes, 'wild_movers': wild[:10], 'rows_without_sign_evidence': no_sign[:10]}
    for label, bad in (('stale levels shown as current', stale), ('sub-5bp "quotes" survived', tiny), ('duplicate / unmerged sovereigns', dupes),
                       ('movers with >500bp 1d change', wild), ('rows without sign evidence', no_sign)):
        if bad:
            failed.append('%s: %s' % (label, bad[:6]))
except Exception as exc:  # noqa: BLE001
    failed.append('%s: %s' % (type(exc).__name__, str(exc)[:800]))

lines = ['# ops 6511 - justhodl-cds-desk rebuild after quote-validation + staleness fixes (v1.1.0)', '',
         '- commit: %s' % THIS_COMMIT,
         '- function: %s' % json.dumps(out.get('function'), default=str),
         '- schedule: %s' % json.dumps(out.get('schedule'), default=str),
         '- chunks: %d' % len(out['chunks']),
         '- packet: %s' % json.dumps(out.get('packet'), default=str),
         '- regressions: %s' % json.dumps(out.get('regressions'), default=str), '']
if failed:
    lines += ['## FAILED', ''] + ['- ' + f for f in failed] + ['']
lines += ['## Full data', '', '```json', json.dumps(out, default=str, separators=(',', ':')), '```', '', 'VERDICT: %s' % ('FAIL' if failed else 'PASS')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:7]))
if failed:
    sys.exit(1)
