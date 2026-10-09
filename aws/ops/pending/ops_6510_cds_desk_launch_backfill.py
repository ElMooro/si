"""ops 6510 - launch justhodl-cds-desk: wait for the new function (created by deploy-lambdas in the same push),
make sure its EventBridge Scheduler schedule exists, rebuild the bank from the first public DTCC day
(2024-09-03) to today in bounded chunks, run one daily pass, then verify the published packet.

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
REPORT = ROOT / 'aws/ops/reports/latest/ops_6510_cds_desk_launch_backfill.md'
CFG = Config(retries={'max_attempts': 0}, read_timeout=600, connect_timeout=30)
lam = boto3.client('lambda', region_name=REGION, config=CFG)
sch = boto3.client('scheduler', region_name=REGION, config=Config(retries={'max_attempts': 2}))
s3 = boto3.client('s3', region_name=REGION)
out = {'ran_at': datetime.now(timezone.utc).isoformat(), 'chunks': []}
failed = []


def wait_for_function(max_wait_s=1500):
    """deploy-lambdas creates the function in a parallel workflow; a new function is State=Pending first."""
    t0 = time.time()
    last = None
    while time.time() - t0 < max_wait_s:
        try:
            cfg = lam.get_function_configuration(FunctionName=FN)
            last = (cfg.get('State'), cfg.get('LastUpdateStatus'))
            if cfg.get('State') == 'Active' and cfg.get('LastUpdateStatus') in (None, 'Successful'):
                return cfg
        except lam.exceptions.ResourceNotFoundException:
            last = ('missing', None)
        time.sleep(20)
    raise RuntimeError('function not Active after %ds (last=%s)' % (max_wait_s, last))


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
except Exception as exc:  # noqa: BLE001
    failed.append('%s: %s' % (type(exc).__name__, str(exc)[:800]))

lines = ['# ops 6510 - justhodl-cds-desk launch + DTCC backfill', '',
         '- function: %s' % json.dumps(out.get('function'), default=str),
         '- schedule: %s' % json.dumps(out.get('schedule'), default=str),
         '- chunks: %d' % len(out['chunks']),
         '- packet: %s' % json.dumps(out.get('packet'), default=str), '']
if failed:
    lines += ['## FAILED', ''] + ['- ' + f for f in failed] + ['']
lines += ['## Full data', '', '```json', json.dumps(out, default=str, separators=(',', ':')), '```', '', 'VERDICT: %s' % ('FAIL' if failed else 'PASS')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:7]))
if failed:
    sys.exit(1)
