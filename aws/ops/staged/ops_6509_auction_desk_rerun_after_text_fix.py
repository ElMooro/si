"""Re-run justhodl-auction-desk and justhodl-auction-grader after the interpretation text fix
(ordinal percentiles, capitalised percentile sentence) deployed with auction_interpretation.py.
No backfill: the bank and full bank were repaired by ops 6508. Retries disabled on the Lambda client.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
REPORT = ROOT / 'aws/ops/reports/latest/ops_6509_auction_desk_rerun_after_text_fix.md'
BUCKET = 'justhodl-dashboard-live'
CFG = Config(retries={'max_attempts': 0}, read_timeout=600, connect_timeout=30)
s3 = boto3.client('s3')
lam = boto3.client('lambda', config=CFG)
out = {'ran_at': datetime.now(timezone.utc).isoformat()}
failed = []


def invoke(fn, payload):
    r = lam.invoke(FunctionName=fn, InvocationType='RequestResponse', Payload=json.dumps(payload).encode())
    body = json.loads(r['Payload'].read() or b'{}')
    if r.get('FunctionError'):
        failed.append('%s invoke failed: %s' % (fn, str(body)[:400]))
    return body


out['desk_invoke'] = invoke('justhodl-auction-desk', {})
desk = json.loads(s3.get_object(Bucket=BUCKET, Key='data/auction-desk.json')['Body'].read())
today = desk.get('today') or {}
out['desk'] = {'version': desk.get('version'), 'generated_at': desk.get('generated_at'),
               'fingerprints': (desk.get('crisis_fingerprints') or {}).get('status'),
               'curve_rows': len(((desk.get('curve_map') or {}).get('rows') or [])),
               'reads': [(a.get('term'), (a.get('read') or {}).get('what_it_means')) for a in today.get('auctions') or []]}
if desk.get('version') != '1.6.0' or out['desk']['fingerprints'] != 'complete' or out['desk']['curve_rows'] < 6:
    failed.append('desk packet incomplete: %s' % json.dumps(out['desk'], default=str)[:300])
if any('th percentile' in (t or '') and ('1th' in (t or '') or '2th' in (t or '') or '3th' in (t or '')) for _, t in out['desk']['reads']):
    failed.append('ordinal fix not live')
out['grader_invoke'] = invoke('justhodl-auction-grader', {})
grades = json.loads(s3.get_object(Bucket=BUCKET, Key='data/auction-grades.json')['Body'].read())
out['grades'] = {k: grades.get(k) for k in ('version', 'generated_at', 'input_desk_generated_at', 'n_graded', 'n_with_letter', 'row_basis', 'telegram_sent')}
if grades.get('input_desk_generated_at') != desk.get('generated_at'):
    failed.append('grader did not consume the fresh desk packet')

lines = ['# ops 6509 - auction desk + grader re-run after interpretation text fix', '',
         '- desk: %s' % json.dumps(out['desk'], default=str), '- grades: %s' % json.dumps(out['grades'], default=str), '']
if failed:
    lines += ['## FAILED', ''] + ['- ' + f for f in failed] + ['']
lines += ['## Full data', '', '```json', json.dumps(out, default=str, separators=(',', ':')), '```', '', 'VERDICT: %s' % ('FAIL' if failed else 'PASS')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:5]))
if failed:
    sys.exit(1)
