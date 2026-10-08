"""Auction desk v1.6.0 - repair the retained bank, capture the complete FiscalData history, grade.

Desk v1.6.0 adds the interpretation layer (hit ratios, stop-vs-median dispersion, par set-up and
auction-day move, curve demand map, watch flags, crisis fingerprints vs 2007-2023 windows). The
retained bank (data/warm/treasury-auctions/history.json.gz) was captured before the tendered,
median and original-term fields were normalised, and the full bank was a slim 20-field capture,
so the first run must:
  1. invoke justhodl-auction-desk with backfill=true (repairs identity + supplements in place,
     re-fetching FiscalData for the bank window) and history=true (forces a complete recapture
     of the full bank, which the loader also self-triggers when original_security_term is absent);
  2. invoke justhodl-auction-grader (v2.0.0, projects the desk grades for the intelligence pages);
  3. read back data/auction-desk.json, the delivery view, data/auction-grades.json and the full
     bank, and verify curve_map / alerts / crisis_fingerprints are complete.
Lambda client has retries disabled so a slow desk run cannot be re-invoked by botocore.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
REPORT = ROOT / 'aws/ops/reports/latest/ops_6508_auction_desk_interpretation_backfill.md'
BUCKET = 'justhodl-dashboard-live'
DESK = 'justhodl-auction-desk'
GRADER = 'justhodl-auction-grader'
CFG = Config(retries={'max_attempts': 0}, read_timeout=600, connect_timeout=30)
s3 = boto3.client('s3')
lam = boto3.client('lambda', config=CFG)
NOW = datetime.now(timezone.utc)
out = {'ran_at': NOW.isoformat()}
failed = []


def get_json(key, gz=False):
    body = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
    return json.loads(gzip.decompress(body) if gz else body)


def invoke(fn, payload):
    r = lam.invoke(FunctionName=fn, InvocationType='RequestResponse', Payload=json.dumps(payload).encode())
    body = json.loads(r['Payload'].read() or b'{}')
    if r.get('FunctionError'):
        failed.append('%s invoke failed: %s' % (fn, str(body)[:400]))
    return body


for fn in (DESK, GRADER):
    cfg = lam.get_function_configuration(FunctionName=fn)
    out[fn + '_deployed'] = {'last_modified': cfg['LastModified'], 'timeout': cfg['Timeout'], 'memory': cfg['MemorySize']}

out['desk_invoke'] = invoke(DESK, {'backfill': True, 'history': True})
desk = get_json('data/auction-desk.json')
out['desk'] = {k: desk.get(k) for k in ('version', 'generated_at', 'interpretation_contract', 'freshness', 'wi_tail')}
if desk.get('version') != '1.6.0':
    failed.append('desk version %r, expected 1.6.0' % desk.get('version'))
cm = desk.get('curve_map') or {}
out['curve_map'] = [{k: r.get(k) for k in ('bucket', 'auction_date', 'grade', 'score', 'pd_hit_pct', 'high_minus_median_bp', 'streak_string', 'consecutive_weak')}
                    for r in cm.get('rows') or []]
if len(out['curve_map']) < 6:
    failed.append('curve_map has %d rows' % len(out['curve_map']))
al = desk.get('alerts') or {}
out['alerts'] = {'n_items': len(al.get('items') or []), 'n_watch': al.get('n_watch'), 'items': (al.get('items') or [])[:12]}
fp = desk.get('crisis_fingerprints') or {}
out['crisis_fingerprints'] = {'status': fp.get('status'), 'error': fp.get('error'), 'reason': fp.get('reason'), 'current': fp.get('current'),
                              'nearest': fp.get('nearest'), 'episodes': [{k: e.get(k) for k in ('id', 'n_auctions', 'mean_btc', 'mean_pd_hit_pct', 'mean_high_minus_median_bp', 'max_high_minus_median_bp', 'distance_from_current')} for e in fp.get('episodes') or []],
                              'timeline_points': len(fp.get('timeline') or [])}
if fp.get('status') != 'complete':
    failed.append('crisis_fingerprints status %r: %s' % (fp.get('status'), fp.get('error') or fp.get('reason')))
today = desk.get('today') or {}
out['today'] = {'date': today.get('date'), 'headline': (today.get('verdict') or {}).get('headline'),
                'read': ((today.get('verdict') or {}).get('read') or {}).get('headline'),
                'ai_headline': (today.get('ai_note') or {}).get('headline'),
                'auctions': [{k: a.get(k) for k in ('term', 'type', 'grade', 'pd_hit_pct', 'indirect_hit_pct', 'high_minus_median_bp', 'concession_5s_bp', 'reaction_bp')} for a in today.get('auctions') or []]}
missing_read = [a.get('cusip') for a in (desk.get('auctions') or []) if not isinstance(a.get('read'), dict)]
out['auctions_without_read'] = len(missing_read)
view_loc = get_json('data/auction-desk-view.json')
out['view_locator'] = {k: view_loc.get(k) for k in ('contract', 'generated_at')}
try:
    view = get_json(view_loc['view']['key'])
    out['view'] = {'has_curve_map': bool(view.get('curve_map')), 'has_alerts': bool(view.get('alerts')),
                   'fingerprints_status': (view.get('crisis_fingerprints') or {}).get('status'),
                   'today_auctions_with_read': sum(1 for a in (view.get('today') or {}).get('auctions') or [] if a.get('read'))}
    if not out['view']['has_curve_map'] or out['view']['fingerprints_status'] != 'complete':
        failed.append('delivery view lacks interpretation sections: %s' % out['view'])
except Exception as exc:  # noqa: BLE001
    failed.append('view read failed: %s' % exc)

out['grader_invoke'] = invoke(GRADER, {})
grades = get_json('data/auction-grades.json')
out['grades'] = {k: grades.get(k) for k in ('version', 'generated_at', 'input_desk_generated_at', 'summary', 'n_graded', 'n_with_letter', 'telegram_sent')}
if grades.get('version') != '2.0.0':
    failed.append('grader version %r' % grades.get('version'))

full = get_json('data/warm/treasury-auctions/history-full.json.gz', gz=True)
rows = sorted((full.get('rows') or {}).values(), key=lambda r: str(r.get('auction_date') or ''))
out['full_bank'] = {'rows': len(rows), 'first': full.get('first'), 'last': full.get('last'), 'error': full.get('error'),
                    'with_original_term': sum(1 for r in rows if r.get('original_security_term')),
                    'with_pd_tendered': sum(1 for r in rows if r.get('pd_tendered') is not None),
                    'with_median': sum(1 for r in rows if r.get('median_yield') is not None or r.get('median_discount_rate') is not None),
                    'last_capture': full.get('last_capture')}
if full.get('error') or (rows and out['full_bank']['with_original_term'] < len(rows) * 0.9):
    failed.append('full bank incomplete: %s' % json.dumps({k: v for k, v in out['full_bank'].items() if k != 'last_capture'}, default=str))
bank = get_json('data/warm/treasury-auctions/history.json.gz', gz=True)
brows = list((bank.get('records') or {}).values())
out['bank'] = {'rows': len(brows), 'with_original_term': sum(1 for r in brows if r.get('original_security_term')),
               'with_pd_tendered': sum(1 for r in brows if r.get('pd_tendered') is not None),
               'with_error': sum(1 for r in brows if r.get('error')), 'bank_error': bank.get('error')}
if bank.get('error'):
    failed.append('bank error: %s' % bank.get('error'))

lines = ['# ops 6508 - auction desk v1.6.0 interpretation backfill + grader v2.0.0', '',
         '- deployed: %s' % json.dumps({k: v for k, v in out.items() if k.endswith('_deployed')}, default=str),
         '- desk: %s' % json.dumps(out['desk'], default=str),
         '- today: %s' % json.dumps(out['today'], default=str),
         '- alerts: n_items=%s n_watch=%s' % (out['alerts']['n_items'], out['alerts']['n_watch']),
         '- fingerprints: status=%s nearest=%s' % (out['crisis_fingerprints']['status'], json.dumps(out['crisis_fingerprints']['nearest'], default=str)),
         '- view: %s' % json.dumps(out.get('view'), default=str),
         '- grades: %s' % json.dumps(out['grades'], default=str),
         '- full bank: %s' % json.dumps(out['full_bank'], default=str),
         '- bank: %s' % json.dumps(out['bank'], default=str), '',
         '## Curve map', '']
for r in out['curve_map']:
    lines.append('- %s %s %s %s · dealer fill %s · stop−median %s · streak %s' % (r['bucket'], r['auction_date'], r['grade'], r['score'], r['pd_hit_pct'], r['high_minus_median_bp'], r['streak_string']))
lines += ['', '## Watch flags', ''] + ['- %s %s %s' % (i.get('date'), i.get('severity'), i.get('text')) for i in out['alerts']['items']]
lines += ['', '## Episodes', ''] + ['- %s' % json.dumps(e, default=str) for e in out['crisis_fingerprints']['episodes']]
if failed:
    lines += ['', '## FAILED', ''] + ['- ' + f for f in failed]
lines += ['', '## Full data', '', '```json', json.dumps(out, default=str, separators=(',', ':')), '```', '',
          'VERDICT: %s' % ('FAIL' if failed else 'PASS')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:14]))
if failed:
    sys.exit(1)
