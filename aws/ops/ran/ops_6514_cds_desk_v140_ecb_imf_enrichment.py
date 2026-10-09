"""ops 6514 - justhodl-cds-desk v1.4.0: two new free sources (ECB SovCISS, IMF WEO) and the history-modal redraw.

v1.4.0 adds (a) the ECB's daily Composite Indicator of Sovereign Stress (SovCISS, dataset CISS, keyless csvdata) for the
euro area and eleven member states since 2000 — the only free, country-specific sovereign-market stress record that
reaches 2008 and 2011 — published as long series `sovciss_<ISO3>` / `sovciss_EA` in data/cds-desk-history.json and used
by the page as the 2006 → record for those sovereigns; (b) IMF World Economic Outlook fundamentals (DataMapper API,
keyless JSON: gross debt, fiscal balance, current account, growth, inflation) attached to every sovereign universe
entry, row, unpriced and dormant record (`fund`) plus a `fundamentals` block in the packet.  Neither needs a bank
re-walk: one daily pass publishes both.  The page change (the name's tape record is drawn as a range glyph in the
right margin instead of a time-compressed line on the 20-year axis) ships in the same push via Pages.

Waits until deploy-lambdas (parallel workflow, same push) has published the release receipt for THIS commit.
Descriptive engine only (decision.call is None, sizing_eligible False).  Retries disabled on the Lambda client.
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

FN = 'justhodl-cds-desk'
REGION = 'us-east-1'
BUCKET = 'justhodl-dashboard-live'
PACKET_KEY = 'data/cds-desk.json'
HISTORY_KEY = 'data/cds-desk-history.json'
REPORT = ROOT / 'aws/ops/reports/latest/ops_6514_cds_desk_v140_ecb_imf_enrichment.md'
RECEIPT_KEY = 'data/ops/releases/%s.json' % FN
ECB_ISO3 = ('AUT', 'BEL', 'DEU', 'ESP', 'FIN', 'FRA', 'GRC', 'IRL', 'ITA', 'NLD', 'PRT', 'EA')
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
    sov = groups.get('sovereign') or {}
    fund = packet.get('fundamentals') or {}
    universe = sov.get('universe') or []
    with_fund = [e for e in universe if e.get('fund')]
    rows = [r for g in groups.values() for r in g.get('rows', [])]
    sov_rows_fund = [r for r in sov.get('rows', []) if r.get('fund')]
    sample = {e['iso3']: {k: e['fund'].get(k) for k in ('year', 'debt_gdp', 'debt_gdp_weo', 'fiscal_bal_gdp', 'cab_gdp', 'gdp_growth', 'inflation')}
              for e in universe if e.get('fund') and e.get('iso3') in ('USA', 'JPN', 'ITA', 'DEU', 'FRA', 'ESP', 'KOR', 'CHN', 'TWN', 'ARG', 'TUR', 'NGA', 'EGY')}
    out['packet'] = {'version': packet.get('version'), 'as_of': packet.get('as_of'), 'generated_at': packet.get('generated_at'),
                     'n_days': (packet.get('source') or {}).get('n_days'),
                     'n_liquid': {g: v.get('n_liquid') for g, v in groups.items()}, 'n_tracked': {g: v.get('n_tracked') for g, v in groups.items()},
                     'sovereign_coverage': sov.get('coverage'),
                     'fundamentals': {k: v for k, v in fund.items() if k != 'indicators'},
                     'universe_with_fund': len(with_fund), 'universe_without_fund': [e['name'] for e in universe if not e.get('fund')],
                     'sovereign_rows_with_fund': len(sov_rows_fund), 'sovereign_rows': len(sov.get('rows', [])),
                     'fund_sample': sample, 'decision': packet.get('decision'), 'history_meta': packet.get('history')}
    if packet.get('decision', {}).get('call') is not None or packet.get('decision', {}).get('sizing_eligible'):
        failed.append('doctrine breach: decision block is not descriptive')
    if tuple(int(x) for x in str(packet.get('version', '0')).split('.')[:3]) < (1, 4, 0):
        failed.append('packet built by old code: version %s' % packet.get('version'))
    if (today - date.fromisoformat(packet.get('as_of') or '2024-09-03')).days > 6:
        failed.append('packet as_of is stale: %s' % packet.get('as_of'))
    if fund.get('error'):
        failed.append('fundamentals error: %s' % fund['error'])
    if (fund.get('n_sovereigns') or 0) < 100 or len(with_fund) < 100:
        failed.append('IMF fundamentals attached to only %s / %s sovereigns' % (len(with_fund), len(universe)))
    if sov.get('rows') and len(sov_rows_fund) < 0.9 * len(sov['rows']):
        failed.append('IMF fundamentals missing on priced sovereign rows: %s / %s' % (len(sov_rows_fund), len(sov['rows'])))
    for iso in ('USA', 'JPN', 'ITA', 'DEU', 'KOR', 'CHN'):
        f = (sample.get(iso) or {})
        if f.get('debt_gdp') is None:
            failed.append('IMF debt/GDP missing for %s' % iso)
    if sample.get('JPN', {}).get('debt_gdp') and not 150 < sample['JPN']['debt_gdp'] < 300:
        failed.append('Japan debt/GDP implausible: %s' % sample['JPN'])
    imf_status = fund.get('sources') or {}
    if imf_status and any(v != 'live' for v in imf_status.values()):
        out['packet']['imf_sources_not_live'] = {k: v for k, v in imf_status.items() if v != 'live'}  # cache fallback is by design; warn only
    # previous-release invariants must still hold
    if (out['packet']['n_liquid'].get('sovereign') or 0) < 15 or (out['packet']['n_liquid'].get('us_corp') or 0) < 60:
        failed.append('liquid universe thinner than expected: %s' % out['packet']['n_liquid'])
    first_day = (packet.get('source') or {}).get('first_day')
    if not first_day or first_day > '2024-09-10':
        failed.append('bank history no longer starts at the first public file: first_day=%s' % first_day)
    for k in ('term', 'wides_1y', 'tights_1y', 'activity', 'method', 'fundamentals'):
        if k not in packet:
            failed.append('packet missing block %s' % k)
    if (packet.get('history') or {}).get('error'):
        failed.append('history publish error: %s' % packet['history']['error'])

    # history file: ECB SovCISS long series for the euro area and eleven states, weekly points from 2006, crisis peaks
    head = s3.head_object(Bucket=BUCKET, Key=HISTORY_KEY)
    hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HISTORY_KEY)['Body'].read())
    long = hist.get('long') or {}
    ser = long.get('series') or {}
    sov_ser = {k: v for k, v in ser.items() if k.startswith('sovciss_')}
    out['history'] = {'bytes': head.get('ContentLength'), 'as_of': hist.get('as_of'), 'n_names': len(hist.get('names') or {}),
                      'sovciss': {k: {'n_points': len(v.get('points') or []), 'first': (v.get('points') or [[None]])[0][0], 'last': v.get('last'),
                                      'pct_rank_since_2006': v.get('pct_rank_since_2006'),
                                      'peaks': [(p.get('episode'), p.get('value')) for p in v.get('peaks') or []]} for k, v in sorted(sov_ser.items())},
                      'other_long_series': sorted(k for k in ser if not k.startswith('sovciss_')),
                      'sources': long.get('sources'), 'limits': long.get('limits')}
    for iso in ECB_ISO3:
        v = ser.get('sovciss_' + iso)
        if not v:
            failed.append('ECB SovCISS series missing: %s' % iso)
            continue
        pts = v.get('points') or []
        if len(pts) < 900 or pts[0][0] > '2006-01-15':
            failed.append('ECB SovCISS %s too short or late: %d points from %s' % (iso, len(pts), pts[0][0] if pts else None))
        if (today - date.fromisoformat(v['last']['date'])).days > 14:
            failed.append('ECB SovCISS %s stale: last %s' % (iso, v['last']))
        if not any(p.get('episode', '').startswith('Euro') for p in v.get('peaks') or []):
            failed.append('ECB SovCISS %s has no euro-crisis peak' % iso)
    for iso in ('ITA', 'ESP', 'GRC', 'PRT', 'IRL'):
        v = ser.get('sovciss_' + iso)
        if v:
            euro = next((p['value'] for p in v['peaks'] if p['episode'].startswith('Euro')), 0)
            if euro < 0.6:
                failed.append('ECB SovCISS %s euro-crisis peak implausibly low: %s' % (iso, euro))
    for k in ('baa10y', 'ofr_credit', 'ofr_em', 'gz_spread', 'ebp'):
        if k not in ser:
            failed.append('long series missing: %s' % k)
    not_live = {k: v.get('status') for k, v in (long.get('sources') or {}).items() if v.get('status') != 'live'}
    if not_live:
        out['history']['sources_not_live'] = not_live  # warning only
    if out['history']['n_names'] < 300:
        failed.append('history file carries only %s names' % out['history']['n_names'])
    # regressions carried from ops 6511-6513
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

lines = ['# ops 6514 - justhodl-cds-desk v1.4.0: ECB SovCISS + IMF WEO enrichment', '',
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
print('\n'.join(lines[:10]))
if failed:
    sys.exit(1)
