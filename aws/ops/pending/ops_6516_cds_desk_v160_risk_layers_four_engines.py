"""ops 6516 - CDS desk v1.6.0 risk layers + four new source engines (FDIC BankFind, NY Fed CMDI, ESMA ratings, EBA risk dashboard).

Khalid asked for the sovereign / systemic risk sources the system was missing, plugged into the CDS desk first and each one
written into the data engine under its own engine + page.  This release:

  * justhodl-fdic-bankfind     -> data/fdic-bankfind.json      + data/warm/fdic-bankfind/      (page /fdic-bankfind.html)
  * justhodl-nyfed-cmdi        -> data/nyfed-cmdi.json         + data/warm/nyfed-cmdi/         (page /nyfed-cmdi.html)
  * justhodl-esma-ratings      -> data/esma-ratings.json       + data/warm/esma-ratings/       (page /esma-ratings.html)
  * justhodl-eba-risk-dashboard-> data/eba-risk-dashboard.json + data/warm/eba-risk-dashboard/ (page /eba-risk-dashboard.html)
  * justhodl-cds-desk v1.6.0   -> reads the four packets + IMF DataMapper (ARA, GDD) + ECB SUP (bank -> sovereign exposures,
                                  written to data/warm/ecb-sup/) + OFR Form PF (hf/v1 warm store) and attaches them to the
                                  sovereign universe (rating / ara / pdebt / banks / eba) and four packet blocks
                                  (bank_sovereign, layers.esma, hedge_funds, us_credit).
  * justhodl-provider-catalog  -> registers dtcc, fdic-bankfind, nyfed-cmdi, esma-ratings, eba-risk-dashboard providers so
                                  everything shows on data.html / provider.html.

Order: the four source engines first (each must publish), then the CDS desk daily pass, then the provider catalog.
First run (2026-10-09 17:16Z, dispatch after the recovery deploy) ran every engine successfully but FAILED on two of its own
checks: FDIC aggregates are keyed by quarter and the ESMA consensus block uses `consensus_label` -- corrected here, re-run.
Waits until deploy-lambdas (parallel workflow, same push) has published a release receipt covering THIS commit for every
function.  Descriptive engines only (decision.call None, sizing_eligible False).  Retries disabled on the Lambda client.
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
REPORT = ROOT / 'aws/ops/reports/latest/ops_6516_cds_desk_v160_risk_layers_four_engines.md'
SOURCE_ENGINES = ('justhodl-fdic-bankfind', 'justhodl-nyfed-cmdi', 'justhodl-esma-ratings', 'justhodl-eba-risk-dashboard')
CDS_FN = 'justhodl-cds-desk'
CATALOG_FN = 'justhodl-provider-catalog'
ALL_FNS = SOURCE_ENGINES + (CDS_FN, CATALOG_FN)
SLUG = {'justhodl-fdic-bankfind': 'fdic-bankfind', 'justhodl-nyfed-cmdi': 'nyfed-cmdi',
        'justhodl-esma-ratings': 'esma-ratings', 'justhodl-eba-risk-dashboard': 'eba-risk-dashboard'}
MIN_SERIES = {'fdic-bankfind': 12, 'nyfed-cmdi': 6, 'esma-ratings': 100, 'eba-risk-dashboard': 150}
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

    # 1. the four source engines
    out['sources'] = {}
    hots = {}
    for fn in SOURCE_ENGINES:
        t0 = time.time()
        res = invoke(fn, {})
        res['elapsed_s'] = round(time.time() - t0, 1)
        out['sources'][fn] = {'invoke': res}
        try:
            rec, hot = check_source_engine(fn, today)
            out['sources'][fn]['check'] = rec
            hots[SLUG[fn]] = hot
        except Exception as exc:  # noqa: BLE001
            failed.append('%s: %s: %s' % (fn, type(exc).__name__, str(exc)[:300]))

    # source-specific sanity on real values
    f = hots.get('fdic-bankfind') or {}
    # aggregates are keyed by Call Report quarter (28 quarters); check the latest one (first run read the dict itself -> None)
    agg_all = (f.get('extra') or {}).get('aggregates') or f.get('aggregates') or {}
    agg = (agg_all.get(f.get('latest_quarter') or (f.get('extra') or {}).get('latest_quarter') or '') or {}) if isinstance(agg_all, dict) else {}
    out['fdic_quarters'] = sorted(agg_all.keys()) if isinstance(agg_all, dict) else None
    if f and isinstance(agg_all, dict) and len(agg_all) < 20:
        failed.append('fdic-bankfind: only %d quarters of aggregates (N_QUARTERS=28 expected)' % len(agg_all))
    if f and not agg:
        failed.append('fdic-bankfind: no aggregates block')
    if agg:
        if not (2000 < (agg.get('n_banks') or 0) < 6000):
            failed.append('fdic-bankfind: implausible bank count %s' % agg.get('n_banks'))
        if not (20 < (agg.get('uninsured_share') or 0) < 70):
            failed.append('fdic-bankfind: implausible uninsured share %s' % agg.get('uninsured_share'))
    c = hots.get('nyfed-cmdi') or {}
    ck = {k.get('label'): (k.get('value'), k.get('sub')) for k in (c.get('kpis') or []) if isinstance(k, dict)} if c else {}
    out['cmdi_kpis'] = ck
    e = hots.get('esma-ratings') or {}
    cons = (e.get('extra') or {}).get('consensus') or e.get('consensus') or {}
    out['esma_consensus_sample'] = {k: cons.get(k) for k in ('USA', 'FRA', 'ITA', 'DEU', 'JPN', 'GBR', 'ARG', 'TUR') if k in cons}
    if e and len(cons) < 80:
        failed.append('esma-ratings: consensus covers only %d sovereigns' % len(cons))
    for iso in ('USA', 'FRA', 'ITA', 'DEU'):
        if e and not (cons.get(iso) or {}).get('consensus_label'):  # ESMA consensus block uses consensus_label / consensus_notch
            failed.append('esma-ratings: no consensus rating for %s' % iso)
    b = hots.get('eba-risk-dashboard') or {}
    if b and (b.get('n_series') or 0) < 150:
        failed.append('eba-risk-dashboard: %s series' % b.get('n_series'))

    # 2. the CDS desk daily pass, now reading the four packets + IMF + ECB SUP + OFR Form PF
    t0 = time.time()
    out['cds_daily'] = invoke(CDS_FN, {'action': 'daily'})
    out['cds_daily']['elapsed_s'] = round(time.time() - t0, 1)
    packet = get_json('data/cds-desk.json')
    groups = packet.get('groups') or {}
    sov = groups.get('sovereign') or {}
    universe = sov.get('universe') or []
    layers = packet.get('layers') or {}
    status = layers.get('status') or {}
    counts = {k: sum(1 for u in universe if u.get(k)) for k in ('fund', 'stress', 'rating', 'ara', 'pdebt', 'banks', 'eba')}
    bs = packet.get('bank_sovereign') or {}
    hf = packet.get('hedge_funds') or {}
    uc = packet.get('us_credit') or {}
    es = layers.get('esma') or {}
    out['cds_packet'] = {'version': packet.get('version'), 'as_of': packet.get('as_of'), 'generated_at': packet.get('generated_at'),
                         'decision': packet.get('decision'), 'layers_status': status, 'layers_version': layers.get('version'),
                         'universe': len(universe), 'universe_layer_counts': counts,
                         'bank_sovereign': {'period': bs.get('period'), 'n_rows': len(bs.get('rows') or []), 'total_eur_bn': bs.get('ea_banks_total_sov_eur_bn'),
                                            'top': (bs.get('rows') or [])[:5]},
                         'esma': {'as_of': es.get('as_of'), 'n_sovereigns': es.get('n_sovereigns'), 'n_recent_actions': len(es.get('recent_actions') or []),
                                  'latest': (es.get('recent_actions') or [None])[0]},
                         'hedge_funds': {'as_of': hf.get('as_of'), 'n_series': hf.get('n_series'),
                                         'last': {k: (v.get('last') or {}).get('value') for k, v in (hf.get('series') or {}).items()}},
                         'us_credit': {'cmdi': {k: {kk: vv for kk, vv in v.items() if kk != 'tail'} for k, v in (uc.get('cmdi') or {}).items() if isinstance(v, dict)},
                                       'fdic': {'quarter': (uc.get('fdic') or {}).get('quarter'), 'aggregates': (uc.get('fdic') or {}).get('aggregates'),
                                                'n_screen': len((uc.get('fdic') or {}).get('screen') or [])}},
                         'sample_rows': {u['iso3']: {'rating': (u.get('rating') or {}).get('consensus'), 'ara': (u.get('ara') or {}).get('ara'),
                                                     'pdebt': (u.get('pdebt') or {}).get('private_debt_gdp'), 'banks_eur_bn': (u.get('banks') or {}).get('ea_banks_eur_bn'),
                                                     'eba_home_bias': (u.get('eba') or {}).get('home_bias_pct')}
                                         for u in universe if u.get('iso3') in ('USA', 'FRA', 'ITA', 'DEU', 'ESP', 'JPN', 'ARG', 'TUR', 'BRA', 'IND', 'ZAF')}}
    if packet.get('decision', {}).get('call') is not None or packet.get('decision', {}).get('sizing_eligible'):
        failed.append('cds-desk: doctrine breach, decision block is not descriptive')
    if tuple(int(x) for x in str(packet.get('version', '0')).split('.')[:3]) < (1, 6, 0):
        failed.append('cds-desk: packet built by old code: version %s' % packet.get('version'))
    if (today - date.fromisoformat(packet.get('as_of') or '2024-09-03')).days > 6:
        failed.append('cds-desk: packet as_of is stale: %s' % packet.get('as_of'))
    for layer in ('imf_ara_gdd', 'ecb_sup', 'esma', 'eba', 'ofr_form_pf', 'us_credit'):
        st = str(status.get(layer) or '')
        if not st.startswith('ok'):
            failed.append('cds-desk: layer %s not ok: %s' % (layer, st or 'missing'))
    if counts['rating'] < 80:
        failed.append('cds-desk: ESMA ratings attached to only %d sovereigns' % counts['rating'])
    if counts['pdebt'] < 40:
        failed.append('cds-desk: IMF private debt attached to only %d sovereigns' % counts['pdebt'])
    if counts['ara'] < 25:
        failed.append('cds-desk: IMF reserve adequacy attached to only %d sovereigns' % counts['ara'])
    if counts['banks'] < 15 or len(bs.get('rows') or []) < 15:
        failed.append('cds-desk: ECB SUP bank-sovereign rows %d / universe %d' % (len(bs.get('rows') or []), counts['banks']))
    if counts['eba'] < 20:
        failed.append('cds-desk: EBA home-bias attached to only %d sovereigns' % counts['eba'])
    for iso in ('FRA', 'ITA', 'ESP', 'DEU'):
        u = next((x for x in universe if x.get('iso3') == iso), {})
        if not (u.get('banks') or {}).get('ea_banks_eur_bn'):
            failed.append('cds-desk: ECB SUP exposure missing for %s' % iso)
        if not (u.get('rating') or {}).get('consensus'):
            failed.append('cds-desk: ESMA rating missing for %s' % iso)
    if (hf.get('n_series') or 0) < 8 or not (hf.get('series') or {}).get('sov_gne'):
        failed.append('cds-desk: OFR Form PF block thin: %s' % hf.get('n_series'))
    if not ((uc.get('cmdi') or {}).get('market') or {}).get('value') and ((uc.get('cmdi') or {}).get('market') or {}).get('value') != 0:
        failed.append('cds-desk: CMDI market value missing')
    if not ((uc.get('fdic') or {}).get('aggregates') or {}).get('n_banks'):
        failed.append('cds-desk: FDIC aggregates missing')
    for banned in ('buy', 'sell', 'target', 'forecast', 'predict'):
        if banned in json.dumps({'bs': bs.get('read'), 'hf': hf.get('read'), 'es': es.get('read'), 'uc': [(uc.get('cmdi') or {}).get('read'), (uc.get('fdic') or {}).get('read')]}).lower():
            failed.append('cds-desk: doctrine, layer text contains "%s"' % banned)
    # ECB SUP written into the data engine as its own warm store
    sup_state = get_json('data/warm/ecb-sup/state.json')
    out['ecb_sup_state'] = {k: sup_state.get(k) for k in ('period', 'n_series', 'updated_at', 'source') if k in sup_state}
    out['ecb_sup_state']['catalog_len'] = len(sup_state.get('catalog') or [])
    if len(sup_state.get('catalog') or []) < 15:
        failed.append('ecb-sup warm store: catalog has %d series' % len(sup_state.get('catalog') or []))
    s3.head_object(Bucket=BUCKET, Key='data/warm/ecb-sup/src/sup_s13_e0010.csv')
    # previous-release invariants
    n_liquid = {g: v.get('n_liquid') for g, v in groups.items()}
    out['cds_packet']['n_liquid'] = n_liquid
    if (n_liquid.get('sovereign') or 0) < 15 or (n_liquid.get('us_corp') or 0) < 60:
        failed.append('cds-desk: liquid universe thinner than expected: %s' % n_liquid)
    if counts['stress'] < 25 or counts['fund'] < 100:
        failed.append('cds-desk: v1.4/v1.5 layers regressed: fund %d stress %d' % (counts['fund'], counts['stress']))
    for k in ('term', 'wides_1y', 'tights_1y', 'activity', 'method', 'fundamentals', 'ecb_stress'):
        if k not in packet:
            failed.append('cds-desk: packet missing block %s' % k)
    if (packet.get('history') or {}).get('error'):
        failed.append('cds-desk: history publish error: %s' % packet['history']['error'])

    # 3. provider catalog: the new providers must appear on data.html
    t0 = time.time()
    out['catalog_invoke'] = invoke(CATALOG_FN, {})
    out['catalog_invoke'] = {'elapsed_s': round(time.time() - t0, 1), 'summary': str(out['catalog_invoke'])[:400]}
    cat = get_json('data/provider-catalog.json')
    provs = {p.get('slug'): p for p in (cat.get('providers') or [])} if isinstance(cat.get('providers'), list) else (cat.get('providers') or {})
    out['catalog'] = {}
    for slug in ('dtcc', 'fdic-bankfind', 'nyfed-cmdi', 'esma-ratings', 'eba-risk-dashboard', 'ofr-hfm', 'ecb', 'imf'):
        p = provs.get(slug) if isinstance(provs, dict) else None
        if p is None:
            failed.append('provider-catalog: %s missing from data/provider-catalog.json' % slug)
            continue
        n_ser = p.get('series_count')
        out['catalog'][slug] = {'n_keys': p.get('n_keys'), 'total_mb': p.get('total_mb'), 'series_count': n_ser,
                                'hot_feeds': p.get('hot_feeds'), 'freshest_h': p.get('freshest_h')}
        if slug in SLUG.values() and (p.get('n_keys') or 0) < 3:
            failed.append('provider-catalog: %s shows only %s keys' % (slug, p.get('n_keys')))
        if slug in SLUG.values() and not ((n_ser or 0) >= MIN_SERIES[slug]):
            failed.append('provider-catalog: %s series count %s' % (slug, n_ser))
        try:
            s3.head_object(Bucket=BUCKET, Key='data/providers/%s.json' % slug)
        except Exception as exc:  # noqa: BLE001
            failed.append('provider-catalog: data/providers/%s.json missing (%s)' % (slug, type(exc).__name__))
except Exception as exc:  # noqa: BLE001
    failed.append('%s: %s' % (type(exc).__name__, str(exc)[:800]))

lines = ['# ops 6516 - CDS desk v1.6.0 risk layers + FDIC BankFind, NY Fed CMDI, ESMA ratings, EBA risk dashboard engines', '',
         '- commit: %s' % THIS_COMMIT,
         '- functions: %s' % json.dumps(out.get('functions'), default=str),
         '- sources: %s' % json.dumps(out.get('sources'), default=str),
         '- cds_daily: %s' % json.dumps(out.get('cds_daily'), default=str),
         '- cds_packet: %s' % json.dumps(out.get('cds_packet'), default=str),
         '- ecb_sup_state: %s' % json.dumps(out.get('ecb_sup_state'), default=str),
         '- esma_consensus_sample: %s' % json.dumps(out.get('esma_consensus_sample'), default=str),
         '- catalog: %s' % json.dumps(out.get('catalog'), default=str), '']
if failed:
    lines += ['## FAILED', ''] + ['- ' + f for f in failed] + ['']
lines += ['## Full data', '', '```json', json.dumps(out, default=str, separators=(',', ':')), '```', '', 'VERDICT: %s' % ('FAIL' if failed else 'PASS')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:10]))
if failed:
    sys.exit(1)
