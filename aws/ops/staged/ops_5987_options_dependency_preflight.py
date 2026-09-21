"""Retain public options/GEX/composite predecessors; never invoke an engine.

No provider requests, account reads, credentials, notifications or schedule writes.
Source qualification is a separate, reviewed step after this complete baseline.
"""
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib, json, subprocess, sys
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/ops/staged'), str(ROOT/'aws/ops/checks')]
from ops_report import report
import ops_5975_etf_constituent_source_preflight as evidence

BUCKET = 'justhodl-dashboard-live'
PREFIX = 'audit-private/20260909-originals/options-research/'
FUNCTIONS = ('justhodl-polygon-options-flow', 'justhodl-dealer-gex', 'justhodl-massive-signals')
PACKETS = ('data/polygon-options-flow.json', 'data/dealer-gex.json', 'data/dealer-gex-history.json',
    'data/massive-signals.json', 'data/massive-capability.json', 'data/polygon-options.json',
    'data/polygon-ratios.json', 'flow-data.json', 'data/polygon-fx-regime.json',
    'data/polygon-futures-curves.json', 'data/etf-desk.json', 'data/etf-desk-research.json',
    'data/tail-risk.json')
CLOCKS = ('observed_at', 'as_of', 'observation_date', 'volume_as_of', 'open_interest_as_of',
    'quote_as_of', 'iv_as_of', 'generated_at')


def retain(client, raw):
    digest = hashlib.sha256(raw).hexdigest()
    key = PREFIX + digest + '.bin'
    try:
        client.put_object(Bucket=BUCKET, Key=key, Body=raw, ContentType='application/octet-stream',
            CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if evidence.code(exc) not in ('PreconditionFailed', 'ConditionalRequestConflict', '409', '412'):
            raise
    assert evidence.bounded(client.get_object(Bucket=BUCKET, Key=key)['Body']) == raw
    return {'key': key, 'sha256': digest, 'bytes': len(raw)}


def row_summary(rows):
    assert isinstance(rows, list)
    valid = [r for r in rows if isinstance(r, dict)]
    return {'rows': len(rows), 'object_rows': len(valid),
        'fields': dict(sorted(Counter(k for r in valid for k in r).items())),
        'non_null_clock_rows': {k: sum(r.get(k) is not None for r in valid) for k in CLOCKS},
        'replay_rows': sum(isinstance(r.get('replay'), dict) for r in valid)}


def describe(key, doc):
    assert key in PACKETS and isinstance(doc, dict), 'Unexpected public predecessor shape'
    out = {'root_fields': sorted(doc),
        **{k: doc.get(k) for k in ('contract', 'version', 'generated_at', 'as_of')},
        'replay_present': isinstance(doc.get('replay'), dict),
        'permissions': {k: doc.get(k) for k in ('forecast_qualified', 'calls_eligible', 'sizing_eligible', 'execution_eligible')}}
    if key == 'data/polygon-options-flow.json':
        rows = doc.get('all_results', [])
        out['rows'] = row_summary(rows)
        out['declared_universe'] = doc.get('universe')
        out['requested'] = doc.get('n_requested')
        out['scanned'] = doc.get('n_scanned')
        out['partial_rows'] = sum(r.get('snapshot_truncated') is True for r in rows if isinstance(r, dict))
        out['failed_rows'] = sum(r.get('error') is not None for r in rows if isinstance(r, dict))
        out['rows_with_zero_call_put_ratio'] = sum(r.get('cv_pv_ratio') == 0 for r in rows if isinstance(r, dict))
        out['legacy_alert_levels'] = dict(Counter(str(r.get('alert_level')) for r in rows if isinstance(r, dict)))
        out['returned_contract_counts'] = {str(r.get('ticker')): r.get('n_contracts') for r in rows if isinstance(r, dict)}
    elif key == 'data/dealer-gex.json':
        rows = doc.get('underlyings', {})
        assert isinstance(rows, dict)
        out['underlyings'] = {ticker: {'root_fields': sorted(row),
            **{k: row.get(k) for k in ('symbol', 'spot', 'n_contracts', 'total_dealer_gex_billions', 'regime')},
            'error_present': row.get('err') is not None,
            'non_null_clocks': {k: row.get(k) is not None for k in CLOCKS}}
            for ticker, row in rows.items() if isinstance(row, dict)}
        out['calculation_config'] = doc.get('calculation_config')
        out['legacy_composite_regime'] = (doc.get('market_composite') or {}).get('composite_regime')
        out['squeeze_candidate_count'] = len(doc.get('squeeze_candidates') or [])
    elif key == 'data/dealer-gex-history.json':
        rows = doc.get('history', [])
        out['history'] = row_summary(rows)
        clocks = [r['ts'] for r in rows if isinstance(r, dict) and type(r.get('ts')) in (int, float)]
        out['history'].update(first_ts=min(clocks) if clocks else None, last_ts=max(clocks) if clocks else None)
    elif key == 'data/massive-signals.json':
        rows = doc.get('tickers', {})
        assert isinstance(rows, dict)
        out['ticker_rows'] = row_summary(list(rows.values()))
        out['ticker_names'] = sorted(rows)
        out['top_ranked_count'] = len(doc.get('top_prepump') or [])
        out['source_fields'] = {k: sorted(v) if isinstance(v, dict) else [] for k, v in (doc.get('sources') or {}).items()}
        out['market_fields'] = sorted(doc.get('market') or {})
    return out


def capture(client, key):
    assert key in PACKETS, 'Public research allowlist only'
    try: obj = client.get_object(Bucket=BUCKET, Key=key)
    except Exception as exc:
        if evidence.code(exc) not in ('NoSuchKey', '404'): raise
        return None, {'status': 'missing'}
    raw = evidence.bounded(obj['Body'])
    ref = retain(client, raw)
    ref.update(acquired_at=datetime.now(timezone.utc).isoformat(),
        last_modified=obj['LastModified'].astimezone(timezone.utc).isoformat(), etag=obj['ETag'],
        version_id=obj.get('VersionId'))
    return ref, {'status': 'retained', 'bytes': len(raw), **describe(key, json.loads(raw))}


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1')
    events = boto3.client('events', region_name='us-east-1')
    scheduler = boto3.client('scheduler', region_name='us-east-1')
    with report('ops_5987_options_dependency_preflight') as r:
        subprocess.run([sys.executable, str(ROOT/'tests/test_options_dependency_preflight.py')], cwd=ROOT, check=True)
        runtimes = {fn: evidence.runtime(lam, s3, events, scheduler, fn) for fn in FUNCTIONS}
        r.kv(runtimes=runtimes)
        refs, inventory = {}, {}
        total = 0
        for key in PACKETS:
            ref, info = capture(s3, key)
            inventory[key] = info
            if ref:
                refs[key] = ref
                total += ref['bytes']
                assert total <= 128 * 1024 * 1024, 'Complete baseline byte budget exceeded'
        manifest = {'contract': 'options-dependency-preflight.v1',
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'runtimes': runtimes, 'packets': refs, 'inventory': inventory,
            'scope': 'Complete existing public research and history; no provider acquisition or producer invocation.'}
        ref = retain(s3, evidence.encoded(manifest))
        protected = [ref['key'], *(v['key'] for v in refs.values())]
        def check(key):
            assert evidence.denied('https://justhodl.ai/'+key)
            assert evidence.denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=6) as pool: list(pool.map(check, protected))
        r.kv(retained_manifest=ref, packet_inventory=inventory, retained_packet_count=len(refs),
            retained_packet_bytes=total, protected_artifacts_checked=len(protected), originals_anonymously_denied=True,
            engine_invocations=0, provider_requests=0, private_account_reads=0, paid_ai_calls=0,
            notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
