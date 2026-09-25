"""Preserve the complete 72-series Liquidity agent and existing canonical roots.

No new provider request, invocation, account access, public write, notification
or schedule change. The baseline is evidence for a future native migration.
"""
from pathlib import Path
from datetime import datetime, timezone
import base64, gzip, hashlib, io, json, re, subprocess, sys, urllib.request
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'aws/ops', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime, bounded
import canonical_fred_replay as canonical
import retained_access_evidence as access

BUCKET = 'justhodl-dashboard-live'
FUNCTION = 'justhodl-liquidity-agent'
PRIVATE = 'audit-private/20260909-originals/liquidity-agent-research/'
REQUEST = 'chatgpt-liquidity-agent-original-baseline-6121'
sha = lambda raw: hashlib.sha256(raw).hexdigest()
encoded = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
now = lambda: datetime.now(timezone.utc).isoformat()
STATUS = PRIVATE + 'requests/' + sha(REQUEST.encode()) + '.json'
INPUTS = ('liquidity-data.json', 'data/liquidity-flow.json', 'data/report-measurements.json', 'data/global-liquidity.json', 'data/china-liquidity.json')
SERIES = ('WALCL', 'WTREGEN', 'RRPONTSYD', 'WSHOSHO', 'WSHOTSL', 'WSHOMCB', 'WRESBAL', 'TOTRESNS', 'EXCSRESNW', 'M2SL', 'M1SL', 'BOGMBASE', 'DTWEXBGS', 'DTWEXAFEGS', 'DTWEXEMEGS', 'DGS2', 'DGS10', 'DGS30', 'T10Y2Y', 'T10Y3M', 'DFII10', 'SOFR', 'DFF', 'DPCREDIT', 'WORAL', 'TREAST', 'WSHONBIILB', 'WSHOBL', 'WSHOFADSL', 'WSHOMBLS', 'H41RESPPALDKNWA', 'WCBSL', 'TRESEGUSM052N', 'IORB', 'EFFR', 'OBFR', 'M2V', 'WCURCIR', 'DEXUSEU', 'DEXJPUS', 'DEXCHUS', 'DEXKOUS', 'DEXBZUS', 'DFII5', 'DFII30', 'T10YIE', 'T5YIE', 'T5YIFR', 'THREEFYTP10', 'DGS3MO', 'BAMLC0A0CM', 'BAMLC0A1CAAA', 'BAMLC0A2CAA', 'BAMLC0A3CA', 'BAMLC0A4CBBB', 'BAMLC0A0CMFOAS', 'BAMLC0A0CMIOAS', 'BAMLC0A0CMUOAS', 'BAMLH0A0HYM2', 'BAMLH0A1HYBB', 'BAMLH0A2HYB', 'BAMLH0A3HYC', 'BAMLEMCBPIOAS', 'BAMLEMHBHYCRPIOAS', 'BAMLHE00EHYIOAS', 'DRTSCILM', 'DRTSCLCC', 'DRTSCLM', 'NFCI', 'ANFCI', 'STLFSI4', 'OFRFSI')


def retain(s3, raw):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= 64 * 1024 * 1024:
        raise ValueError('Complete bounded predecessor required')
    ref = {'key': PRIVATE + sha(raw) + '.bin', 'sha256': sha(raw), 'bytes': len(raw)}
    try:
        s3.put_object(Bucket=BUCKET, Key=ref['key'], Body=raw, ContentType='application/octet-stream', CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise
    assert bounded(s3.get_object(Bucket=BUCKET, Key=ref['key'])['Body']) == raw
    return ref


def journal(s3, value, claim=False):
    raw = encoded(value)
    s3.put_object(Bucket=BUCKET, Key=STATUS, Body=raw, ContentType='application/json', CacheControl='no-store', **({'IfNoneMatch': '*'} if claim else {}))
    assert bounded(s3.get_object(Bucket=BUCKET, Key=STATUS)['Body']) == raw


def main():
    subprocess.run([sys.executable, str(ROOT/'tests/test_liquidity_agent_original_baseline.py')], cwd=ROOT, check=True)
    s3, lam, events, scheduler = (boto3.client(name, region_name='us-east-1') for name in ('s3', 'lambda', 'events', 'scheduler'))
    with report('ops_6121_liquidity_agent_original_baseline') as r:
        progress = {'contract': 'liquidity-agent-source-baseline.v1', 'request_id': REQUEST,
                    'started_at': now(), 'status': 'claimed', 'captures': {}, 'repo_predecessors': {}, 'canonical_originals': {}}
        journal(s3, progress, True)
        try:
            before = runtime(lam, s3, events, scheduler, FUNCTION)
            location = lam.get_function(FunctionName=FUNCTION)['Code']['Location']
            raw = bounded(urllib.request.urlopen(location, timeout=40))
            assert base64.b64encode(hashlib.sha256(raw).digest()).decode() == before['code_sha256']
            progress['native_predecessor'] = {'runtime': before, 'whole_zip': retain(s3, raw)}
            journal(s3, progress)
            packets = {}
            for key in INPUTS:
                response = s3.get_object(Bucket=BUCKET, Key=key);raw = bounded(response['Body'])
                packets[key] = json.loads(raw)
                progress['captures'][key] = {'source_key': key, 'original': retain(s3, raw), 'etag': response['ETag'], 'captured_at': now()}
                journal(s3, progress)
            cache = {}
            def read(key):
                if not isinstance(key, str) or not re.fullmatch(r'data/(report-research|evidence/fred)/[A-Za-z0-9_./-]+', key) or '..' in key:
                    raise ValueError('Canonical macro replay path required')
                if key not in cache:
                    raw = bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body'], canonical.MAX)
                    container = retain(s3, raw)
                    if key.endswith('.gz'):
                        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
                            raw = stream.read(canonical.MAX + 1)
                        if len(raw) > canonical.MAX: raise ValueError('Whole original exceeds replay bound')
                    progress['canonical_originals'][key] = {'source_key': key, 'stored_container': container, 'original': retain(s3, raw)}
                    cache[key] = raw
                return cache[key]
            packet = packets['data/report-measurements.json']
            originals = canonical.restore(packet, SERIES, read)
            summary = {}
            for sid, original in originals.items():
                if original is None:
                    summary[sid] = {'status': 'missing_canonical_series'}
                    continue
                meta = original['definition']['seriess'][0]; rows = original['observations']['observations']
                dates = sorted(row['date'] for row in rows)
                summary[sid] = {'status': 'reconstructed_from_whole_originals', 'native_unit': meta['units'],
                    'frequency': meta['frequency'], 'frequency_short': meta['frequency_short'],
                    'seasonal_adjustment': meta['seasonal_adjustment'], 'source_last_updated': meta.get('last_updated'),
                    'original_row_count': len(rows), 'missing_value_rows': sum(row.get('value') in (None, '.', '') for row in rows),
                    'first_observation_date': dates[0] if dates else None, 'last_observation_date': dates[-1] if dates else None,
                    'acquired_at': original['acquired_at'], 'source_quality': packet['measurements'][sid].get('quality'),
                    'historical_release_availability_verified': False}
            paths = subprocess.check_output(['git', 'grep', '-l', '-F', 'liquidity-data.json', '--',
                'aws/lambdas/*/source/*.py', 'aws/shared/*.py', '*.html', '*.js', 'assets/*.js'], cwd=ROOT, text=True).splitlines()
            paths = set(paths) | {'aws/lambdas/' + FUNCTION + '/source/lambda_function.py', 'aws/shared/canonical_fred_replay.py',
                'aws/shared/report_observations.py', 'aws/shared/research_brief_model.py', 'aws/shared/evidence_store.py',
                'aws/shared/pd_fails_context.py', 'aws/lambdas/justhodl-liquidity-agent/source/liquidity_contract.py', 'liquidity.html'}
            config = ROOT / 'aws/lambdas' / FUNCTION / 'config.json'
            progress['tracked_config_present'] = config.exists()
            if config.exists(): paths.add(config.relative_to(ROOT).as_posix())
            for path in sorted(paths): progress['repo_predecessors'][path] = retain(s3, (ROOT / path).read_bytes())
            progress.update(status='retained', source_replay=packet['replay'], source_generated_at=packet['generated_at'], source_summary=summary)
            journal(s3, progress); manifest = retain(s3, encoded(progress))
            protected = {STATUS, manifest['key'], progress['native_predecessor']['whole_zip']['key']}
            protected.update(ref['key'] for ref in progress['repo_predecessors'].values())
            protected.update(row['original']['key'] for row in progress['captures'].values())
            for row in progress['canonical_originals'].values(): protected.update((row['stored_container']['key'], row['original']['key']))
            outcomes = [access.check(key) for key in sorted(protected)]
            access_ref = retain(s3, encoded({'contract': 'liquidity-agent-baseline-access.v1', 'outcomes': outcomes}))
            outcomes.append(access.check(access_ref['key']))
            privacy = access.summarize(outcomes); r.kv(access_evidence=access_ref, **privacy)
            assert privacy['all_denied'], 'Inspect anonymous-access outcomes before changing the producer'
            assert runtime(lam, s3, events, scheduler, FUNCTION) == before
            journal(s3, {**progress, 'status': 'complete', 'completed_at': now(), 'manifest': manifest, 'privacy': privacy})
            r.kv(manifest=manifest, native_predecessor=progress['native_predecessor'], captures=progress['captures'],
                source_replay=packet['replay'], source_generated_at=packet['generated_at'], source_summary=summary,
                repo_predecessors=progress['repo_predecessors'], canonical_originals=progress['canonical_originals'],
                all_predecessor_bytes_preserved=True, canonical_originals_replayed=True, requested_series=len(SERIES), reconstructed_series=sum(v['status']=='reconstructed_from_whole_originals' for v in summary.values()), missing_series=[k for k,v in summary.items() if v['status']=='missing_canonical_series'], native_package_unchanged=True,
                native_formula_changed=False, tracked_config_present=progress['tracked_config_present'], provider_requests=0, producer_invocations=0, consumer_invocations=0,
                public_writes=0, private_account_reads=0, paid_ai_calls=0, notifications_sent=0, signal_writes=0, schedules_changed=0,
                forecast_qualified=False, sizing_qualified=False)
        except Exception as exc:
            journal(s3, {**progress, 'status': 'failed', 'error_type': type(exc).__name__})
            raise


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
