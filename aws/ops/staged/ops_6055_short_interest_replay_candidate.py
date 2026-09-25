"""Qualify immutable settlement research from 6054 originals; no fresh data call.

This writes content-addressed research evidence only. The native head, handler,
schedules, consumers, accounts, notifications and portfolios are untouched.
"""
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import gc, subprocess, sys, time, urllib.request
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6053_short_interest_source_baseline as base
import short_interest_research_model as model
import short_interest_research_store as store
import short_interest_evidence as evidence
BUCKET = base.BUCKET
REQUEST = 'chatgpt-short-interest-replay-candidate-6055'
STATUS = model.PRIVATE + 'requests/' + model.sha(REQUEST.encode()) + '.json'
SOURCE = {'key': model.PRIVATE + '231f424b3c4202dcf3644ee5daa64ca3536ae41a72123290c059014ace00a440.bin',
          'sha256': '231f424b3c4202dcf3644ee5daa64ca3536ae41a72123290c059014ace00a440', 'bytes': 68816}


def journal(s3, value, claim=False):
    body = model.encoded(value)
    s3.put_object(Bucket=BUCKET, Key=STATUS, Body=body, ContentType='application/json', CacheControl='no-store', **({'IfNoneMatch': '*'} if claim else {}))
    assert base.read(s3, STATUS) == body


def fingerprint(compiled):
    return {'packet': model.digest(compiled['packet']), 'shards': {k: model.digest(v) for k, v in compiled['shards'].items()}}


def main():
    import resource
    s3 = boto3.client('s3', region_name='us-east-1')
    read = store.reader(s3, BUCKET)
    with report('ops_6055_short_interest_replay_candidate') as r:
        subprocess.run([sys.executable, str(ROOT / 'tests/test_short_interest_research.py')], cwd=ROOT, check=True)
        try:
            prior = model.strict(base.read(s3, STATUS))
        except Exception as exc:
            if not base.raw.missing(exc):
                raise
            prior = None
        if prior:
            assert prior['status'] == 'complete', 'Never repeat failed/ambiguous qualification'
            ref, profile, proof = prior['replay'], prior['profile'], prior['qualification']
            compiled = store.replay(ref, read)
        else:
            source = model.strict(model.original(SOURCE, read))
            assert source['contract'] == 'short-interest-complete-sources.v1'
            for name, path in (('short_interest_inventory', 'aws/ops/checks/short_interest_inventory.py'), ('offexchange_measurements', 'aws/shared/offexchange_measurements.py')):
                assert model.sha((ROOT / path).read_bytes()) == source['source_modules'][name]
            inputs = {'contract': 'short-interest-original-inputs.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
                      'selection_cutoff': model.clock(source['captures']['partitions']['received_at']).date().isoformat(),
                      'source_manifest': SOURCE, 'settlement_plan': source['settlement_plan'], 'captures': source['captures']}
            journal(s3, {'request_id': REQUEST, 'status': 'claimed', 'source_manifest': SOURCE}, True)
            try:
                started = time.monotonic()
                compiled = model.compile_output(inputs, read)
                profile = {'compile_seconds': round(time.monotonic() - started, 3),
                           'index_bytes': len(model.encoded(compiled['packet'])), 'shard_count': len(compiled['shards']),
                           'shard_bytes': sum(len(model.encoded(v)) for v in compiled['shards'].values()),
                           'max_shard_bytes': max(len(model.encoded(v)) for v in compiled['shards'].values())}
                started = time.monotonic()
                proof = evidence.qualify(inputs, compiled, lambda ref: model.original(ref, read))
                profile['independent_check_seconds'] = round(time.monotonic() - started, 3)
                expected = fingerprint(compiled)
                ref = store.retain(s3, BUCKET, inputs, compiled)
                journal(s3, {'request_id': REQUEST, 'status': 'retained', 'replay': ref})
                del compiled
                gc.collect()
                started = time.monotonic()
                compiled = store.replay(ref, store.reader(s3, BUCKET))
                profile.update(replay_seconds=round(time.monotonic() - started, 3), max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                assert fingerprint(compiled) == expected
                assert profile['max_rss_kib'] < 430 * 1024, 'Candidate exceeds conservative native 512 MB bound'
                journal(s3, {'request_id': REQUEST, 'status': 'complete', 'replay': ref, 'profile': profile, 'qualification': proof})
            except Exception as exc:
                journal(s3, {'request_id': REQUEST, 'status': 'failed', 'error_type': type(exc).__name__})
                raise
        run = store.verified_run(ref, read)
        public = {ref['manifest_key'], run['input']['key'], run['output']['key'],
                  *(v['key'] for v in run['compilers'].values()), *(v['key'] for v in compiled['packet']['record_shards'].values())}
        def public_check(key):
            request = urllib.request.Request('https://justhodl.ai/' + key, headers={'User-Agent': 'JustHodl-research-acceptance/1.0', 'Cache-Control': 'no-cache'})
            assert store.bounded(urllib.request.urlopen(request, timeout=25)) == store.reader(s3, BUCKET)(key), 'Public research bytes differ'
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(public_check, sorted(public)):
                pass
        inputs = store.checked(run['input'], 'inputs', read)
        protected = {STATUS, SOURCE['key'], *(v['original']['key'] for v in inputs['captures'].values())}
        def private_check(key):
            assert denied_with_retry('https://justhodl.ai/' + key) and denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(private_check, sorted(protected)):
                pass
        r.kv(replay=ref, profile=profile, qualification=proof, counts=compiled['packet']['counts'],
             public_artifacts_checked=len(public), protected_artifacts_checked=len(protected),
             frozen_compilers={name: value['sha256'] for name, value in run['compilers'].items()},
             provider_requests=0, engine_invocations=0, native_head_writes=0, private_account_reads=0,
             paid_ai_calls=0, notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
