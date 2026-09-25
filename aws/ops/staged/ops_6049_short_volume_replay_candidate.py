"""Qualify immutable short-volume research; no provider or native-head operations."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import gc, sys, time, subprocess, urllib.request
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'aws/ops'), str(ROOT / 'aws/ops/staged'), str(ROOT / 'aws/ops/checks')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6046_short_volume_baseline as base
import short_volume_research_model as model
import short_volume_research_store as store
import short_volume_evidence as evidence
BUCKET = base.BUCKET
REQUEST = 'chatgpt-short-volume-replay-candidate-6049'
STATUS = model.PRIVATE + 'requests/' + model.sha(REQUEST.encode()) + '.json'
SOURCE = {'key': model.PRIVATE + '74dd03f0fd09e176baf4b3d64a9ea3f19942067a4d9a64ff50d2269a069d212c.bin',
          'sha256': '74dd03f0fd09e176baf4b3d64a9ea3f19942067a4d9a64ff50d2269a069d212c', 'bytes': 80917}


def journal(s3, value, claim=False):
    raw = model.encoded(value)
    s3.put_object(Bucket=BUCKET, Key=STATUS, Body=raw, ContentType='application/json', CacheControl='no-store',
                  **({'IfNoneMatch': '*'} if claim else {}))
    assert base.read(s3, STATUS) == raw


def fingerprint(compiled):
    return {'packet': model.digest(compiled['packet']),
            'shards': {key: model.digest(shard) for key, shard in compiled['shards'].items()}}


def main():
    import resource
    s3 = boto3.client('s3', region_name='us-east-1')
    read = store.reader(s3, BUCKET)
    with report('ops_6049_short_volume_replay_candidate') as r:
        for name in ('test_short_volume_measurements.py', 'test_short_volume_research_model.py',
                     'test_short_volume_research_store.py', 'test_short_volume_evidence.py'):
            subprocess.run([sys.executable, str(ROOT / 'tests' / name)], cwd=ROOT, check=True)
        try:
            existing = model.strict(base.read(s3, STATUS))
        except Exception as exc:
            if not base.retained.missing(exc):
                raise
            existing = None
        if existing:
            assert existing['status'] == 'complete', 'Never repeat incomplete candidate request'
            ref = existing['replay']
            compiled = store.replay(ref, read)
            profile, qualification = existing['profile'], existing['qualification']
        else:
            manifest = model.strict(model.original(SOURCE, read))
            assert manifest['contract'] == 'short-volume-original-source-inventory.v1'
            for path, expected in manifest['source_modules'].items():
                assert model.sha((ROOT / path).read_bytes()) == expected, 'Accepted source compiler changed'
            inputs = {k: manifest[k] for k in ('selection_cutoff', 'month_plan', 'selected_files', 'captures')}
            inputs.update(contract='short-volume-original-inputs.v1', generated_at=datetime.now(timezone.utc).isoformat(), source_manifest=SOURCE)
            journal(s3, {'request_id': REQUEST, 'status': 'claimed', 'source_manifest': SOURCE}, True)
            started = time.monotonic()
            compiled = model.compile_output(inputs, read)
            compile_seconds = time.monotonic() - started
            qualification = evidence.qualify(inputs, compiled, lambda v: model.original(v, read))
            expected = fingerprint(compiled)
            ref = store.retain(s3, BUCKET, inputs, compiled)
            profile = {'compile_seconds': round(compile_seconds, 3), 'index_bytes': len(model.encoded(compiled['packet'])),
                       'shard_count': len(compiled['shards']), 'shard_bytes': sum(len(model.encoded(v)) for v in compiled['shards'].values()),
                       'max_shard_bytes': max(len(model.encoded(v)) for v in compiled['shards'].values())}
            journal(s3, {'request_id': REQUEST, 'status': 'retained', 'replay': ref})
            del compiled
            gc.collect()
            started = time.monotonic()
            compiled = store.replay(ref, store.reader(s3, BUCKET))
            profile.update(replay_seconds=round(time.monotonic() - started, 3),
                           max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            assert fingerprint(compiled) == expected
            assert profile['max_rss_kib'] < 850 * 1024, 'Candidate exceeds conservative 1,024 MB native bound'
            journal(s3, {'request_id': REQUEST, 'status': 'complete', 'replay': ref, 'profile': profile, 'qualification': qualification})
        run = store.verified_run(ref, read)
        packet = compiled['packet']
        public = {ref['manifest_key'], run['input']['key'], run['output']['key'],
                  *(v['key'] for v in run['compilers'].values()), *(v['key'] for v in packet['record_shards'].values())}
        # Thread-local bounded readers; no shared mutable LRU across threads.
        def check(key):
            request = urllib.request.Request('https://justhodl.ai/' + key, headers={'User-Agent': 'JustHodl-research-acceptance/1.0', 'Cache-Control': 'no-cache'})
            if store.bounded(urllib.request.urlopen(request, timeout=25)) != store.reader(s3, BUCKET)(key):
                raise ValueError('Public immutable artifact differs')
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(check, sorted(public)):
                pass
        inputs = store.checked(run['input'], 'inputs', read)
        protected = {STATUS, SOURCE['key'], *(v['original']['key'] for v in inputs['captures'].values())}
        def deny(key):
            assert denied_with_retry('https://justhodl.ai/' + key) and denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny, sorted(protected)):
                pass
        r.kv(replay=ref, profile=profile, counts=packet['counts'], qualification=qualification,
             public_artifacts_checked=len(public), protected_artifacts_checked=len(protected),
             compiler_sha256={k: v['sha256'] for k, v in run['compilers'].items()},
             provider_requests=0, engine_invocations=0, native_head_writes=0, private_account_reads=0,
             paid_ai_calls=0, notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
