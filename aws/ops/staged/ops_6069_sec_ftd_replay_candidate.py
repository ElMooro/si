"""Qualify whole SEC CNS research from accepted 6068 originals, without a native ship."""
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import gc, subprocess, sys, time, urllib.request
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6060_sec_control_trailer_baseline as base
import sec_ftd_research_model as model
import sec_ftd_research_store as store
import sec_ftd_evidence as evidence
BUCKET = base.BUCKET
REQUEST = 'chatgpt-sec-ftd-replay-candidate-6069'
STATUS = model.PRIVATE + 'requests/' + model.sha(REQUEST.encode()) + '.json'
SOURCE = {'key': model.PRIVATE + '601af1fdca325da49ff50991aafd52b8b75fede20a2301611b7af0e47478bf11.bin',
          'sha256': '601af1fdca325da49ff50991aafd52b8b75fede20a2301611b7af0e47478bf11', 'bytes': 44267}


def journal(s3, value, claim=False):
    body = model.encoded(value)
    s3.put_object(Bucket=BUCKET, Key=STATUS, Body=body, ContentType='application/json', CacheControl='no-store',
                  **({'IfNoneMatch': '*'} if claim else {}))
    assert base.read(s3, STATUS) == body


def fingerprint(compiled):
    return {'packet': model.digest(compiled['packet']), 'shards': {k: model.digest(v) for k, v in compiled['shards'].items()}}


def candidate_inputs(source):
    assert source['contract'] == 'sec-settlement-complete-sources.v1'
    assert source['request_id'] == 'chatgpt-sec-retained-history-qualification-6068'
    for path in ('aws/ops/checks/sec_ftd_inventory.py', 'aws/shared/sec_ftd_source.py'):
        assert model.sha((ROOT / path).read_bytes()) == source['inventory_parser']['sha256']
    assert source['cross_archive_inventory']['identical_cross_archive_repetitions'] == 0
    assert source['cross_archive_inventory']['conflicting_cross_archive_records'] == 0
    return {'contract': 'sec-ftd-original-inputs.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
            'archive_count': 12, 'source_manifest': SOURCE,
            **{key: source[key] for key in ('selection_cutoff', 'selected_archives', 'index', 'captures')}}


def main():
    import resource
    s3 = boto3.client('s3', region_name='us-east-1')
    read = store.reader(s3, BUCKET)
    with report('ops_6069_sec_ftd_replay_candidate') as r:
        for name in ('measurements', 'research', 'evidence', 'context'):
            subprocess.run([sys.executable, str(ROOT / ('tests/test_sec_ftd_' + name + '.py'))], cwd=ROOT, check=True)
        try:
            prior = model.strict(base.read(s3, STATUS))
        except Exception as exc:
            if not base.raw.missing(exc):
                raise
            prior = None
        source = model.strict(model.original(SOURCE, read))
        if prior:
            assert prior['status'] == 'complete', 'Never repeat failed/ambiguous qualification'
            ref, profile, proof = prior['replay'], prior['profile'], prior['qualification']
            compiled = store.replay(ref, read)
        else:
            inputs = candidate_inputs(source)
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
                assert resource.getrusage(resource.RUSAGE_SELF).ru_maxrss < 850 * 1024, 'Candidate exceeds conservative 1024 MB native budget'
                expected = fingerprint(compiled)
                ref = store.retain(s3, BUCKET, inputs, compiled)
                journal(s3, {'request_id': REQUEST, 'status': 'retained', 'replay': ref})
                del compiled
                gc.collect()
                started = time.monotonic()
                compiled = store.replay(ref, store.reader(s3, BUCKET))
                profile.update(replay_seconds=round(time.monotonic() - started, 3), max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                assert fingerprint(compiled) == expected
                assert profile['max_rss_kib'] < 850 * 1024, 'Replay exceeds conservative 1024 MB native budget'
                journal(s3, {'request_id': REQUEST, 'status': 'complete', 'replay': ref, 'profile': profile, 'qualification': proof})
            except Exception as exc:
                journal(s3, {'request_id': REQUEST, 'status': 'failed', 'error_type': type(exc).__name__})
                raise
        run = store.verified_run(ref, read)
        public = {ref['manifest_key'], run['input']['key'], run['output']['key'],
            *(v['key'] for v in run['compilers'].values()), *(v['key'] for v in compiled['packet']['record_shards'].values())}
        def public_check(key):
            request = urllib.request.Request('https://justhodl.ai/' + key,
                headers={'User-Agent': 'JustHodl-research-acceptance/1.0', 'Cache-Control': 'no-cache'})
            assert store.bounded(urllib.request.urlopen(request, timeout=25)) == store.reader(s3, BUCKET)(key), 'Public research bytes differ'
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(public_check, sorted(public)):
                pass
        protected = {STATUS, SOURCE['key'], source['index']['original']['key'], source['inventory_parser']['key'],
            source['baseline']['key'], source['baseline_inventory_parser']['key'], source['source_diagnostic_report']['key'],
            *(v['original']['key'] for v in source['captures'].values()), *(v['key'] for v in source['adopted_failed_journals'].values())}
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
