"""Exact package, whole retained-cache compilation/replay; never invoke a producer."""
from pathlib import Path
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import patch
import importlib.util
import resource
import subprocess
import sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/checks', 'aws/lambdas/justhodl-cycle-features/source', 'aws/lambdas/justhodl-cycle-features/tests')]
from ops_report import report
from market_runtime_evidence import runtime
import cycle_publication as pub
import cycle_sources as sources
FN = 'justhodl-cycle-features'
BUCKET = 'justhodl-dashboard-live'
BASELINE_SHA = '14a62c2163f78b9fd32fa2b8bbb9acc3ff92da141f88009b6b9a1621df0a5ef9'


def main():
    lam, s3, events, scheduler = (boto3.client(n, region_name='us-east-1') for n in ('lambda', 's3', 'events', 'scheduler'))
    with report('ops_6184_cycle_source_acceptance') as r:
        expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/' + FN + '/source/cycle_sources.py'], cwd=ROOT, text=True).strip()
        before = runtime(lam, s3, events, scheduler, FN)
        if before['receipt'] != {'status': 'matched', 'commit': expected} or (before['memory_mb'], before['timeout']) != (3008, 600):
            raise ValueError('Exact cycle package and original runtime required')
        if before['schedules'] != [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-cycle-features-daily', 'state': 'ENABLED',
                                    'expression': 'cron(30 10 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}]:
            raise ValueError('Original cycle schedule differs')
        def original(ref):
            if ref['key'] != pub.PRIVATE + ref['sha256'] + '.bin':
                raise ValueError('Protected original identity required')
            obj = s3.get_object(Bucket=BUCKET, Key=ref['key']);raw = pub.bounded(obj['Body'])
            if len(raw) != ref['bytes'] or obj['ContentLength'] != len(raw) or pub.sha(raw) != ref['sha256']:
                raise ValueError('Complete retained original differs')
            return raw
        baseline = pub.strict(original({'key': pub.PRIVATE + BASELINE_SHA + '.bin', 'sha256': BASELINE_SHA, 'bytes': 20921}))
        if baseline['status'] != 'complete':
            raise ValueError('Complete predecessor baseline required')
        tests = ROOT / 'aws/lambdas' / FN / 'tests/run_tests.py'
        subprocess.run([sys.executable, str(tests)], cwd=ROOT, check=True)
        spec = importlib.util.spec_from_file_location('cycle_offline_tests', tests)
        module = importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        memory = module.Memory();env = module.load(memory)
        retained, dates = [], {}
        for key in sorted(sources.KEYS | {pub.HEAD, pub.MANIFEST}):
            capture = baseline['captures'][key]
            if capture['status'] != 'whole_object_retained':
                raise ValueError('Incomplete cache fixture: ' + key)
            ref = capture['original'];raw = original(ref);memory.seed(key, raw)
            dates[key] = pub.clock(capture['last_modified'])
            retained.append({'source_key': key, 'reference': ref, 'last_modified': capture['last_modified']})
        get = memory.get_object
        def dated_get(**kw):
            obj = get(**kw)
            if kw['Key'] in dates:obj['LastModified'] = dates[kw['Key']]
            return obj
        memory.get_object = dated_get
        url_keys = {env['CLI_URL']: env['CLI_CACHE_KEY'], **{v[0]: v[1] for v in env['LANES'].values()}}
        fixture_http = []
        class Response(BytesIO):
            status = 200
            def __init__(self, raw):
                super().__init__(raw);self.headers = {'Content-Length': str(len(raw))}
        def from_retained_cache(request, timeout):
            key = url_keys[request.full_url];body = pub.decoded(memory.rows[key])
            fixture_http.append({'source_cache': key, 'decoded_bytes': len(body), 'decoded_sha256': pub.sha(body)})
            return Response(body)
        # Test the compiler and evidence path with the complete retained data.
        # These in-memory responses are explicitly not new provider acquisitions.
        with patch.object(sources.urllib.request, 'build_opener') as build:
            build.return_value.open = from_retained_cache
            env['lambda_handler']()
        doc = pub.strict(pub.decoded(memory.rows[pub.HEAD]));meta = pub.strict(memory.rows[pub.MANIFEST])
        result = sources.replay_publication(env, doc, meta, memory.rows.__getitem__)
        if result['countries'] != 34 or result['observations'] < 100000:
            raise ValueError('Whole retained feature population unexpectedly small')
        head = pub.read(s3, BUCKET, pub.HEAD);manifest = pub.read(s3, BUCKET, pub.MANIFEST)
        native = {'status': 'pending_original_1030_schedule', 'version': head['doc'].get('version'), 'generated_at': head['doc']['generated_at']}
        if head['doc'].get('source_evidence'):
            def native_original(key):
                if not key.startswith(pub.PRIVATE) or not key.endswith('.bin'):
                    raise ValueError('Protected native original required')
                return pub.bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body'])
            ref = manifest['doc']['feature_snapshot'];stored = original(ref)
            if stored != head['raw'] or pub.sha(pub.decoded(stored)) != ref['decoded_sha256']:
                raise ValueError('Public complete cycle projection differs')
            native.update(status='complete_native_acquisition_replayed',
                          replay=sources.replay_publication(env, head['doc'], manifest['doc'], native_original))
        if runtime(lam, s3, events, scheduler, FN) != before:
            raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected, actual_runtime=before, baseline_sha256=BASELINE_SHA,
             complete_retained_objects=retained, fixture_response_origin='retained_predecessor_caches_not_new_HTTP_acquisitions',
             fixture_responses=fixture_http, fixture_replay=result,
             fixture_output_projection_sha256=pub.sha(pub.encode(sources.projection(doc))),
             maximum_runner_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
             whole_predecessor_hashes={pub.HEAD: pub.sha(head['raw']), pub.MANIFEST: pub.sha(manifest['raw'])},
             native_publication=native, provider_requests=0, native_invocations=0, archive_writes=0,
             public_writes=0, history_writes=0, account_reads=0, notifications_sent=0, schedules_changed=0,
             scope='Exact package and complete retained-cache in-memory capture/compiler/replay. Native provider acquisition, definitions, first-publication availability and model validation remain separate acceptance gates.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
