"""Verify actual package and retained-cache curve separation; never invoke."""
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
import importlib.util
import subprocess
import sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/checks', 'aws/lambdas/justhodl-cycle-features/source')]
from ops_report import report
from market_runtime_evidence import runtime
import cycle_publication as pub
FN = 'justhodl-cycle-features'
BUCKET = 'justhodl-dashboard-live'
BASELINE_SHA = '14a62c2163f78b9fd32fa2b8bbb9acc3ff92da141f88009b6b9a1621df0a5ef9'


def main():
    lam, s3, events, scheduler = (boto3.client(n, region_name='us-east-1') for n in ('lambda', 's3', 'events', 'scheduler'))
    with report('ops_6183_cycle_definition_acceptance') as r:
        expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/' + FN + '/source/cycle_publication.py'], cwd=ROOT, text=True).strip()
        before = runtime(lam, s3, events, scheduler, FN)
        if before['receipt'] != {'status': 'matched', 'commit': expected} or (before['memory_mb'], before['timeout']) != (3008, 600):
            raise ValueError('Exact cycle package and original runtime required')
        if before['schedules'] != [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-cycle-features-daily', 'state': 'ENABLED',
                                    'expression': 'cron(30 10 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}]:
            raise ValueError('Original cycle schedule differs')
        def original(ref):
            if ref['key'] != pub.PRIVATE + ref['sha256'] + '.bin':
                raise ValueError('Protected original identity required')
            obj = s3.get_object(Bucket=BUCKET, Key=ref['key'])
            raw = pub.bounded(obj['Body'])
            if len(raw) != ref['bytes'] or obj['ContentLength'] != len(raw) or pub.sha(raw) != ref['sha256']:
                raise ValueError('Complete retained cache differs')
            return raw
        baseline = pub.strict(original({'key': pub.PRIVATE + BASELINE_SHA + '.bin', 'sha256': BASELINE_SHA, 'bytes': 20921}))
        if baseline['status'] != 'complete':
            raise ValueError('Complete predecessor required')
        tests = ROOT / 'aws/lambdas' / FN / 'tests/run_tests.py'
        subprocess.run([sys.executable, str(tests)], cwd=ROOT, check=True)
        spec = importlib.util.spec_from_file_location('cycle_offline_tests', tests)
        module = importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        env = module.load(None)
        used = []
        def retained(key):
            if key not in ('data/warm/oecd/cycle/DF_KEI.csv.gz', 'data/warm/oecd/cycle/DF_FINMARK.csv.gz',
                           'data/global-sovereign.json', 'data/asia-leads.json'):
                raise ValueError('Only explicit retained curve inputs allowed')
            capture = baseline['captures'][key]
            if capture['status'] != 'whole_object_retained':
                raise ValueError('Original cache unavailable')
            ref = capture['original'];raw = original(ref)
            used.append({'source_key': key, 'reference': ref, 'captured_at': capture['captured_at'], 'last_modified': capture['last_modified']})
            return pub.decoded(raw)
        env['fetch_lane'] = lambda name, status, *args, **kw: (retained({'KEI': 'data/warm/oecd/cycle/DF_KEI.csv.gz', 'FINMARK': 'data/warm/oecd/cycle/DF_FINMARK.csv.gz'}[name]), 'retained baseline cache')
        env['get_json'] = lambda key: pub.strict(retained(key))
        features = {iso: defaultdict(lambda: env['Series']('M')) for iso in env['ISO3']}
        status, context = {}, {}
        env['load_oecd_kei'](features, status)
        env['load_oecd_finmark'](features, status)
        curves = {iso: dict(f['curve'].d) for iso, f in features.items() if f.get('curve')}
        env['load_fleet_feeds'](features, status, context)
        after = {iso: dict(f['curve'].d) for iso, f in features.items() if f.get('curve')}
        if curves != after or status['global_sovereign_curve_nowcast']['countries_extended'] != 0 or context['global_sovereign']['monthly_observation'] is not False:
            raise ValueError('Unverified sovereign context changed monthly curves')
        heads = {key: pub.read(s3, BUCKET, key) for key in (pub.HEAD, pub.MANIFEST)}
        head = heads[pub.HEAD]['doc'];manifest = heads[pub.MANIFEST]['doc']
        publication = {'status': 'pending_original_1030_schedule', 'generated_at': head['generated_at'], 'version': head.get('version')}
        if head.get('publication_context'):
            ref = manifest['feature_snapshot'];raw = original(ref)
            if raw != heads[pub.HEAD]['raw'] or pub.sha(pub.decoded(raw)) != ref['decoded_sha256'] or len(pub.decoded(raw)) != ref['decoded_bytes']:
                raise ValueError('Whole public feature snapshot differs')
            for packet in (head, manifest):
                if any(packet.get(k) is not False for k in pub.PERMISSIONS):
                    raise ValueError('Unqualified feature authority differs')
            if head['sources']['global_sovereign_curve_nowcast']['countries_extended'] != 0:
                raise ValueError('Synthetic monthly extension remains')
            publication['status'] = 'normal_derived_publication_verified_not_original_source_replay'
        if runtime(lam, s3, events, scheduler, FN) != before:
            raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected, actual_runtime=before, baseline_sha256=BASELINE_SHA, retained_inputs=used,
             curve_countries=len(curves), curve_observations=sum(len(v) for v in curves.values()),
             curve_hash_before=pub.sha(pub.encode(curves)), curve_hash_after=pub.sha(pub.encode(after)),
             country_latest_period={iso: max(values) for iso, values in curves.items()},
             whole_predecessor_hashes={key: pub.sha(value['raw']) for key, value in heads.items()},
             native_publication=publication, provider_requests=0, native_invocations=0, archive_writes=0,
             public_writes=0, history_writes=0, account_reads=0, notifications_sent=0, schedules_changed=0,
             scope='Exact package, complete retained-cache curve compiler and no mutation from sovereign context. Existing definitions, first-publication availability, wider feature transforms and model validation remain unqualified.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
