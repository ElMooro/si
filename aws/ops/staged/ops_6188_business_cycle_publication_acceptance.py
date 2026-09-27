"""Read-only exact package and complete retained projection verification.

No provider requests, native invocation, real output writes or schedule changes.
The in-memory publication fixture uses complete retained predecessor outputs;
it is not a replay of their original provider acquisitions or arithmetic.
"""
from pathlib import Path
from datetime import datetime, timezone, timedelta
import importlib.util
import subprocess
import sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/checks', 'aws/lambdas/justhodl-global-business-cycle/source')]
from ops_report import report
from market_runtime_evidence import runtime
import business_cycle_store as store
FN = 'justhodl-global-business-cycle'
BUCKET = 'justhodl-dashboard-live'
BASELINE_SHA = '5385a8c96a3929aa463af0b3e78cd6acba9dc1b99936d4a4863428f1e762f3d9'


def main():
    lam, s3, events, scheduler = (boto3.client(n, region_name='us-east-1') for n in ('lambda', 's3', 'events', 'scheduler'))
    with report('ops_6188_business_cycle_publication_acceptance') as r:
        expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/' + FN], cwd=ROOT, text=True).strip()
        before = runtime(lam, s3, events, scheduler, FN)
        if before['receipt'] != {'status': 'matched', 'commit': expected} or (before['memory_mb'], before['timeout']) != (1536, 900):
            raise ValueError('Exact business-cycle package and original runtime required')
        if before['schedules'] != [{'kind': 'EventBridge rule', 'name': 'justhodl-gbc-daily', 'state': 'ENABLED',
                                   'expression': 'cron(0 12 * * ? *)', 'native_targets': 1}]:
            raise ValueError('Original business-cycle schedule differs')

        def original(ref):
            if ref['key'] != store.PRIVATE + ref['sha256'] + '.bin':
                raise ValueError('Protected original identity required')
            obj = s3.get_object(Bucket=BUCKET, Key=ref['key']);raw = store.bounded(obj['Body'])
            if obj['ContentLength'] != len(raw) or len(raw) != ref['bytes'] or store.sha(raw) != ref['sha256']:
                raise ValueError('Complete retained original differs')
            return raw

        baseline = store.strict(original({'key': store.PRIVATE + BASELINE_SHA + '.bin', 'sha256': BASELINE_SHA, 'bytes': 15423}))
        if baseline['status'] != 'complete':
            raise ValueError('Complete predecessor baseline required')
        tests = ROOT / 'aws/lambdas' / FN / 'tests/run_tests.py'
        subprocess.run([sys.executable, str(tests)], cwd=ROOT, check=True)
        spec = importlib.util.spec_from_file_location('business_cycle_tests', tests)
        module = importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        memory = module.Memory();raws = {}
        for key in store.KEYS:
            capture = baseline['captures'][key]
            if capture['status'] != 'whole_object_retained':
                raise ValueError('Missing complete baseline output')
            raws[key] = original(capture['original']);memory.seed(key, raws[key])
        started = max(store.clock(store.strict(raw)['generated_at']) for raw in raws.values()) + timedelta(days=1)
        publication = store.PublicationClient(memory, BUCKET, started.isoformat())
        compilers = {name: store.sha((ROOT / ('aws/shared/' + name if name == 'managed_secret.py' else 'aws/lambdas/' + FN + '/source/' + name)).read_bytes()) for name in store.COMPILERS}
        planned, population = {}, {}
        for key, raw in raws.items():
            value = store.strict(raw)
            value.update(generated_at=started.isoformat(), engine_version='3.0.4')
            planned[key] = store.encode(value)
            publication.put_object(Bucket=BUCKET, Key=key, Body=planned[key])
            population[key] = {'countries': len(value['by_country']),
                               'country_history_rows': sum(len(row.get('history', [])) for row in value['by_country'].values()),
                               'global_history_rows': len(value.get('global', value.get('aggregate', []))) if key != store.HEAD else 0,
                               'feature_rows': sum(len(row.get('components', [])) for row in value['by_country'].values())}
        proof = publication.finish(compilers)
        for key in store.KEYS:
            result = store.strict(memory.rows[key]);context = result['publication_context']
            if memory.rows[context['predecessors'][key]['key']] != raws[key] or memory.rows[context['complete_unmodified_calculations'][key]['key']] != planned[key]:
                raise ValueError('Complete predecessor or unmodified calculations were not preserved')
            expected_projection = store.research_projection(store.strict(planned[key]))
            if {k: v for k, v in result.items() if k != 'publication_context'} != expected_projection:
                raise ValueError('Complete research projection differs')
            if any(result[k] is not False for k in store.PERMISSIONS) or result['decision']['verb'] != 'WAIT':
                raise ValueError('Unsupported authority retained')
            source = store.strict(planned[key])
            for iso, row in source['by_country'].items():
                if any(result['by_country'][iso].get(field) != value for field, value in row.items()):
                    raise ValueError('Existing country field or whole history changed')
        live = {key: store.read(s3, BUCKET, key) for key in store.KEYS}
        native = {'status': 'pending_original_1200_schedule', 'current_generated_at': live[store.HEAD]['packet']['generated_at'],
                  'version': live[store.HEAD]['packet'].get('engine_version')}
        if live[store.HEAD]['packet'].get('contract') == 'global-business-cycle-research.v1':
            doc = live[store.HEAD]['packet'];ctx = doc['publication_context']
            if ctx['compiler_sha256'] != compilers:
                raise ValueError('Native compiler closure differs')
            for key, ref in ctx['complete_unmodified_calculations'].items():
                if key not in store.KEYS:
                    raise ValueError('Unreviewed publication key')
                source = store.strict(original(ref));expected_doc = {**store.research_projection(source), 'publication_context': ctx}
                if live[key]['packet'] != expected_doc:
                    raise ValueError('Complete native public projection differs')
            for ref in ctx['predecessors'].values():
                if ref:original(ref)
            native['status'] = 'complete_native_derived_projection_verified'
        if runtime(lam, s3, events, scheduler, FN) != before:
            raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected, actual_runtime=before, baseline_sha256=BASELINE_SHA,
             fixture_scope='Complete retained derived calculations projected in memory, not original-provider or arithmetic replay',
             complete_population=population, whole_predecessor_bytes={k: len(v) for k, v in raws.items()},
             whole_fixture_output_sha256={k: store.sha(memory.rows[k]) for k in store.KEYS},
             fixture_publication_order=[k for k in memory.writes if k in store.KEYS],
             native_publication=native, current_head_sha256={k: store.sha(v['raw']) for k, v in live.items()},
             provider_requests=0, native_invocations=0, archive_writes=0, public_writes=0,
             history_writes=0, account_reads=0, notifications_sent=0, schedules_changed=0,
             scope='Complete publication retention and research authority boundary. Source capture/definitions, current-vintage arithmetic, consumer migration and model qualification remain open.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
