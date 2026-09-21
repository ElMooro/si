"""Qualify retained parent composition; no producer invocation or head write."""
from pathlib import Path
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib, json, subprocess, sys, time
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/ops/staged'), str(ROOT/'aws/ops/checks'), str(ROOT/'aws/shared')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6001_massive_dependency_preflight as baseline
import massive_research_model as model
import massive_research_store as store
BUCKET = baseline.BUCKET
REQUEST = 'chatgpt-ops-6003-massive-composition-candidate'


def independent(output, read):
    """Independently reconcile parent identities, edges and every row pointer.

This checks composition against recorded parent outputs, not source-provider
arithmetic already handled by the parent engines' separate qualifications.
"""
    graph = output['dependency_graph']; bodies = {}; roots = {}; edges = set()
    for node_id, node in graph['nodes'].items():
        kind = node['kind']; _, contract, prefix, run_contract, run_kind = model.SOURCES[kind]
        run_raw = read(node['replay']['manifest_key']); run = json.loads(run_raw)
        assert node['replay']['manifest_key'] == prefix+'runs/'+hashlib.sha256(run_raw).hexdigest()+'.json'
        assert run['contract'] == run_contract and (run_kind is None or run['kind'] == run_kind)
        raw = read(node['output']['key']); packet = json.loads(raw); digest = hashlib.sha256(raw).hexdigest()
        assert node['output'] == run['output'] == {'key': prefix+'outputs/'+digest+'.json', 'sha256': digest, 'bytes': len(raw)}
        assert digest == node['replay']['output_sha256'] == run['output_sha256']
        assert packet['contract'] == contract and packet['generated_at'] == run['generated_at'] == node['generated_at']
        assert all(packet[k] is False for k in model.FLAGS)
        assert node_id == kind+':'+digest
        bodies[node_id] = packet
        if kind == 'populations': edges.add((node_id, 'options:'+packet['source_run']['output_sha256']))
        elif kind == 'etf_desk':
            for role, parent in (('flows', 'fund_flows'), ('holdings', 'holdings')):
                edges.add((node_id, parent+':'+packet['canonical_sources'][role]['replay']['output_sha256']))
            roots.setdefault('fund_profile', set()).add(node_id)
            if packet['quality'].get('additional_funds'):
                roots.setdefault('fund_flow', set()).add(node_id); roots.setdefault('constituents', set()).add(node_id)
        else: roots.setdefault({'options': 'option_chain', 'fund_flows': 'fund_flow', 'holdings': 'constituents'}[kind], set()).add(node_id)
    assert {(e['view'], e['source']) for e in graph['edges']} == edges
    assert all(a in bodies and b in bodies for a, b in edges)
    assert {row['family']: set(row['nodes']) for row in graph['measurement_families']} == roots
    assert graph['statistical_independence_established'] is False
    expected = set(); counts = Counter()
    for kind, row in output['sources'].items():
        if row['node'] is None: continue
        body = bodies[row['node']]; capture = row['capture']
        raw = read(capture['original']['key']); original = json.loads(raw)
        assert hashlib.sha256(raw).hexdigest() == capture['original']['sha256']
        assert len(raw) == capture['original']['bytes']
        assert {k: v for k, v in original.items() if k != 'replay'} == body
        assert original['replay'] == graph['nodes'][row['node']]['replay']
        collection = 'chains' if kind == 'options' else 'underlyings' if kind == 'populations' else 'funds'
        assert row['instrument_count'] == len(body[collection])
        for symbol, value in body[collection].items():
            expected.add((symbol, kind, row['node'], '/'+collection+'/'+symbol)); counts['parent_instrument_rows'] += 1
            if kind == 'options':
                counts['option_returned_rows'] += value['coverage']['returned_rows']
                counts['option_eligible_rows'] += value['coverage']['eligible_identity_rows']
            elif kind == 'populations':
                counts['population_eligible_rows'] += value['totals']['counts']['identity_eligible_rows']
                counts['population_expiry_strike_groups'] += value['expiry_strike_groups']
    actual = []
    for ticker, rows in output['instruments'].items():
        for row in rows:
            actual.append((ticker, row['source'], row['node'], row['pointer']))
            fields = row['pointer'].split('/')[1:]; value = bodies[row['node']]
            for field in fields: value = value[field]
            assert isinstance(value, dict)
    assert len(actual) == len(set(actual)) and set(actual) == expected
    assert all(output[k] is False for k in model.FLAGS)
    assert output['independent_investment_votes'] == graph['independent_investment_votes'] == 0
    assert output['verification']['original_provider_replay_performed_by_composite'] is False
    return dict(counts)


def main():
    import resource
    s3 = boto3.client('s3', region_name='us-east-1'); lam = boto3.client('lambda', region_name='us-east-1')
    events = boto3.client('events', region_name='us-east-1'); scheduler = boto3.client('scheduler', region_name='us-east-1')
    read = store.reader(s3, BUCKET)
    with report('ops_6003_massive_composite_candidate') as r:
        subprocess.run([sys.executable, str(ROOT/'tests/test_massive_research.py')], cwd=ROOT, check=True)
        subprocess.run([sys.executable, str(ROOT/'tests/test_massive_composite_candidate.py')], cwd=ROOT, check=True)
        runtime = baseline.evidence.runtime(lam, s3, events, scheduler, 'justhodl-massive-signals')
        packages = baseline.check_packages(lam, ROOT, baseline.CONSUMERS)
        assert all(p['pass'] for p in packages), 'Read the actual consumer package mismatches first'
        key = store.request_key(REQUEST)
        try: s3.head_object(Bucket=BUCKET, Key=key)
        except Exception as exc:
            if not store.missing(exc): raise
        else: raise RuntimeError('Candidate request already exists; inspect retained evidence instead of repeating')
        try: s3.head_object(Bucket=BUCKET, Key=model.CURRENT)
        except Exception as exc:
            if not store.missing(exc): raise
        else: raise RuntimeError('Native composite head exists; inspect before migration')
        started = time.monotonic()
        status = store.run(s3, BUCKET, REQUEST, 'runner-ops-6003', publish_current=False)
        elapsed = time.monotonic()-started
        r.kv(candidate_status_key=key, candidate_replay=status.get('replay'), candidate_seconds=round(elapsed, 3))
        assert status['status'] == 'complete' and status['published'] is False and status['aliases'] == {}
        run = store.verified_run(status['replay'], read); inputs = store.checked(run['input'], 'inputs', read)
        output = store.checked(run['output'], 'outputs', read)
        assert store.replay(status['replay'], read) == output
        counts = independent(output, read)
        assert output['quality']['bound_native_sources'] == 5
        for k, capture in inputs['predecessors'].items():
            assert model.original(capture['original'], read) == baseline.evidence.bounded(s3.get_object(Bucket=BUCKET, Key=k)['Body'])
        assert baseline.evidence.runtime(lam, s3, events, scheduler, 'justhodl-massive-signals') == runtime
        try: s3.head_object(Bucket=BUCKET, Key=model.CURRENT)
        except Exception as exc:
            if not store.missing(exc): raise
        else: raise AssertionError('Candidate changed a current public head')
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        candidate = {'contract': 'massive-composite-candidate.v1', 'generated_at': store.now(),
            'candidate_replay': status['replay'], 'candidate_status_key': key, 'counts': counts,
            'native_source_clocks': {k: v.get('source_generated_at') for k, v in output['sources'].items()},
            'measurement_families': output['dependency_graph']['measurement_families'],
            'quality': output['quality'], 'predecessor_runtime': runtime, 'consumer_packages': packages,
            'candidate_seconds': round(elapsed, 3), 'peak_runner_rss_kib': peak,
            'composition_replay_verified': True, 'parent_outputs_independently_checked': True,
            'original_provider_replay_performed_by_this_audit': False,
            'provider_requests': 0, 'engine_invocations': 0, 'public_head_writes': 0, 'schedules_changed': 0,
            'private_account_reads': 0, 'paid_ai_calls': 0, 'notifications_sent': 0, 'portfolio_writes': 0}
        retained = baseline.baseline.retain(s3, model.encoded(candidate))
        protected = {key, retained['key'], *(c['original']['key'] for c in [*inputs['sources'].values(), *inputs['predecessors'].values()] if c['original'])}
        r.kv(retained_candidate=retained, counts=counts, quality=output['quality'], peak_runner_rss_kib=peak,
            protected_artifacts_pending=len(protected))
        def deny(k):
            assert denied_with_retry('https://justhodl.ai/'+k) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+k)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny, sorted(protected)): pass
        candidate.update(privacy_verified=True, protected_artifacts_checked=len(protected), candidate_evidence=retained)
        accepted = baseline.baseline.retain(s3, model.encoded(candidate)); deny(accepted['key'])
        r.kv(accepted_candidate=accepted, candidate_replay=status['replay'], counts=counts,
            candidate_seconds=round(elapsed, 3), peak_runner_rss_kib=peak, protected_artifacts_checked=len(protected)+1,
            originals_anonymously_denied=True, provider_requests=0, engine_invocations=0, public_head_writes=0,
            schedules_changed=0, private_account_reads=0, paid_ai_calls=0, notifications_sent=0, portfolio_writes=0)


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
