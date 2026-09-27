"""Read-only Capex package/cadence and published-cohort arithmetic acceptance.

No provider acquisition or native invocation. This checks published input
amounts, not complete provider-response originals or accounting periods.
"""
from pathlib import Path
import ast
import json
import subprocess
import sys
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/checks', 'aws/shared', 'aws/ops/staged')]
from ops_report import report
from market_runtime_evidence import runtime, bounded
from ops_6232_sec_search_research_acceptance import check_runtime
from sec_atom_model import strict, sha, clock

FN = 'justhodl-capex-pulse'
KEY = 'data/capex-pulse.json'
BUCKET = 'justhodl-dashboard-live'


def compiler():
    raw = (ROOT / 'aws/lambdas' / FN / 'source/lambda_function.py').read_bytes()
    tree = ast.parse(raw)
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'aggregate_capex']
    if len(nodes) != 1 or nodes[0].decorator_list: raise ValueError('Exact pure cohort compiler required')
    hyperscalers = ast.literal_eval(next(n.value for n in tree.body if isinstance(n, ast.Assign)
                       and any(isinstance(t, ast.Name) and t.id == 'HYPERSCALERS' for t in n.targets)))
    namespace = {}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<isolated pure Capex aggregation>', 'exec'), namespace)
    return namespace['aggregate_capex'], hyperscalers


def publication(raw):
    packet = strict(raw)
    if not isinstance(packet, dict) or not isinstance(packet.get('rows'), list): raise ValueError('Whole Capex population required')
    result = {'status': 'pending_original_schedule_publication', 'bytes': len(raw), 'sha256': sha(raw),
              'generated_at': packet.get('generated_at'), 'version': packet.get('version')}
    if packet.get('version') != '1.1.1': return result
    if (clock(packet.get('generated_at')) is None or packet.get('call') is not None
            or any(packet.get(k) is not False for k in ('calls_eligible', 'forecast_qualified', 'sizing_eligible'))
            or packet.get('quality', {}).get('annual_comparability_verified') is not False
            or packet.get('quality', {}).get('provider_originals_replayed') is not False):
        raise ValueError('Explicit unqualified acquired-window publication required')
    aggregate, names = compiler(); rows = packet['rows']
    if packet.get('n') != len(rows): raise ValueError('Whole declared row count differs')
    for row in rows:
        single = aggregate([row])
        if (row.get('capex_ttm_b') != round(row['current_window_usd'] / 1e9, 2)
                or row.get('yoy_pct') != single['yoy_pct']
                or row.get('annual_comparability_verified') is not False):
            raise ValueError('Published row arithmetic or scope differs')
    sectors = {r['sector'] for r in rows}
    if set(packet.get('sectors', {})) != sectors: raise ValueError('Complete sector population required')
    def compare(actual, population):
        expected = aggregate(population)
        if any(actual.get(k) != v for k, v in expected.items()): raise ValueError('Published cohort arithmetic differs')
        return {k: expected[k] for k in ('n', 'comparison_n', 'comparison_missing_n', 'yoy_pct')}
    groups = {'market': compare(packet['market'], rows)}
    for sector in sorted(sectors):
        groups['sector:' + sector] = compare(packet['sectors'][sector], [r for r in rows if r['sector'] == sector])
    hyp = [r for r in rows if r['ticker'] in names]
    groups['hyperscalers'] = compare(packet['hyperscalers'], hyp)
    if packet['hyperscalers']['rows'] != sorted(hyp, key=lambda r: r['capex_ttm_b'], reverse=True):
        raise ValueError('Whole hyperscaler cohort differs')
    result.update(status='published_cohort_arithmetic_reproduced', groups=groups,
                  provider_originals_replayed=False, annual_comparability_verified=False)
    return result


def main():
    native = ROOT / 'aws/lambdas' / FN / 'source/lambda_function.py'
    subprocess.run([sys.executable, str(native.parent.parent / 'tests/run_tests.py')], cwd=ROOT, check=True)
    original = json.loads((ROOT / 'docs/audit/2026-09-27/accounting-original-baseline.json').read_bytes())['actual_producers'][FN]
    expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/' + FN], cwd=ROOT, text=True).strip()
    clients = {n: boto3.client(n, region_name='us-east-1') for n in ('lambda', 's3', 'events', 'scheduler')}
    args = [clients[n] for n in ('lambda', 's3', 'events', 'scheduler')]
    with report('ops_6234_capex_cohort_acceptance') as r:
        before = runtime(*args, FN); check_runtime(before, original, expected, 2)
        obj = clients['s3'].get_object(Bucket=BUCKET, Key=KEY); raw = bounded(obj['Body'])
        if obj.get('ContentLength') != len(raw): raise ValueError('Whole publication required')
        packet = publication(raw)
        if runtime(*args, FN) != before: raise ValueError('Runtime changed during read-only acceptance')
        r.kv(expected_commit=expected, actual_runtime=before, native_publication=packet,
             native_invocations=0, provider_requests=0, account_reads=0, credential_reads=0,
             learning_log_reads=0, consumer_output_reads=0, public_writes=0, history_writes=0, schedule_changes=0,
             scope='Exact package and original cadence; paired published-amount arithmetic only. Whole provider originals, period/currency alignment, source completeness and investment qualification remain open.')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
