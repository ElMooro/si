"""Preserve actual SEC keyword consumers before changing signal eligibility.

Code packages, declared runtime and schedules only. No consumer output, account,
learning log, native invocation, provider acquisition or notification is read or run.
"""
from pathlib import Path
from datetime import datetime, timezone
import ast
import json
import subprocess
import sys
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/checks', 'aws/ops/staged')]
from ops_report import report
from ops_6204_shipping_consumer_baseline import runtime
from ops_6229_filing_original_baseline import retain, sha
import retained_access_evidence as access

PINS = {
    'justhodl-best-ideas': 'd5637f2ff46dcc1f406cf1d99caf27ce6902c4858e710d7dcaa25cbe80e992db',
    'justhodl-convergence-radar': '81b53e62b4964fc2ae0e2f9caa1a46e5f1d503d89d2bf04f070921e509af45e1',
    'justhodl-pump-mechanics': '998aa2614ccf51a521118cc6bd9ec376fdac6194b9e681ea18483af12243ce76',
}


def source_check(function, raw):
    if function not in PINS or sha(raw) != PINS[function]:
        raise ValueError('Exact reviewed SEC consumer source required')
    constants = {node.value for node in ast.walk(ast.parse(raw)) if isinstance(node, ast.Constant) and isinstance(node.value, str)}
    if 'data/sec-filings-intel.json' not in constants:
        raise ValueError('Declared SEC dependency required')
    return {'bytes': len(raw), 'sha256': sha(raw), 'native_imported_or_executed': False}


def main():
    for test in ('tests/ops/test_sec_consumer_original_baseline.py', 'tests/test_shipping_consumer_baseline.py'):
        subprocess.run([sys.executable, str(ROOT / test)], cwd=ROOT, check=True)
    checked = {fn: source_check(fn, (ROOT / 'aws/lambdas' / fn / 'source/lambda_function.py').read_bytes()) for fn in PINS}
    clients = {name: boto3.client(name, region_name='us-east-1') for name in ('lambda', 's3', 'events', 'scheduler')}
    with report('ops_6231_sec_consumer_original_baseline') as r:
        s3 = clients['s3']
        consumers = {fn: runtime(clients['lambda'], s3, clients['events'], clients['scheduler'], fn) for fn in PINS}
        baseline = {'contract': 'sec-consumer-code-baseline.v1', 'captured_at': datetime.now(timezone.utc).isoformat(),
                    'snapshot_atomic': False, 'source_checks': checked, 'consumers': consumers}
        ref = retain(s3, json.dumps(baseline, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8'))
        protected = [ref['key'], *[row['whole_zip']['key'] for row in consumers.values() if 'whole_zip' in row]]
        privacy = access.summarize([access.check(key) for key in protected])
        if not privacy['all_denied']: raise ValueError('Original consumer code must remain private')
        summaries = {}
        for fn, row in consumers.items():
            summaries[fn] = {key: value for key, value in row.items() if key not in ('inventory', 'repository_sources')}
            if 'inventory' in row:
                summaries[fn].update({key: row['inventory'][key] for key in ('code_matches_repository', 'source_files_checked', 'source_differences')})
        r.kv(baseline=ref, actual_consumers=summaries, source_checks=checked, **privacy,
             native_invocations=0, provider_requests=0, consumer_output_reads=0, account_reads=0,
             credential_reads=0, learning_log_reads=0, notifications_sent=0, public_writes=0, history_writes=0, schedule_changes=0,
             scope='Whole actual code, reviewed repository sources and unchanged runtime/cadence only. SEC keyword search does not establish a confirmed event or investment edge; consumer eligibility repairs remain pending.')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
