"""Retain the complete Options Flow research producer and only their declared public packets.

No native producer import/invocation, provider acquisition, account/configuration
secret read, public overwrite, schedule change or notification. Writes are
immutable private originals and the ordinary operation report only.
"""
from pathlib import Path
from datetime import datetime, timezone
import ast
import hashlib
import json
import subprocess
import sys
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/checks', 'aws/ops/staged')]
from ops_report import report
from market_runtime_evidence import bounded
from ops_6204_shipping_consumer_baseline import runtime
import retained_access_evidence as access

BUCKET = 'justhodl-dashboard-live'
PRIVATE = 'audit-private/20260909-originals/options-flow-scanner-research/'
SOURCES = {'justhodl-options-flow-scanner': ('e5f75781e53c644863a8be172a89980f1056cb89683b824bb06b8fce1b2d1d33', 'data/options-flow-scanner.json')}
SOURCES.update({'justhodl-best-ideas': ('48c041f688796c194d965fe93dcdf25183cdb7694e5a38d6d03bbde7d49b35e0', None), 'justhodl-flow-confluence': ('765149695ce6a970a4846260016efcb44d5c2e65db70fb1ef8644d113b2bc6cc', None), 'justhodl-options-confluence': ('3c93837656a220ebefad01353f7b9d1511b3bbb057fd5878894cf8036df366a5', None)})
KEYS = tuple(value[1] for value in SOURCES.values() if value[1] is not None)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def source_check(function, raw):
    if function not in SOURCES or sha(raw) != SOURCES[function][0]:
        raise ValueError('Unreviewed whole Options Flow producer')
    constants = {n.value for n in ast.walk(ast.parse(raw))
                 if isinstance(n, ast.Constant) and isinstance(n.value, str)}
    if SOURCES[function][1] is not None and SOURCES[function][1] not in constants:
        raise ValueError('Pinned source does not declare the research packet')
    return {'bytes': len(raw), 'sha256': sha(raw), 'imported_or_executed': False}


def retain(s3, raw):
    if not isinstance(raw, bytes) or len(raw) > 64 * 1024 * 1024:
        raise ValueError('Complete bounded original required')
    ref = {'key': PRIVATE + sha(raw) + '.bin', 'bytes': len(raw), 'sha256': sha(raw)}
    try:
        s3.put_object(Bucket=BUCKET, Key=ref['key'], Body=raw, IfNoneMatch='*',
                      ContentType='application/octet-stream', CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in (
                '409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise
    obj = s3.get_object(Bucket=BUCKET, Key=ref['key'])
    back = bounded(obj['Body'])
    if obj.get('ContentLength') != len(back) or back != raw:
        raise ValueError('Complete immutable readback differs')
    return ref


def capture(s3, key):
    if key not in KEYS:
        raise ValueError('Undeclared Options Flow research object')
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('404', 'NoSuchKey'):
            return {'status': 'missing'}
        raise
    raw = bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength'] != len(raw) or not obj.get('ETag'):
        raise ValueError('Complete versioned source required')
    return {'status': 'whole_object_retained', 'original': retain(s3, raw),
            'etag': obj['ETag'], 'last_modified': obj['LastModified'].isoformat(),
            'original_provider_verified': False}


def main():
    for test in ('tests/ops/test_options_flow_original_baseline.py', 'tests/test_shipping_consumer_baseline.py'):
        subprocess.run([sys.executable, str(ROOT / test)], cwd=ROOT, check=True)
    checked = {fn: source_check(fn, (ROOT / 'aws/lambdas' / fn / 'source/lambda_function.py').read_bytes())
               for fn in SOURCES}
    clients = {name: boto3.client(name, region_name='us-east-1') for name in ('lambda', 's3', 'events', 'scheduler')}
    with report('ops_6257_options_flow_original_baseline') as r:
        s3 = clients['s3']
        producers = {fn: runtime(clients['lambda'], s3, clients['events'], clients['scheduler'], fn) for fn in SOURCES}
        originals = {key: capture(s3, key) for key in KEYS}
        baseline = {'contract': 'options-flow-scanner-original-baseline.v1', 'captured_at': datetime.now(timezone.utc).isoformat(),
                    'snapshot_atomic': False, 'source_checks': checked, 'producers': producers, 'captures': originals}
        ref = retain(s3, json.dumps(baseline, sort_keys=True, separators=(',', ':'), allow_nan=False).encode())
        protected = [ref['key']]
        protected.extend(p['whole_zip']['key'] for p in producers.values() if 'whole_zip' in p)
        protected.extend(row['original']['key'] for row in originals.values() if 'original' in row)
        privacy = access.summarize([access.check(key) for key in protected])
        if not privacy['all_denied']:
            raise ValueError('Original code and retained research must remain private')
        summaries = {}
        for fn, actual in producers.items():
            summaries[fn] = {k: v for k, v in actual.items() if k not in ('inventory', 'repository_sources')}
            if 'inventory' in actual:
                summaries[fn].update({k: actual['inventory'][k] for k in ('code_matches_repository', 'source_files_checked', 'source_differences')})
        r.kv(baseline=ref, actual_producers=summaries, source_checks=checked, originals=originals, **privacy,
             native_invocations=0, provider_requests=0, account_reads=0, learning_log_reads=0, notifications_sent=0,
             public_writes=0, history_writes=0, schedule_changes=0,
             scope='Whole deployed producer and three consumer packages; one exact existing public Options Flow producer research packet only. No consumer output is read. Provider response originals, acquisition completeness, original vintages, donor quality, common input ancestry, category joins, source timing, score calibration and signal performance remain unverified.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
