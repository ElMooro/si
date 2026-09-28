"""Retain the complete Momentum Leaders research producer and only their declared public packets.

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
PRIVATE = 'audit-private/20260909-originals/momentum-leaders-research/'
SOURCES = {'justhodl-momentum-leaders': ('1883f6d5f10931a5aec873f4f4c485882b2ca7beaca7ab4158fb115972c3b783', 'data/momentum-leaders.json'), 'justhodl-velocity-acceleration': ('bb0ee22597c318422b07b5dcf21ccbe9b53506144390f742d78fc25cfec93bb4', None), 'justhodl-apex-fusion': ('b05816b42f077ae0b637866046f9b52414b8d44859b05720ebbdc69c903b3b0b', None), 'justhodl-catalyst-clusters': ('021066b32a496d07e8c35c69924f87b652563a9a0ce4aeee09cb7f1a528f6f2b', None), 'justhodl-catalyst-classifier': ('41f8e4dde9bbe1f34182372fcfe4a38de4f7dd6c3b336d6d1c7f3345d812302b', None), 'justhodl-pump-radar-brief': ('f99b2d0631bbf339008dc51012974929c407f08be181dd75a2c40664a8fe9a21', None), 'justhodl-pump-positioning': ('76a89965e65008e4c40e72d701cff74f10586f64b9506733bdced964d7de9129', None), 'justhodl-theme-cascade-backtest': ('ec335f6dff51a33bb4cd106fb375d07aaba402c271498f440e3f9e383f42aa39', None), 'justhodl-theme-cascade': ('5f551b3b118927355c189ca39c112409b4c3bdbd94d11ca8c64234aa723da338', None), 'justhodl-theme-classifier': ('5d4f3a74cd5b7e0b849c7921681eb2a33ca7f0163a9ce253045f36848d4576c1', None)}
KEYS = tuple(value[1] for value in SOURCES.values() if value[1] is not None)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def source_check(function, raw):
    if function not in SOURCES or sha(raw) != SOURCES[function][0]:
        raise ValueError('Unreviewed whole Momentum Leaders producer')
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
        raise ValueError('Undeclared Momentum Leaders research object')
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
    for test in ('tests/ops/test_momentum_leaders_original_baseline.py', 'tests/test_shipping_consumer_baseline.py'):
        subprocess.run([sys.executable, str(ROOT / test)], cwd=ROOT, check=True)
    checked = {fn: source_check(fn, (ROOT / 'aws/lambdas' / fn / 'source/lambda_function.py').read_bytes())
               for fn in SOURCES}
    clients = {name: boto3.client(name, region_name='us-east-1') for name in ('lambda', 's3', 'events', 'scheduler')}
    with report('ops_6265_momentum_leaders_original_baseline') as r:
        s3 = clients['s3']
        producers = {fn: runtime(clients['lambda'], s3, clients['events'], clients['scheduler'], fn) for fn in SOURCES}
        originals = {key: capture(s3, key) for key in KEYS}
        baseline = {'contract': 'momentum-leaders-original-baseline.v1', 'captured_at': datetime.now(timezone.utc).isoformat(),
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
             scope='Whole deployed producer and nine consumer packages; one exact existing public Momentum Leaders producer research packet only. No consumer output or private state is read. Provider response originals, acquisition completeness, original vintages, donor quality, common input ancestry, category joins, source timing, score calibration and signal performance remain unverified.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
