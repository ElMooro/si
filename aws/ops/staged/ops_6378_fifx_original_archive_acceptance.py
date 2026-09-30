"""Read-only FI/FX native identity and full original archive recovery check.

Never reads a current/private/account/consumer packet or journal; never invokes
a producer, probes a provider, changes a schedule or writes an AWS object.
"""
from pathlib import Path
from datetime import datetime, timezone
import json
import subprocess
import sys
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/lambdas/justhodl-fifx-vol-migration/source', 'aws/ops', 'aws/ops/checks', 'aws/shared')]
from market_runtime_evidence import runtime, BUCKET
from fifx_archive_acceptance import inspect
import fifx_store as store
import fifx_model as model
FN = 'justhodl-fifx-vol-migration'
BASELINE = ROOT/'docs/audit/2026-09-28/fifx-concurrent-release-acceptance.json'
EXPECTED = '3be085cd8e35efe29869483ebb485e8d215ad495'


class ReceiptOnly:
    def __init__(self, client): self.client = client
    def get_object(self, **request):
        if request != {'Bucket': BUCKET, 'Key': 'data/ops/releases/'+FN+'.json'}:
            raise ValueError('Only the exact FI/FX release receipt may be read')
        return self.client.get_object(**request)


def validate(actual):
    original = json.loads(BASELINE.read_bytes())['evidence']['actual_runtime']
    if (actual.get('receipt') != {'status': 'matched', 'commit': EXPECTED}
        or not store.same_json(actual, original)):
        raise ValueError('Exact repaired FI/FX receipt, package, resources and original schedule required')


def origin(lam, actual):
    config = lam.get_function_configuration(FunctionName=FN)
    if (config.get('FunctionName') != FN or config.get('CodeSha256') != actual['code_sha256']
        or config.get('Environment', {}).get('Variables', {}).get('S3_BUCKET', BUCKET) != BUCKET):
        raise ValueError('Reviewed native package or publication bucket differs')
    return {'function': FN, 'code_sha256': actual['code_sha256'], 'configured_bucket_matches_reviewed_source': True}


def jsonable(value):
    def typed(item):
        if isinstance(item, datetime) and item.tzinfo is not None:
            return {'sdk_type': 'datetime', 'iso8601': item.isoformat()}
        raise TypeError('Unreviewed SDK evidence type')
    return json.loads(json.dumps(value, default=typed, allow_nan=False))


def verify(result):
    import boto3
    source = subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN+'/source'], cwd=ROOT, text=True).strip()
    if source != EXPECTED: raise ValueError('The reviewed native source changed before acceptance')
    lam, s3, events, scheduler = [boto3.client(service, region_name='us-east-1') for service in ('lambda','s3','events','scheduler')]
    clients = (lam, ReceiptOnly(s3), events, scheduler)
    before = runtime(*clients, FN); validate(before); configured = origin(lam, before)
    originals = inspect(s3, BUCKET, store, model, cutoff='2026-09-28T21:53:24Z', checked_at=datetime.now(timezone.utc).isoformat())
    after = runtime(*clients, FN); validate(after)
    if not store.same_json(before, after) or origin(lam, after) != configured:
        raise ValueError('Native runtime changed during complete original replay')
    evidence = {'expected_commit': EXPECTED, 'native_before': before, 'native_after': after, 'origin': configured,
                'complete_original_archive': jsonable(originals), 'native_invocations': 0, 'provider_requests': 0,
                'current_packet_reads': 0, 'private_reads': 0, 'account_reads': 0, 'consumer_output_reads': 0,
                'native_writes': 0, 'archive_writes': 0, 'schedule_changes': 0,
                'scope': 'Exact repaired native package and original operating settings; full retained public archive replay. Source availability is separate from replay integrity. No current publication, schedule causation, first-release or investment qualification claim.'}
    result.kv(evidence=evidence); return evidence


def main():
    from ops_report import report
    with report('ops_6378_fifx_original_archive_acceptance') as result: verify(result)


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
