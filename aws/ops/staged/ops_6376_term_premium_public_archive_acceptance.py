"""Read-only exact Term Premium runtime and complete original public archives.

No current pointer, account, private predecessor/journal, provider, downstream
consumer, native invoke, publication, schedule change or archive modification.
Archive replay does not prove that the public current pointer was advanced.
"""
from pathlib import Path
from datetime import datetime, timezone
import json
import sys

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-term-premium/source')]
from market_runtime_evidence import runtime,BUCKET
from term_premium_archive_acceptance import inspect
import term_premium_model as model
import term_premium_store as store

FN='justhodl-term-premium'
EXPECTED='33a552073789abd27609e307209077a6c84c8407'
CODE_SHA='4qAD9ijkK0VMmJuFwZUovB4qDUhb7iirJTxNQdWyklM='
CUTOFF='2026-09-28T14:06:24Z'
BASELINE=ROOT/'docs/audit/2026-09-28/term-premium-timeout-code-acceptance.json'


class ReceiptOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**request):
        if request!={'Bucket':BUCKET,'Key':'data/ops/releases/'+FN+'.json'}:
            raise ValueError('Only the exact Term Premium release receipt may be read')
        return self.client.get_object(**request)


def exact_runtime(actual):
    expected=json.loads(BASELINE.read_bytes())['code_and_runtime']['actual_runtime']
    if (actual.get('receipt')!={'status':'matched','commit':EXPECTED}
        or actual.get('code_sha256')!=CODE_SHA or not store.same_json(actual,expected)):
        raise ValueError('Exact accepted Term Premium package, resources or original schedule differs')


def check_origin(lam):
    config=lam.get_function_configuration(FunctionName=FN)
    bucket=config.get('Environment',{}).get('Variables',{}).get('S3_BUCKET',BUCKET)
    if config.get('FunctionName')!=FN or config.get('CodeSha256')!=CODE_SHA or bucket!=BUCKET:
        raise ValueError('Reviewed native source or configured publication bucket differs')
    # No environment variables, credentials or signed package URLs are emitted.
    return {'function':FN,'configured_bucket_matches_reviewed_source':True,'code_sha256':config['CodeSha256']}


def jsonable(value):
    def original_type(item):
        if isinstance(item,datetime):
            if item.tzinfo is None:raise ValueError('SDK timestamp lacks timezone')
            return {'sdk_type':'datetime','iso8601':item.isoformat()}
        raise TypeError('Unreviewed SDK evidence type')
    return json.loads(json.dumps(value,default=original_type,allow_nan=False))


def verify(result):
    import boto3
    clients=[boto3.client(service,region_name='us-east-1') for service in ('lambda','s3','events','scheduler')]
    lam,s3,events,scheduler=clients
    control=(lam,ReceiptOnly(s3),events,scheduler)
    before=runtime(*control,FN);exact_runtime(before);origin=check_origin(lam)
    archive=inspect(s3,BUCKET,store,model,cutoff=CUTOFF,checked_at=datetime.now(timezone.utc).isoformat())
    after=runtime(*control,FN);exact_runtime(after)
    if not store.same_json(before,after) or check_origin(lam)!=origin:
        raise ValueError('Native runtime changed during original archive replay')
    evidence={'expected_commit':EXPECTED,'native_before':before,'native_after':after,'origin':origin,
              'public_original_archive':jsonable(archive),'native_invocations':0,'provider_requests':0,
              'current_packet_reads':0,'private_reads':0,'account_reads':0,'consumer_output_reads':0,
              'native_writes':0,'archive_writes':0,'schedule_changes':0,'current_head_publication_verified':False,
              'scope':'Exact release/package/settings/schedule and complete public original archive replay only. Acquisition clock after repair does not prove schedule causation, current pointer delivery, investment authority or point-in-time qualification.'}
    result.kv(evidence=evidence)
    return evidence


def main():
    from ops_report import report
    with report('ops_6376_term_premium_public_archive_acceptance') as result:verify(result)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
