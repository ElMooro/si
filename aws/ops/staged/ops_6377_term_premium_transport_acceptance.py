"""Read-only native transport release and whole public predecessor replay.

No current/private/account/consumer packet, provider probe, native invocation,
AWS write or schedule change. Existing original archives validate compatibility;
they cannot establish delivery of a publication produced by the new release.
"""
from pathlib import Path
from datetime import datetime,timezone
from copy import deepcopy
import base64,json,re,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-term-premium/source')]
from market_runtime_evidence import runtime,BUCKET
from term_premium_archive_acceptance import inspect
import term_premium_model as model
import term_premium_store as store
FN='justhodl-term-premium'
BASELINE=ROOT/'docs/audit/2026-09-28/term-premium-timeout-code-acceptance.json'


class ReceiptOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**request):
        if request!={'Bucket':BUCKET,'Key':'data/ops/releases/'+FN+'.json'}:
            raise ValueError('Only the exact Term Premium release receipt may be read')
        return self.client.get_object(**request)


def validate(actual,expected):
    if not re.fullmatch('[a-f0-9]{40}',expected):raise ValueError('Exact source commit required')
    prior=json.loads(BASELINE.read_bytes())['code_and_runtime']['actual_runtime']
    target=deepcopy(prior);target['receipt']={'status':'matched','commit':expected}
    code=actual.get('code_sha256')
    if type(code) is not str or len(base64.b64decode(code,validate=True))!=32:
        raise ValueError('Exact native package hash required')
    target['code_sha256']=code
    if not store.same_json(actual,target):raise ValueError('Exact native receipt or unchanged runtime/schedule differs')


def origin(lam,actual):
    config=lam.get_function_configuration(FunctionName=FN)
    if (config.get('FunctionName')!=FN or config.get('CodeSha256')!=actual['code_sha256']
        or config.get('Environment',{}).get('Variables',{}).get('S3_BUCKET',BUCKET)!=BUCKET):
        raise ValueError('Reviewed native package or publication bucket differs')
    return {'function':FN,'code_sha256':actual['code_sha256'],'configured_bucket_matches_reviewed_source':True}


def jsonable(value):
    def typed(item):
        if isinstance(item,datetime) and item.tzinfo is not None:
            return {'sdk_type':'datetime','iso8601':item.isoformat()}
        raise TypeError('Unreviewed SDK evidence type')
    return json.loads(json.dumps(value,default=typed,allow_nan=False))


def verify(result):
    import boto3
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN+'/source'],cwd=ROOT,text=True).strip()
    lam,s3,events,scheduler=[boto3.client(service,region_name='us-east-1') for service in ('lambda','s3','events','scheduler')]
    clients=(lam,ReceiptOnly(s3),events,scheduler)
    before=runtime(*clients,FN);validate(before,expected);configured=origin(lam,before)
    originals=inspect(s3,BUCKET,store,model,cutoff='2026-09-28T14:06:24Z',checked_at=datetime.now(timezone.utc).isoformat())
    if originals['status']!='complete_original_archive_replayed':raise ValueError('Complete original predecessor replay required')
    after=runtime(*clients,FN);validate(after,expected)
    if not store.same_json(before,after) or origin(lam,after)!=configured:raise ValueError('Runtime changed during original replay')
    evidence={'expected_commit':expected,'native_before':before,'native_after':after,'origin':configured,
              'complete_original_archive':jsonable(originals),'native_invocations':0,'provider_requests':0,
              'current_packet_reads':0,'private_reads':0,'account_reads':0,'consumer_output_reads':0,
              'native_writes':0,'archive_writes':0,'schedule_changes':0,'new_release_publication_verified':False,
              'scope':'Exact new package and original settings/schedule; complete retained predecessor replay with reviewed current code. No current pointer, native publication, schedule causation, first-release or investment qualification claim.'}
    result.kv(evidence=evidence);return evidence


def main():
    from ops_report import report
    with report('ops_6377_term_premium_transport_acceptance') as result:verify(result)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
