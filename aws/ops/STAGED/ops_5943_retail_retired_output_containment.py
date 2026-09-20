"""Deny anonymous retired account-aware retail state; preserve objects and IAM.
Only bucket-policy metadata and anonymous HEADs are read. No private payload,
Lambda invocation, mirror publication, schedule change or object deletion.
"""
from pathlib import Path
import copy,hashlib,json,sys,urllib.request,urllib.error
ROOT=Path(__file__).resolve().parents[3];sys.path.insert(0,str(ROOT/'aws/ops'))
from ops_report import report
sys.path[:0]=[str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from audit_20260909_security import canonical_statement,policies_equal
BUCKET='justhodl-dashboard-live';ACCOUNT='857687956942'
KEYS=('data/retail-alert-state.json','data/retail-alerts.json')
SID='JustHodlRetiredRetailOperational20260920'
PREFIX='audit-private/20260909-originals/retail-containment/'
def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':')).encode()
def digest(doc):return hashlib.sha256(encoded(doc)).hexdigest()
def statement():
    keys=set(KEYS)|{k.removeprefix('data/') for k in KEYS}
    paths=keys|{'history/archive/feed/'+k+'/*' for k in keys}|{'data/history/archive/feed/'+k+'/*' for k in keys}
    return {'Sid':SID,'Effect':'Deny','Principal':'*','Action':['s3:GetObject','s3:GetObjectVersion'],
        'Resource':sorted('arn:aws:s3:::'+BUCKET+'/'+k for k in paths),
        'Condition':{'StringNotEquals':{'aws:PrincipalAccount':ACCOUNT}}}
def merged(before):
    if not isinstance(before,dict) or before.get('Version')!='2012-10-17' or not isinstance(before.get('Statement'),list):raise ValueError('Reviewed existing policy shape required')
    after=copy.deepcopy(before);matches=[r for r in after['Statement'] if r.get('Sid')==SID]
    if matches and (len(matches)!=1 or canonical_statement(matches[0])!=canonical_statement(statement())):raise ValueError('Existing containment identity conflicts')
    if not matches:after['Statement'].append(statement())
    if len(encoded(after))>20480:raise ValueError('S3 policy size limit; nothing applied')
    return after
def status(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'Cache-Control':'no-cache','User-Agent':'JustHodl-Privacy-Verification/1.0'}),timeout=20) as response:return response.status
    except urllib.error.HTTPError as exc:code=exc.code;exc.close();return code
def main():
    import boto3
    s3=boto3.client('s3',region_name='us-east-1');sts=boto3.client('sts',region_name='us-east-1')
    with report('ops_5943_retail_retired_output_containment') as r:
        assert sts.get_caller_identity()['Account']==ACCOUNT
        before=json.loads(s3.get_bucket_policy(Bucket=BUCKET,ExpectedBucketOwner=ACCOUNT)['Policy']);after=merged(before)
        raw=encoded(before);backup=PREFIX+hashlib.sha256(raw).hexdigest()+'.json'
        try:s3.put_object(Bucket=BUCKET,Key=backup,Body=raw,ContentType='application/json',CacheControl='no-store',IfNoneMatch='*')
        except Exception as exc:
            if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('PreconditionFailed','412'):raise
        obj=s3.get_object(Bucket=BUCKET,Key=backup)['Body']
        try:assert obj.read(20481)==raw
        finally:obj.close()
        assert status('https://'+BUCKET+'.s3.us-east-1.amazonaws.com/'+backup)==403
        # Recheck immediately before write; a concurrent policy change requires review.
        assert policies_equal(json.loads(s3.get_bucket_policy(Bucket=BUCKET,ExpectedBucketOwner=ACCOUNT)['Policy']),before)
        changed=not policies_equal(before,after)
        if changed:s3.put_bucket_policy(Bucket=BUCKET,ExpectedBucketOwner=ACCOUNT,Policy=encoded(after).decode())
        actual=json.loads(s3.get_bucket_policy(Bucket=BUCKET,ExpectedBucketOwner=ACCOUNT)['Policy'])
        assert policies_equal(actual,after)
        checks=[]
        for base in ('https://justhodl.ai/','https://justhodl-data-proxy.raafouis.workers.dev/','https://'+BUCKET+'.s3.us-east-1.amazonaws.com/','https://'+BUCKET+'.s3.amazonaws.com/'):
            for key in KEYS:
                observed=status(base+key);checks.append({'origin':base,'key':key,'status':observed,'body_read':False});assert observed in (401,403)
        r.kv(accepted=True,policy_changed=changed,before_policy_sha256=digest(before),after_policy_sha256=digest(after),
            policy_backup=backup,previous_statements_preserved=True,objects_and_versions_deleted=0,
            private_payloads_read=0,producer_invocations=0,iam_permissions_expanded=False,anonymous_checks=checks)
if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
