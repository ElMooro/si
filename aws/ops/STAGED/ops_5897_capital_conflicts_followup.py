"""Finalize the naturally refreshed public conflict consumer without invoking it."""
from datetime import datetime, timezone
from pathlib import Path
import base64, hashlib, io, json, sys, urllib.error, urllib.request, zipfile
import boto3

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared')]
from ops_report import report
from capital_research_boundary import current_basis, context

BUCKET='justhodl-dashboard-live';FN='justhodl-engine-conflicts'
COMMIT='5d51b7c6422bc1dd41ea24546fdd2b7456202ba0'
PROOF='data/capital-consumer-verification.json'


def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=40) as r:
        raw=r.read(32*1024*1024+1)
    assert len(raw)<=32*1024*1024
    return raw


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    lam=boto3.client('lambda',region_name='us-east-1');s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_5897_capital_conflicts_followup') as r:
        receipt=json.loads(public('data/ops/releases/'+FN+'.json'));cfg=lam.get_function_configuration(FunctionName=FN)
        assert receipt['commit']==COMMIT and cfg['CodeSha256']==receipt['code_sha256']
        assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
        with urllib.request.urlopen(lam.get_function(FunctionName=FN)['Code']['Location'],timeout=40) as response:archive=response.read(32*1024*1024+1)
        assert len(archive)<=32*1024*1024 and base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
        paths=[*(ROOT/'aws/lambdas'/FN/'source').glob('*.py'),ROOT/'aws/shared/capital_research_boundary.py']
        with zipfile.ZipFile(io.BytesIO(archive)) as z:
            for p in paths:assert z.read(p.name)==p.read_bytes()
        before=s3.get_object(Bucket=BUCKET,Key=PROOF)
        try:old_raw=before['Body'].read(4*1024*1024+1)
        finally:before['Body'].close()
        assert len(old_raw)<=4*1024*1024 and json.loads(old_raw)==json.loads(public(PROOF))
        proof=json.loads(old_raw)
        assert proof['releases']['engine-conflicts']['commit']==COMMIT
        body=public('data/engine-conflicts.json');packet=json.loads(body)
        capital=json.loads(public('data/capital-flow.json'))
        stamp=datetime.fromisoformat(packet['generated_at'].replace('Z','+00:00'));now=datetime.now(timezone.utc)
        assert stamp>=datetime.fromisoformat(receipt['deployed_at'].replace('Z','+00:00')) and 0<=(now-stamp).total_seconds()<=7*3600
        assert current_basis(packet) and packet['capital_flow_exclusion']==context(capital)
        assert packet['n_conflicts']==len(packet['conflicts']) and all(v['type']!='FLOW vs PRICE' for v in packet['conflicts'])
        references=[]
        for source,raw in ((PROOF,old_raw),('data/engine-conflicts.json',body)):
            sha=hashlib.sha256(raw).hexdigest();key='audit-private/20260909-originals/capital-consumer-proofs/'+sha+'.bin'
            try:s3.put_object(Bucket=BUCKET,Key=key,Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
            except Exception as exc:
                if getattr(exc,'response',{}).get('Error',{}).get('Code') not in ('PreconditionFailed','ConditionalRequestConflict'):raise
            stream=s3.get_object(Bucket=BUCKET,Key=key)['Body']
            try:assert stream.read(4*1024*1024+1)==raw
            finally:stream.close()
            assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
            references.append({'source':source,'sha256':sha,'bytes':len(raw),'retention':'private_audit_copy','anonymous_denied':True})
        proof['consumers']['engine-conflicts']={'generated_at':packet['generated_at'],'public_sha256':hashlib.sha256(body).hexdigest(),
            'acceptance':'direct_path_verified_from_existing_publication','engine_invocations_this_op':0,'verified_at':now.isoformat()}
        proof['updated_at']=now.isoformat()
        proof['natural_followup']={'engine':'engine-conflicts','verified_at':now.isoformat(),'runtime_commit':COMMIT,
            'code_sha256':cfg['CodeSha256'],'references':references,'engine_invocations':0,
            'scope':'This follow-up updates only Engine Conflicts. Other recorded attestations retain their original verification dates. No specific scheduler trigger or private classifier execution was inspected.'}
        assert lam.get_function_configuration(FunctionName=FN)['CodeSha256']==cfg['CodeSha256']
        assert json.loads(public('data/ops/releases/'+FN+'.json'))['commit']==COMMIT
        s3.put_object(Bucket=BUCKET,Key=PROOF,Body=json.dumps(proof,sort_keys=True,allow_nan=False).encode(),
                      ContentType='application/json',CacheControl='no-store',IfMatch=before['ETag'])
        assert json.loads(public(PROOF))==proof
        r.kv(proof_key=PROOF,runtime_commit=COMMIT,code_sha256=cfg['CodeSha256'],natural_generated_at=packet['generated_at'],
             public_sha256=hashlib.sha256(body).hexdigest(),engine_invocations=0,private_account_reads=0,paid_ai_calls=0,
             notifications_sent=0,portfolio_writes=0,prior_attestation_dates_preserved=True)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Conflict follow-up failed; read the report. Do not invoke the private-context producer as an acceptance retry.')
        sys.exit(1)
