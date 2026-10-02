"""Read-only, complete Crypto funding original replay; never invoke a producer."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,math,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
import crypto_funding_archive as store
from crypto_funding_archive_acceptance import inspect
SOURCE_HASHES = {'aws/ops/checks/crypto_funding_archive_acceptance.py': '8d22664cc95aabe37fb1056bc22b3e4137106dcb1194dd56dfdbafed91bb95ac', 'aws/ops/checks/term_premium_archive_acceptance.py': 'a0acbd2c24034268d4aa838485b030f3b984b3b60facbc49557f3864249d92a0', 'aws/shared/crypto_funding_archive.py': 'aff1c225cf6877f90a493f60714c9680cfb6c8b7c07c9d6cbcaff9fc64dbef53', 'aws/shared/crypto_funding_observations.py': '7ca2f58d527887d1569d4906f5a6135446bb7636a51a4683562c543e80a7e3bc', 'aws/lambdas/justhodl-crypto-intel/source/lambda_function.py': '3af28baf464bbcf1a151841caa1b54099a375c87ee817d70dfcbdc7490c1f3d5'}
EXPECTED_DEPLOY_COMMIT = 'fb02a5a5030099353b27fcc90aa5ac52119284a0'
BUCKET='justhodl-dashboard-live'
RECEIPT='data/ops/releases/justhodl-crypto-intel.json'


def jsonable(value):
    if isinstance(value,datetime):
        if value.tzinfo is None:raise ValueError('Aware native report timestamp required')
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value,dict):
        if not all(isinstance(key,str) for key in value):raise ValueError('Named native metadata required')
        return {key:jsonable(item) for key,item in value.items()}
    if isinstance(value,list):return [jsonable(item) for item in value]
    if value is None or type(value) in (str,bool,int):return value
    if type(value) is float and math.isfinite(value):return value
    raise ValueError('Unsupported original report value')


def receipt(client):
    obj=client.get_object(Bucket=BUCKET,Key=RECEIPT)
    body,size=obj['Body'],obj.get('ContentLength')
    try:
        if type(size) is not int or not 0<size<=1024*1024:
            raise ValueError('Whole bounded public release receipt required')
        parts=[];count=0
        while count<=size:
            chunk=body.read(min(65536,size+1-count))
            if not isinstance(chunk,bytes):raise ValueError('Binary release receipt required')
            if not chunk:break
            parts.append(chunk);count+=len(chunk)
        raw=b''.join(parts)
    finally:body.close()
    if len(raw)!=size:raise ValueError('Release receipt length differs')
    doc=store.strict(raw)
    if doc.get('commit')!=EXPECTED_DEPLOY_COMMIT:raise ValueError('Exact intended deployed commit required')
    store.clock(doc.get('deployed_at'))
    return doc,hashlib.sha256(raw).hexdigest()


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    if len(EXPECTED_DEPLOY_COMMIT)!=40 or not SOURCE_HASHES:raise ValueError('Reviewed original acceptance specification required')
    for path,digest in SOURCE_HASHES.items():
        if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=digest:raise ValueError('Reviewed source changed: '+path)
    client=boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=3,read_timeout=10,retries={'total_max_attempts':1}))
    with report('ops_6439_crypto_funding_original_replay') as out:
        before,receipt_digest=receipt(client)
        result=inspect(client,BUCKET,store,cutoff=before['deployed_at'],checked_at=datetime.now(timezone.utc).isoformat())
        after,after_digest=receipt(client)
        if before!=after or receipt_digest!=after_digest:raise ValueError('Release changed during original acceptance')
        out.kv(evidence={'status':result['status'],'expected_commit':EXPECTED_DEPLOY_COMMIT,
                        'release_receipt_sha256':receipt_digest,'reviewed_source_hashes':SOURCE_HASHES,
                        'original_archive':jsonable(result),'provider_requests':0,'native_invocations':0,
                        'native_writes':0,'schedule_changes':0,'application_log_queries':0,
                        'application_packet_reads':0,'private_reads':0,'account_reads':0,
                        'normal_current_publication_verified':False,'investment_authority':False})

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
