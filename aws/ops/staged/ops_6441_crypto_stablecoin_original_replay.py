"""Read-only, complete Crypto stablecoin original replay; never invoke a producer."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,math,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
import crypto_stablecoin_archive as store
from crypto_stablecoin_archive_acceptance import inspect
SOURCE_HASHES = {'aws/ops/checks/crypto_stablecoin_archive_acceptance.py': 'f9c1500e06e42d6c06df1d06ede91fd5aec175b864a728a46ba102336a0d38b9', 'aws/ops/checks/crypto_funding_archive_acceptance.py': '8d22664cc95aabe37fb1056bc22b3e4137106dcb1194dd56dfdbafed91bb95ac', 'aws/ops/checks/term_premium_archive_acceptance.py': 'a0acbd2c24034268d4aa838485b030f3b984b3b60facbc49557f3864249d92a0', 'aws/shared/crypto_stablecoin_archive.py': '0fe0032164161baa532264757a9f5bd55b908b797211fef6d3a2807263cb1835', 'aws/shared/crypto_stablecoin_observations.py': '2f35f08de0d2cd276ff6e3f36a0a7c56900eca7ac052a902f784e57e839e6a16', 'aws/shared/crypto_stablecoin_transport.py': '33ef41f66adb81afb5b514159c05eb7918987c0e423bfe1791223a7a3cf54395', 'aws/lambdas/justhodl-crypto-intel/source/lambda_function.py': 'a89b66c3626f51a3d8c47c9637e4c714a7b128c9f9b3db9121daa115649d2029'}
EXPECTED_DEPLOY_COMMIT = '3aabf6d48263ad0cda166b2aaa2a21911036d8c6'
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
    with report('ops_6441_crypto_stablecoin_original_replay') as out:
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
