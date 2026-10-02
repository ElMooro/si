"""Read-only, complete Crypto sentiment original replay; never invoke a producer."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,math,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
import crypto_sentiment_archive as store
from crypto_sentiment_archive_acceptance import inspect
SOURCE_HASHES = {'aws/ops/checks/crypto_sentiment_archive_acceptance.py': '01d7b83530e6b89f61744f3231adbfc7d6c5327de2926e15f36026b48a9d878e', 'aws/ops/checks/crypto_funding_archive_acceptance.py': '8d22664cc95aabe37fb1056bc22b3e4137106dcb1194dd56dfdbafed91bb95ac', 'aws/ops/checks/term_premium_archive_acceptance.py': 'a0acbd2c24034268d4aa838485b030f3b984b3b60facbc49557f3864249d92a0', 'aws/shared/crypto_sentiment_archive.py': '01eb0dd803ed0cfeb57febfa127930047e73e7be79346c985ddd0d255d804a0b', 'aws/shared/crypto_sentiment_observations.py': 'a53f747a138a3d85a61092d9aa59064945456d0105c47473a69ff9530a66b43c', 'aws/shared/crypto_sentiment_transport.py': '142836cedc3e5fa4d498765f49a59d0e7ac23ed3454241d97588ee5f7409b304', 'aws/lambdas/justhodl-crypto-intel/source/lambda_function.py': 'ec3467b2237725d32551a2a7a43a3ab82ff6dbc694fac2500920cab8be9f1432'}
EXPECTED_DEPLOY_COMMIT = 'd1f73fa57616453c28e4203f9b7cd2cdc6f08936'
EXPECTED_RELEASE_IDENTITY = {'schema': 'release-receipt.v1', 'function': 'justhodl-crypto-intel', 'code_sha256': '60cgtwTEAYwNSBQhtce5jqrEnn5N+wfUflm3nbtoMIY=', 'verified': True, 'zip_bytes': 1107136, 'zip_sha256_hex': 'eb4720b704c4018c0d481421b5c7b98eaac49e7e4dfb07d47e59b79dbb683086', 'source': {'lambda_function.py': {'bytes': 61261, 'sha256': 'ec3467b2237725d32551a2a7a43a3ab82ff6dbc694fac2500920cab8be9f1432'}}}
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
    if any(type(doc.get(key)) is not type(value) or doc[key]!=value for key,value in EXPECTED_RELEASE_IDENTITY.items()):
        raise ValueError('Exact native-accepted function, package and handler identity required')
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
    with report('ops_6445_crypto_sentiment_original_replay') as out:
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
