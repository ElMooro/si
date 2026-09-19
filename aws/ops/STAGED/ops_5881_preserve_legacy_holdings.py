"""Retain every current legacy holdings cache/state object before canonical migration."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import re
import sys
import tempfile
import urllib.error
import urllib.request
import boto3

ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/ops'))
from ops_report import report
BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/holdings-legacy-catalog/'
MAX_BYTES=128*1024*1024
KEYS=('data/13f-positions.json','data/13f-flows-by-ticker.json','data/13f-by-ticker.json',
      'data/13f-desk.json','data/13f-price-anchors.json','data/13f-cusip-map.json',
      'data/institutional-positions.json','data/capital-flow.json','data/capital-flow-history.json')


def code(exc):return getattr(exc,'response',{}).get('Error',{}).get('Code')


def hash_stream(stream, sink=None):
    sha=hashlib.sha256();size=0
    try:
        while True:
            block=stream.read(1024*1024)
            if not block:break
            size+=len(block)
            if size>MAX_BYTES:raise ValueError('Whole object exceeds preservation bound; no truncation allowed')
            sha.update(block)
            if sink is not None:sink.write(block)
    finally:stream.close()
    return sha.hexdigest(),size


def preserve(client,key,identity):
    assert key.startswith(('13f-cache/','data/13f-cache/','data/13f-state/')) or key in KEYS
    response=client.get_object(Bucket=BUCKET,Key=key,IfMatch=identity['etag'])
    with tempfile.SpooledTemporaryFile(max_size=8*1024*1024) as held:
        sha,size=hash_stream(response['Body'],held)
        assert size==identity['bytes'] and response['ETag']==identity['etag']
        destination=PRIVATE+sha+'.bin';held.seek(0)
        try:client.put_object(Bucket=BUCKET,Key=destination,Body=held,ContentLength=size,IfNoneMatch='*',
                              ContentType='application/octet-stream',CacheControl='no-store')
        except Exception as exc:
            if code(exc) not in ('PreconditionFailed','ConditionalRequestConflict'):raise
        saved=hash_stream(client.get_object(Bucket=BUCKET,Key=destination)['Body'])
        assert saved==(sha,size),'Private preserved bytes differ'
    return {'source':key,**identity,'source_version_id':response.get('VersionId'),
            'sha256':sha,'private_key':destination,'whole_object_verified':True}


def inventory(client,prefix):
    found={}
    for path in (prefix,'data/13f-state/'):
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=BUCKET,Prefix=path):
            for item in page.get('Contents',[]):
                key=item['Key'];assert key not in found and key.startswith(path)
                found[key]={'etag':item['ETag'],'bytes':item['Size'],'last_modified':item['LastModified'].isoformat()}
    for key in KEYS:
        try:head=client.head_object(Bucket=BUCKET,Key=key)
        except Exception as exc:
            if code(exc) in ('404','NoSuchKey'):continue
            raise
        found[key]={'etag':head['ETag'],'bytes':head['ContentLength'],'last_modified':head['LastModified'].isoformat()}
    return dict(sorted(found.items()))


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=20) as response:return response.status in (401,403,404)
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    client=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    with report('ops_5881_preserve_legacy_holdings') as r:
        policy=json.loads(client.get_bucket_policy(Bucket=BUCKET)['Policy'])
        deny=next(v for v in policy['Statement'] if v.get('Sid')=='Audit20260909ImmutableOriginalBackups')
        assert deny['Effect']=='Deny' and deny['Principal']=='*'
        assert {'s3:GetObject','s3:GetObjectVersion'}<=set(deny['Action'])
        assert deny['Condition']=={'StringNotEquals':{'aws:PrincipalAccount':'857687956942'}}
        assert 'arn:aws:s3:::'+BUCKET+'/audit-private/20260909-originals/*' in deny['Resource']
        # Read only the approved routing values; never report environment secrets.
        conf=lam.get_function_configuration(FunctionName='justhodl-13f-positions')
        env=conf.get('Environment',{}).get('Variables',{})
        assert env.get('S3_BUCKET',BUCKET)==BUCKET
        assert env.get('S3_KEY','data/13f-positions.json')=='data/13f-positions.json'
        prefix=env.get('S3_CACHE_PREFIX','13f-cache/')
        assert prefix in ('13f-cache/','data/13f-cache/')
        before=inventory(client,prefix);assert before and any(k.startswith(prefix) for k in before)
        objects=[preserve(client,key,identity) for key,identity in before.items()]
        assert inventory(client,prefix)==before,'Sources changed during preservation; retained objects remain, but no complete catalog is published'
        catalog={'contract':'holdings-legacy-preservation.v1','generated_at':datetime.now(timezone.utc).isoformat(),
                 'scope':'Whole current cache/state/products. Existing object versions and lifecycle policies are unchanged.',
                 'prefixes':[prefix,'data/13f-state/'],'objects':objects,
                 'absent_named_products':[k for k in KEYS if k not in before],
                 'production_objects_mutated':0,'engine_invocations':0,'paid_ai_calls':0,'private_account_reads':0}
        body=json.dumps(catalog,sort_keys=True,separators=(',',':')).encode();sha=hashlib.sha256(body).hexdigest()
        key=PRIVATE+sha+'.json'
        client.put_object(Bucket=BUCKET,Key=key,Body=body,IfNoneMatch='*',ContentType='application/json',CacheControl='no-store')
        assert hash_stream(client.get_object(Bucket=BUCKET,Key=key)['Body'])==(sha,len(body))
        for destination in (key,objects[0]['private_key']):
            assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+destination) and denied('https://justhodl.ai/'+destination)
        receipt={'contract':'holdings-legacy-preservation-receipt.v1','generated_at':catalog['generated_at'],
                 'catalog_private_key':key,'catalog_sha256':sha,'whole_objects':len(objects),
                 'source_bytes':sum(v['bytes'] for v in objects),'cache_objects':sum(v['source'].startswith(prefix) for v in objects),
                 'state_objects':sum(v['source'].startswith('data/13f-state/') for v in objects),
                 'before_after_catalog_unchanged':True,'production_objects_mutated':0,'anonymous_access_denied':True,
                 'engine_invocations':0,'private_account_reads':0,'paid_ai_calls':0}
        public='data/holdings-research/legacy-preservation/'+sha+'.json'
        client.put_object(Bucket=BUCKET,Key=public,Body=json.dumps(receipt,sort_keys=True).encode(),IfNoneMatch='*',
                          ContentType='application/json',CacheControl='public,max-age=31536000,immutable')
        r.kv(receipt=receipt,public_receipt_key=public)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Legacy holdings preservation failed; production objects were not changed. Inspect the committed report.')
        sys.exit(1)
