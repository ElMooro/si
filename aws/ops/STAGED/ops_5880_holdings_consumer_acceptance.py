"""Verify direct 13F exclusions with whole prior preservation and quiet public refreshes."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import io
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zipfile
import boto3
from botocore.config import Config

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared')]
from ops_report import report
from acceptance_invoke import invoke_when_available

BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/holdings-consumers/'
TARGETS={'justhodl-master-ranker':'data/master-ranker.json',
         'justhodl-regime-composite':'data/regime-composite.json',
         'justhodl-signal-fabric':'data/signal-fabric.json'}


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=20) as response:
            return response.status in (401,403,404)
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=180,tcp_keepalive=True,retries={'max_attempts':0}))
    def raw(key):
        response=s3.get_object(Bucket=BUCKET,Key=key)['Body']
        try:body=response.read(64*1024*1024+1)
        finally:response.close()
        assert len(body)<=64*1024*1024,'Whole artifact bound exceeded'
        return body
    def read(key):return json.loads(raw(key))
    with report('ops_5880_holdings_consumer_acceptance') as r:
        releases={}
        for fn in TARGETS:
            expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+fn],text=True).strip()
            receipt=read('data/ops/releases/'+fn+'.json')
            conf=lam.get_function_configuration(FunctionName=fn)
            assert receipt['commit']==expected and receipt['code_sha256']==conf['CodeSha256']
            assert conf['State']=='Active' and conf['LastUpdateStatus']=='Successful'
            # Verify the actual packaged shared boundary, not just its source name.
            location=lam.get_function(FunctionName=fn)['Code']['Location']
            with urllib.request.urlopen(location,timeout=30) as response:archive=response.read(32*1024*1024+1)
            assert len(archive)<=32*1024*1024
            with zipfile.ZipFile(io.BytesIO(archive)) as z:
                assert z.read('holdings_authority.py')==(ROOT/'aws/shared/holdings_authority.py').read_bytes()
                if fn=='justhodl-master-ranker':assert z.read('private_artifact.py')==(ROOT/'aws/shared/private_artifact.py').read_bytes()
            releases[fn]={'commit':expected,'code_sha256':conf['CodeSha256']}
        r.kv(releases=releases,paid_ai_calls=0,notifications_requested=False,private_account_reads=0)
        keys=[*TARGETS.values(),'data/master-ranker.json.prev','data/regime-composite-history.json',
              'data/feature-bus.json','data/fabric-events.json',
              'data/archive/feature-bus/'+datetime.now(timezone.utc).strftime('%Y%m%d')+'.json']
        preserved=[]
        for key in keys:
            try:body=raw(key)
            except Exception as exc:
                if getattr(exc,'response',{}).get('Error',{}).get('Code')=='NoSuchKey':continue
                raise
            sha=hashlib.sha256(body).hexdigest();destination=PRIVATE+sha+'.bin'
            try:s3.put_object(Bucket=BUCKET,Key=destination,Body=body,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
            except Exception as exc:
                if getattr(exc,'response',{}).get('Error',{}).get('Code') not in ('PreconditionFailed','ConditionalRequestConflict'):raise
            assert raw(destination)==body
            assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+destination) and denied('https://justhodl.ai/'+destination)
            preserved.append({'source':key,'sha256':sha,'bytes':len(body)})
        source_before={key:hashlib.sha256(raw(key)).hexdigest() for key in ('data/13f-positions.json','data/13f-flows-by-ticker.json')}
        observations={}
        for fn,key in TARGETS.items():
            started=datetime.now(timezone.utc)
            response,rejections=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',
                Payload=json.dumps({'suppress_events':True,'suppress_alerts':True,'notify':False}).encode()),wait_seconds=120)
            body=json.loads(response['Payload'].read())
            assert not response.get('FunctionError'), 'Consumer failed; inspect runtime error separately'
            assert body.get('statusCode',200)==200
            packet_bytes=raw(key); packet=json.loads(packet_bytes)
            stamp=packet.get('generated_at') or packet.get('as_of')
            assert datetime.fromisoformat(stamp.replace('Z','+00:00'))>=started.replace(microsecond=0)
            if fn=='justhodl-master-ranker':
                q=packet['holdings_context']
                assert not q['vote_eligible'] and q['independent_votes']==0
                assert all('institutional_13f' not in row['systems'] and 'khalid_note' not in row for row in packet['top_tickers'])
                assert all(all(v['system']!='institutional_13f' for v in row['contributions']) for row in packet['top_tickers'])
                assert any(v.get('key')=='data/notes-index.json' and v.get('exclusion')=='private_source_not_read_by_public_ranker' for v in packet['feed_freshness'])
            elif fn=='justhodl-regime-composite':
                row=next(v for v in packet['modules'] if v['key']=='data/13f-positions.json')
                assert row['vote_eligible'] is False and row['polarity'] is None
                assert all(all(m['label']!=row['label'] for m in dim['members']) for dim in packet['dimensions'].values())
            else:
                assert packet['source_stats']['13f-flows']==0 and not packet['holdings_context']['vote_eligible']
                assert all(all(e['engine']!='13f-flows' for e in row['engines']) for row in packet['tickers'])
                bus=read('data/feature-bus.json')
                assert bus['holdings_context']['vote_eligible'] is False
                assert all(v['flow_13f'] is None for v in bus['tickers'].values())
            assert read('data/ops/releases/'+fn+'.json')['commit']==releases[fn]['commit']
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==releases[fn]['code_sha256']
            observations[fn]={'generated_at':stamp,'packet_sha256':hashlib.sha256(packet_bytes).hexdigest(),
                              'throttle_rejections_before_execution':rejections,'direct_13f_contribution_excluded':True}
            r.kv(consumer=fn,acceptance=observations[fn])
        assert all(hashlib.sha256(raw(key)).hexdigest()==sha for key,sha in source_before.items())
        proof={'contract':'holdings-consumer-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
               'releases':releases,'consumers':observations,'protected_whole_products':preserved,
               'source_packets_unchanged':True,'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,
               'portfolio_writes':0,'remaining':'Indirect composites and remaining 13F consumers are not qualified by these direct exclusions.'}
        s3.put_object(Bucket=BUCKET,Key='data/holdings-consumer-verification.json',Body=json.dumps(proof,sort_keys=True).encode(),
                      ContentType='application/json',CacheControl='no-store')
        assert read('data/holdings-consumer-verification.json')==proof
        r.kv(acceptance_complete=True,proof_key='data/holdings-consumer-verification.json')


if __name__=='__main__':
    try:main()
    except Exception:
        print('Holdings consumer acceptance failed; inspect the committed report before retrying any invocation.')
        sys.exit(1)
