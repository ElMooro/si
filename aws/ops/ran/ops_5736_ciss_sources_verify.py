"""Verify exact CISS runtimes, real ECB originals and deterministic commentary."""
from datetime import datetime, timezone
import gzip
import io
import json
from pathlib import Path
import subprocess
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'scripts'),str(ROOT/'aws/shared')]
from ops_report import report
from replay_ciss_research import replay,replay_commentary
import ciss_source_model as model


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--',
        'aws/lambdas/justhodl-ciss-stress/source/lambda_function.py'],text=True).strip()
    assert len(expected)==40,'source revision missing'
    bucket='justhodl-dashboard-live';s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    events=boto3.client('events',region_name='us-east-1')
    def raw(key):
        body=s3.get_object(Bucket=bucket,Key=key)['Body'].read(64*1024*1024+1)
        assert len(body)<=64*1024*1024,'source bound exceeded'
        if key.endswith('.gz'):
            with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:body=stream.read(64*1024*1024+1)
            assert len(body)<=64*1024*1024,'decompressed source bound exceeded'
        return body
    def read(key):return json.loads(raw(key))
    def receipt(fn):
        try:return read('data/ops/releases/'+fn+'.json')
        except ClientError as exc:
            if exc.response.get('Error',{}).get('Code') in ('NoSuchKey','404'):return None
            raise
    with report('ops_5736_ciss_sources_verify') as r:
        names=['justhodl-ciss-stress','justhodl-ciss-ai'];runtimes={};deadline=time.monotonic()+1800
        while len(runtimes)!=len(names):
            for fn in names:
                if fn in runtimes:continue
                row=receipt(fn)
                if row is None or row.get('commit')!=expected:continue
                conf=lam.get_function_configuration(FunctionName=fn)
                assert conf['CodeSha256']==row['code_sha256'],fn+' runtime differs'
                runtimes[fn]={'commit':expected,'code_sha256':conf['CodeSha256']}
            assert time.monotonic()<deadline,'exact runtime receipt timeout'
            if len(runtimes)!=len(names):time.sleep(15)
        r.kv(runtimes=runtimes)
        invoked=[];before=datetime.now(timezone.utc).isoformat()
        for fn in names:
            response=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
            payload=json.loads(response['Payload'].read())
            assert not response.get('FunctionError') and payload.get('statusCode')==200,payload
            invoked.append(fn)
        packet=read('data/ciss-stress.json');comment=read('data/ciss-ai.json')
        assert packet['generated_at']>before and packet['contract']==model.CONTRACT
        assert packet['quality']['status']=='fresh' and packet['headline_reconciliation']['status']=='matched'
        assert packet['n_series']==packet['discovered_series']>=80,'incomplete source coverage'
        assert packet['errors']=={},packet['errors']
        manifest=read(packet['replay']['manifest_key'])
        assert set(manifest['discoveries'])=={'CISS','CLIFS'}
        rebuilt=replay(manifest,read=raw)
        assert rebuilt=={k:v for k,v in packet.items() if k!='replay'},'original warehouse replay differs'
        cmanifest=read(comment['replay']['manifest_key'])
        assert replay_commentary(cmanifest,read=raw)=={k:v for k,v in comment.items() if k!='replay'},'original commentary replay differs'
        assert len(comment['claims'])==7 and comment['source_replay']==packet['replay']
        assert comment['call'] is None and not comment['calls_eligible'] and not comment['sizing_eligible']
        assert packet['ea_regime'] is None and not packet['calls_eligible']
        samples={}
        for key in [model.HEAD,'CISS.D.DE.Z0Z.4F.EC.SS_CIN.IDX','CISS.D.ES.Z0Z.4F.EC.SS_CIN.IDX','CLIFS.M.AT._Z.4F.EC.CLIFS_CI.IDX']:
            row=next(x for x in packet['series'] if x['key']==key)
            if row['observation_status']=='M':assert row['latest'] is None and row['quality']['status']=='missing'
            samples[key]={k:row[k] for k in ('latest','latest_decimal','latest_date','observation_status','unit','quality','evidence')}
        schedules={}
        for fn,rule in [('justhodl-ciss-stress','ciss-stress-daily'),('justhodl-ciss-ai','ciss-ai-daily')]:
            item=events.describe_rule(Name=rule);targets=events.list_targets_by_rule(Rule=rule)['Targets']
            assert item['State']=='ENABLED' and any(t['Arn'].endswith(':function:'+fn) for t in targets)
            schedules[fn]={'rule':rule,'state':item['State'],'schedule':item['ScheduleExpression']}
        proof={'contract':'ciss-sources-verification.v1','commit':expected,'generated_at':datetime.now(timezone.utc).isoformat(),
            'runtimes':runtimes,'warehouse_replay':packet['replay'],'commentary_replay':comment['replay'],
            'coverage':packet['coverage'],'reconciliation':packet['headline_reconciliation'],'samples':samples,'schedules':schedules,
            'original_ecb_csvs_replayed':True,'historical_point_in_time_verified':False,
            'scope':'Canonical CISS/CLIFS warehouse and deterministic commentary; downstream stress composites remain separate migrations',
            'invoked':invoked,'paid_ai_calls':0,'notifications_sent':0,'account_reads':0,'portfolio_writes':0}
        s3.put_object(Bucket=bucket,Key='data/ciss-sources-verification.json',Body=json.dumps(proof).encode(),ContentType='application/json',CacheControl='no-cache')
        r.kv(**proof);r.ok('Exact runtimes, original ECB source replay, missing observations and commentary binding verified')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
