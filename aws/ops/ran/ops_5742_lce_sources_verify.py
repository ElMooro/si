"""Collect all requested LCE identities and verify its original-source replay."""
from datetime import datetime,timezone
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
from replay_lce_research import replay
from lce_research_model import digest,SERIES


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/lce_research_model.py'],text=True).strip()
    assert len(expected)==40,'source revision missing'
    bucket='justhodl-dashboard-live';s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    events=boto3.client('events',region_name='us-east-1')
    scheduler=boto3.client('scheduler',region_name='us-east-1')
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
    names=('justhodl-daily-report-v3','justhodl-liquidity-credit-engine','justhodl-ai-chat','justhodl-allocator')
    with report('ops_5742_lce_sources_verify') as r:
        runtimes={};configs={};deadline=time.monotonic()+1800
        while len(runtimes)!=len(names):
            for fn in names:
                if fn in runtimes:continue
                row=receipt(fn)
                if row is None or row.get('commit')!=expected:continue
                conf=lam.get_function_configuration(FunctionName=fn)
                assert conf['CodeSha256']==row['code_sha256'],fn+' runtime differs'
                configs[fn]=conf;runtimes[fn]={'commit':expected,'code_sha256':conf['CodeSha256']}
            assert time.monotonic()<deadline,'exact runtime receipt timeout'
            if len(runtimes)!=len(names):time.sleep(15)
        r.kv(runtimes=runtimes)
        schedules={}
        for fn in names[:2]:
            conf=configs[fn]
            rules=events.list_rule_names_by_target(TargetArn=conf['FunctionArn'])['RuleNames'];enabled=[]
            for name in rules:
                rule=events.describe_rule(Name=name)
                if rule.get('State')=='ENABLED' and rule.get('ScheduleExpression'):
                    targets=events.list_targets_by_rule(Rule=name)['Targets']
                    if any(t.get('Arn')==conf['FunctionArn'] for t in targets):
                        enabled.append({'name':name,'schedule':rule['ScheduleExpression']})
            schedules[fn]=enabled
        source_schedule=scheduler.get_schedule(Name='justhodl-report-research-hourly')
        assert source_schedule['State']=='ENABLED' and source_schedule['Target']['Arn']==configs[names[0]]['FunctionArn']
        assert json.loads(source_schedule['Target']['Input'])=={'action':'research_measurements'}
        schedules[names[0]].append({'name':source_schedule['Name'],'schedule':source_schedule['ScheduleExpression'],'backend':'Scheduler','action':'research_measurements'})
        if not schedules[names[1]]:
            for page in scheduler.get_paginator('list_schedules').paginate():
                for item in page.get('Schedules',[]):
                    if item.get('State')=='ENABLED' and item.get('Target',{}).get('Arn')==configs[names[1]]['FunctionArn']:
                        rule=scheduler.get_schedule(Name=item['Name'],GroupName=item.get('GroupName','default'))
                        schedules[names[1]].append({'name':rule['Name'],'schedule':rule['ScheduleExpression'],'backend':'Scheduler'})
        def invoke(fn,event):
            response=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=json.dumps(event).encode())
            result=json.loads(response['Payload'].read())
            assert not response.get('FunctionError') and result.get('statusCode') in (200,409),{'function':fn,'result':result}
            return result
        started=datetime.now(timezone.utc).isoformat()
        invoke(names[0],{'action':'research_measurements','suppress_alerts':True})
        source=read('data/report-measurements.json')
        assert set(SERIES)<=set(source['catalog']),'requested source identities missing from collector'
        assert source['generated_at']>started
        assert schedules[names[0]] and schedules[names[1]],'existing source and consumer schedules must remain enabled'
        invoke(names[1],{'suppress_alerts':True})
        packet=read('data/liquidity-credit-engine.json')
        assert packet['generated_at']>started and packet['contract']=='liquidity-credit-research.v1'
        ref=packet['replay'];manifest=read(ref['manifest_key'])
        assert ref['manifest_key']=='data/lce-research/runs/'+digest(manifest)+'.json'
        cache={}
        def retained(key):
            if key not in cache:cache[key]=raw(key)
            return cache[key]
        rebuilt=replay(manifest,read=retained)
        assert rebuilt=={k:v for k,v in packet.items() if k!='replay'}
        assert set(packet['series'])==set(SERIES) and len(SERIES)==51
        assert packet['quality']['fresh_series']>=20,'unexpected broad collection loss'
        assert packet['composite']['score'] is None and packet['regime']=='UNAVAILABLE'
        assert packet['calls_eligible'] is False and packet['sizing_eligible'] is False and packet['call'] is None
        assert packet['interpretation']['target_allocation']==[] and packet['interpretation']['cross_asset']=={}
        samples={}
        for sid in ('RRPONTSYD','TOTRESNS','WALCL','DRTSCILM','DPCREDIT'):
            row=packet['series'][sid]
            samples[sid]={k:row[k] for k in ('available','latest_date','latest_value_decimal','_units','frequency','error')}
            if row['available']:assert row['latest_value']==row['latest_value_raw']
            if row['frequency']=='M':assert row['wow_pct'] is None
            if row['frequency']=='Q':assert row['wow_pct'] is None and row['mom_pct'] is None
        proof={'contract':'liquidity-credit-sources-verification.v1','commit':expected,
            'generated_at':datetime.now(timezone.utc).isoformat(),'runtimes':runtimes,'schedules':schedules,
            'source_quality':source['quality'],'desk_quality':packet['quality'],'samples':samples,
            'source_gaps':{sid:row['error'] or row['quality']['status'] for sid,row in packet['series'].items() if not row['available']},
            'output_generated_at':packet['generated_at'],'replay':ref,'original_replay_reproduced':True,
            'scope':packet['scope'],'paid_ai_calls':0,'notifications_sent':0,'account_reads':0,'portfolio_writes':0}
        s3.put_object(Bucket=bucket,Key='data/liquidity-credit-sources-verification.json',Body=json.dumps(proof).encode(),ContentType='application/json',CacheControl='no-cache')
        r.kv(**proof);r.ok('Four exact runtimes; 51 requested LCE identities, original-source replay, units and enabled schedules verified; chat and allocator not invoked')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
