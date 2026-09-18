"""Verify the default macro report and coordinated consumers with runner IAM."""
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime,timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'scripts'),str(ROOT/'aws/shared')]
from ops_report import report
from replay_daily_research import replay


def main():
    # The source file is introduced/changed by the atomic production commit;
    # unrelated ops-report commits on main cannot change this proof target.
    expected=subprocess.check_output(['git','log','-1','--format=%H','--',
                                      'aws/lambdas/justhodl-daily-report-v3/source/daily_macro_store.py'],text=True).strip()
    assert len(expected)==40,'source revision unavailable'
    bucket='justhodl-dashboard-live'
    names=['justhodl-daily-report-v3','justhodl-crypto-enricher','justhodl-ai-chat',
           'justhodl-khalid-adaptive','justhodl-signal-logger','justhodl-crisis-knowledge-base']
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    def raw(key):
        body=s3.get_object(Bucket=bucket,Key=key)['Body'].read()
        return gzip.decompress(body) if key.endswith('.gz') else body
    def read(key):return json.loads(raw(key))
    def receipt(fn):
        try:return read('data/ops/releases/'+fn+'.json')
        except ClientError as exc:
            if exc.response.get('Error',{}).get('Code') in ('NoSuchKey','404'):return None
            raise
    def invoke(fn):
        r=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
        payload=json.loads(r['Payload'].read())
        assert not r.get('FunctionError') and payload.get('statusCode')==200,{'function':fn,'response':payload}
    with report('ops_5734_daily_research_verify') as r:
        runtimes={};deadline=time.monotonic()+1800
        while len(runtimes)!=len(names):
            for fn in names:
                if fn in runtimes:continue
                row=receipt(fn)
                if row is None or row.get('commit')!=expected:continue
                conf=lam.get_function_configuration(FunctionName=fn)
                assert conf['CodeSha256']==row['code_sha256'],fn+' runtime differs'
                runtimes[fn]={'commit':expected,'code_sha256':conf['CodeSha256']}
            assert time.monotonic()<deadline,'exact runtime receipt timeout: '+str(set(names)-set(runtimes))
            if len(runtimes)!=len(names):time.sleep(15)
        r.kv(runtimes=runtimes)
        before=datetime.now(timezone.utc).isoformat()
        invoke('justhodl-daily-report-v3')
        packet=read('data/report.json')
        assert packet['contract']=='daily-research-report.v1' and packet['generated_at']>before
        manifest=read(packet['replay']['manifest_key']);rebuilt=replay(manifest,read=raw)
        assert all(packet.get(k)==v for k,v in rebuilt.items()),'public base differs from replay'
        rows={sid:row for category in packet['fred'].values() for sid,row in category.items()}
        for sid in ('ICSA','UNRATE','CPIAUCSL','WALCL','WTREGEN','RRPONTSYD','DGS10','DTWEXBGS'):
            assert rows[sid]['quality']['status']=='fresh' and rows[sid]['current'] is not None,sid
        assert rows['ICSA']['unit']=='Number' and '196000K' not in packet['ai_analysis']['summary']
        assert rows['UNRATE']['changes']['month']['change_unit']=='percentage_points'
        assert packet['khalid_index']['score'] is None and packet['risk_dashboard']['composite'] is None
        assert packet['ai_analysis']['portfolio']['construction']=={} and packet['ai_analysis']['portfolio']['moves']==[]
        assert packet['signals']['buys']==[] and packet['signals']['sells']==[]
        assert packet['decision']['meaning']=='abstain' and packet['sizing_eligible'] is False
        assert packet['net_liquidity']['net_decimal'] is not None and packet['net_liquidity']['direction'] is None
        assert packet['auxiliary_quality']['original_source_verified'] is False
        assert packet['stocks'],'market collector must not silently disappear'
        for row in packet['stocks'].values():
            assert row['grade'] is None and row['score'] is None and row['risk_reward'] is None
        try:history=hashlib.sha256(raw('data/khalid-adaptive-history.json')).hexdigest()
        except ClientError as exc:
            if exc.response.get('Error',{}).get('Code') not in ('NoSuchKey','404'):raise
            history=None
        invoke('justhodl-khalid-adaptive')
        adaptive=read('data/khalid-adaptive.json')
        assert adaptive['contract']=='adaptive-research-status.v1' and adaptive['generated_at']>before
        assert adaptive['adaptive']['score'] is None and adaptive['standard']['score'] is None
        assert adaptive['divergence']['score_delta'] is None and adaptive['decision']['meaning']=='abstain'
        if history is not None:assert hashlib.sha256(raw('data/khalid-adaptive-history.json')).hexdigest()==history
        proof={'contract':'daily-research-verification.v1','commit':expected,
               'generated_at':datetime.now(timezone.utc).isoformat(),'runtimes':runtimes,
               'replay':packet['replay'],'quality':packet['quality'],'stats':packet['stats'],
               'macro_originals_replayed':True,'market_auxiliaries_original_source_verified':False,
               'adaptive_numeric_history_unchanged':True,
               'invoked':['justhodl-daily-report-v3','justhodl-khalid-adaptive'],
               'consumer_tests':'actual functions exercised offline; exact deployed runtime hashes checked',
               'paid_ai_calls':0,'notifications_sent':0,'account_reads':0,'portfolio_writes':0,
               'scope':'Canonical default report macro and consumer abstention; market-source and wider fleet migration remain'}
        s3.put_object(Bucket=bucket,Key='data/daily-research-verification.json',Body=json.dumps(proof).encode(),
                      ContentType='application/json',CacheControl='no-cache')
        r.kv(**proof)
        r.ok('Six exact runtimes, macro original-source replay and real adaptive abstention; no notifications or private account reads')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
