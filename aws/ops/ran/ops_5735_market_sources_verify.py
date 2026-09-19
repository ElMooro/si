"""Verify market-source release, actual provider bytes and deterministic output."""
from datetime import datetime, timezone
import gzip
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
from replay_daily_research import replay


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--',
        'aws/lambdas/justhodl-daily-report-v3/source/daily_market_store.py'],text=True).strip()
    assert len(expected)==40,'source revision missing'
    bucket='justhodl-dashboard-live'
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
    with report('ops_5735_market_sources_verify') as r:
        names=['justhodl-daily-report-v3','justhodl-crypto-enricher']
        runtimes={};deadline=time.monotonic()+1800
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
        before=datetime.now(timezone.utc).isoformat()
        response=lam.invoke(FunctionName=names[0],InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
        payload=json.loads(response['Payload'].read())
        assert not response.get('FunctionError') and payload.get('statusCode')==200,payload
        packet=read('data/report.json')
        assert packet['generated_at']>before and packet['contract']=='daily-research-report.v1'
        manifest=read(packet['replay']['manifest_key'])
        reconstructed=replay(manifest,read=raw)
        assert all(packet.get(k)==v for k,v in reconstructed.items()),'current report differs from original-source replay'
        quality=packet['market_measurement_quality']
        assert quality['equities_compiled']>=150,'market universe lost during source migration'
        samples={}
        for symbol in ('SPY','QQQ','TLT','GLD','UUP'):
            row=packet['stocks'][symbol]
            assert row['quality']['status']=='fresh' and row['quality']['original_source_verified'] is True,symbol
            assert row['price'] is not None and row['score'] is None and row['risk_reward'] is None
            assert row['w52_high'] is None and not row['sizing_eligible']
            samples[symbol]={key:row[key] for key in ('price','date','period_start','observed_at','acquired_at','evidence','changes')}
        bitcoin=packet['crypto_by_id']['bitcoin']
        assert bitcoin['quality']['status']=='fresh' and bitcoin['price'] is not None
        assert bitcoin['provider_reported_changes']['24h']['baseline_verified'] is False
        assert packet['crypto']['BTC']['provider_id']=='bitcoin'
        assert packet['decision']['meaning']=='abstain' and packet['sizing_eligible'] is False
        assert packet['auxiliary_quality']['original_source_verified'] is False,'unmigrated auxiliaries must remain explicit'
        proof={'contract':'market-sources-verification.v1','commit':expected,
            'generated_at':datetime.now(timezone.utc).isoformat(),'runtimes':runtimes,
            'replay':packet['replay'],'market_quality':quality,'samples':samples,
            'bitcoin':{k:bitcoin[k] for k in ('price','provider_id','observed_at','acquired_at','evidence')},
            'original_macro_and_market_responses_replayed':True,'historical_point_in_time_verified':False,
            'scope':'Canonical report equity/crypto measurements; ECB/news and wider market engines remain separate migrations',
            'invoked':[names[0]],'paid_ai_calls':0,'notifications_sent':0,'account_reads':0,'portfolio_writes':0}
        s3.put_object(Bucket=bucket,Key='data/market-sources-verification.json',Body=json.dumps(proof).encode(),
            ContentType='application/json',CacheControl='no-cache')
        r.kv(**proof)
        r.ok('Original market responses, exact runtimes and deterministic macro/market replay verified')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
