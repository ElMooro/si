# Superseded by ops 5738: Stage 19 preflight failed before AWS changed; waiting run 35412150181 cancelled.
"""Verify exact CISS source and consumer runtimes with actual observation lineage."""
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


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/ciss_vintage.py'],text=True).strip()
    assert len(expected)==40,'source revision missing'
    bucket='justhodl-dashboard-live';s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
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
    names=['justhodl-ciss-stress','justhodl-ciss-ai','justhodl-sovereign-stress','justhodl-euro-fragmentation']
    with report('ops_5737_ciss_consumers_verify') as r:
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
        started=datetime.now(timezone.utc).isoformat()
        for fn in names:
            response=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
            out=json.loads(response['Payload'].read())
            assert not response.get('FunctionError') and out.get('statusCode')==200,{'function':fn,'result':out}
        source=read('data/ciss-stress.json');comment=read('data/ciss-ai.json')
        assert source['generated_at']>started and source['headline_reconciliation']['status']=='matched'
        assert source['n_series']==source['discovered_series']>=80 and not source['errors']
        assert replay(read(source['replay']['manifest_key']),read=raw)=={k:v for k,v in source.items() if k!='replay'}
        assert replay_commentary(read(comment['replay']['manifest_key']),read=raw)=={k:v for k,v in comment.items() if k!='replay'}
        by_key={row['key']:row for row in source['series']}
        sovereign=read('data/sovereign-stress.json');fragment=read('data/euro-fragmentation.json')
        proof_rows={};counts={}
        for name,packet in [('sovereign',sovereign),('fragmentation',fragment)]:
            assert packet['generated_at']>started and packet['ciss_warehouse']['replay']==source['replay']
            assert packet['call'] is None and packet['calls_eligible'] is False and packet['sizing_eligible'] is False
            assert packet['decision']['meaning']=='abstain' and packet['quality']['publication_date'] is None
            rows=list(packet['systemic_stress_ciss'].values())+list(packet['sovereign_stress_sovciss'].values()) if name=='sovereign' else list(packet['countries'].values())
            matched=0
            for row in rows:
                key=row.get('source_key') if name=='sovereign' else row.get('sovciss_source_key')
                q=row.get('quality') if name=='sovereign' else row.get('sovciss_quality')
                if not key:continue
                original=by_key[key]
                assert q['source_replay']==source['replay']
                if q['status']!='fresh':continue
                value=row['level'] if name=='sovereign' else row['sovciss']
                assert value==original['latest'],key+' value differs'
                evidence=row['source_evidence'] if name=='sovereign' else row['sovciss_evidence']
                assert evidence==original['evidence'],key+' evidence differs'
                comparison=row['comparisons']['1m'] if name=='sovereign' else row['sovciss_comparisons']['1m']
                assert comparison['value']==original['comparisons']['1m']['value']
                assert comparison['baseline_date']==original['comparisons']['1m']['baseline_period']
                matched+=1
                if original['area'] in ('U2','FR'):proof_rows[name+':'+key]={'value':value,'quality':q,'comparison':comparison}
            assert matched>=8,name+' lost qualified ECB coverage'
            counts[name]=matched
        assert sovereign['signal_emission']['enabled'] is False and sovereign['signals_fired']==[]
        pd=sovereign['pd_settlement_fails']
        assert pd.get('calls_eligible') is False,'settlement context authority changed'
        proof={'contract':'ciss-consumers-verification.v1','commit':expected,'generated_at':datetime.now(timezone.utc).isoformat(),
            'runtimes':runtimes,'warehouse_replay':source['replay'],'commentary_replay':comment['replay'],
            'qualified_consumer_rows':counts,'samples':proof_rows,'source_and_consumer_values_equal':True,
            'scope':'CISS lineage, calendar baselines and forecast permissions; other desk inputs and GSSI source qualification remain open',
            'invoked':names,'paid_ai_calls':0,'notifications_sent':0,'account_reads':0,'portfolio_writes':0}
        s3.put_object(Bucket=bucket,Key='data/ciss-consumers-verification.json',Body=json.dumps(proof).encode(),ContentType='application/json',CacheControl='no-cache')
        r.kv(**proof);r.ok('Four exact runtimes, original-source replay, consumer lineage and denied unvalidated signal emission verified')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
