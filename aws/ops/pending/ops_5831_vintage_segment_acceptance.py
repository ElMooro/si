"""Exact runtimes, original archive replay and historical-unit acceptance."""
from datetime import datetime,timezone
import gzip,io,json
from pathlib import Path
import subprocess,sys,time
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'scripts'),str(ROOT/'aws/shared')]
from ops_report import report
from release_runtime_identity import active_alias
from replay_fred_vintage import replay
import fred_vintage_model as model


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-vintage-fred/source/lambda_function.py'],text=True).strip()
    s3=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live'
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    def raw(key):
        body=s3.get_object(Bucket=bucket,Key=key)['Body'].read(64*1024*1024+1)
        assert len(body)<=64*1024*1024,'object bound exceeded'
        if key.endswith('.gz'):
            with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:body=stream.read(64*1024*1024+1)
            assert len(body)<=64*1024*1024,'decompressed bound exceeded'
        return body
    def read(key):return json.loads(raw(key))
    names=('justhodl-vintage-fred','justhodl-ai-chat','justhodl-backtest-engine','justhodl-bond-warroom','justhodl-eurodollar-plumbing',
           'justhodl-euro-fragmentation','justhodl-liquidity-capacity','justhodl-liquidity-credit-engine',
           'justhodl-liquidity-inflection','justhodl-repo','justhodl-risk-gate','justhodl-treasury-rehypo')
    governed={'justhodl-backtest-engine','justhodl-risk-gate'}
    with report('ops_5831_vintage_segment_acceptance') as r:
        runtimes={};promotions={};deadline=time.monotonic()+2700
        while len(runtimes)!=len(names):
            for fn in names:
                if fn in runtimes:continue
                try:receipt=read('data/ops/releases/'+fn+'.json')
                except Exception as exc:
                    if getattr(exc,'response',{}).get('Error',{}).get('Code') in ('NoSuchKey','404'):continue
                    raise
                if receipt.get('commit')!=expected:continue
                conf=lam.get_function_configuration(FunctionName=fn)
                assert conf['CodeSha256']==receipt['code_sha256'],fn+' runtime differs'
                if fn in governed:
                    promotion=active_alias(lam,fn,receipt)
                    if promotion is None:continue
                    promotions[fn]=promotion
                runtimes[fn]={'commit':expected,'code_sha256':conf['CodeSha256']}
            assert time.monotonic()<deadline,'intended runtime/alias timeout'
            if len(runtimes)!=len(names):time.sleep(15)
        r.kv(runtimes=runtimes,active_aliases=promotions)
        def invoke(fn):
            extra={'Qualifier':'live'} if fn in promotions else {}
            response=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}',**extra)
            result=json.loads(response['Payload'].read())
            assert not response.get('FunctionError'),{'function':fn,'result':result}
            if fn in promotions:assert response.get('ExecutedVersion')==promotions[fn]['version'],'wrong executed alias'
            assert result.get('ok') is True or result.get('statusCode')==200,{'function':fn,'result':result}
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256'],'runtime changed during verification'
            return result
        started=datetime.now(timezone.utc).isoformat();result=invoke(names[0])
        index=read('data/vintage/_index.json')
        assert index['generated_at']>started and index['contract']=='fred-vintage-index.v1'
        assert set(index['series'])==set(model.SERIES) and index['n_series']==len(model.SERIES),index['errors']
        assert result.get('collection_id')==index['collection_id'],'wrong current collection'
        accepted={};balances={}
        for sid in model.SERIES:
            entry=index['detail'][sid];packet=read(entry['key']);model.validate_packet(packet)
            assert model.digest(packet)==entry['sha256'] and packet['collection_id']==index['collection_id']
            ref=packet['replay'];manifest=read(ref['manifest_key'])
            assert ref['manifest_key']==model.PREFIX+'runs/'+model.digest(manifest)+'.json','manifest identity differs'
            assert replay(manifest,read=raw)=={k:v for k,v in packet.items() if k!='replay'},sid+' original replay differs'
            assert read('data/vintage/'+sid+'.json')==packet,'current alias differs'
            accepted[sid]={'periods':packet['n_vintages'],'definitions':len(packet['definitions']),
                'coverage':packet['coverage'],'output_sha256':ref['output_sha256'],'manifest_key':ref['manifest_key']}
            if sid in ('WALCL','WTREGEN','RRPONTSYD'):balances[sid]=packet
        history=model.net_liquidity(balances,datetime.now(timezone.utc))
        assert history['status']=='ARCHIVE_RESEARCH' and history['n_points']>=96,history.get('reason')
        assert history['publication_eligible'] is False and history['point_in_time'] is False
        from decimal import Decimal
        for when,legs in history['components'].items():
            assert Decimal(history['series_decimal'][when])==Decimal(legs['WALCL']['value_usd_mn_decimal'])-Decimal(legs['WTREGEN']['value_usd_mn_decimal'])-Decimal(legs['RRPONTSYD']['value_usd_mn_decimal'])
        tga_units={d['units'] for d in balances['WTREGEN']['definitions']}
        assert any('Billions' in unit for unit in tga_units) and any('Millions' in unit for unit in tga_units)
        # Refresh only deterministic public-input desks affected by the compiler
        # closure. Do not invoke historical strategy, private-book or alert paths.
        invoke('justhodl-liquidity-credit-engine');invoke('justhodl-risk-gate')
        for key in ('data/liquidity-credit-engine.json','data/risk-gate.json'):
            doc=read(key);assert doc['generated_at']>started and doc['sizing_eligible'] is False
        proof={'contract':'vintage-archive-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'commit':expected,'runtimes':runtimes,'active_aliases':promotions,'collection_id':index['collection_id'],
            'collection_key':result['collection_key'],'series':accepted,'original_replay_reproduced':True,'segmented_series':['NFCI','STLFSI4'],
            'liquidity_archive_points':history['n_points'],'liquidity_excluded_days':len(history['excluded_days']),
            'tga_historical_units':sorted(tga_units),'publication_eligible':False,'paid_ai_calls':0,'notifications_sent':0,
            'private_account_reads':0,'portfolio_writes':0}
        s3.put_object(Bucket=bucket,Key='data/vintage-archive-verification.json',Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        r.kv(**proof)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Vintage archive acceptance failed; inspect runner report.')
        sys.exit(1)
