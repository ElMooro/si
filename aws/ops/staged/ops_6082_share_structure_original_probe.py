"""Qualify source shapes for four capital-structure examples, never a model.

Twenty-eight complete sources: reuse matching prior annual originals; acquire
remaining reviewed endpoints once, with durable claims and bounded rate.
No native/consumer invocation, account, signal, public or schedule mutation.
"""
from pathlib import Path
from collections import Counter
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6081_share_structure_retained_baseline as baseline
import share_structure_sources as source
import financial_statement_campaign as prior_sources

BUCKET=source.BUCKET
FUNCTION=baseline.FUNCTION
BASELINE={'key':source.PRIVATE+'eea4a1e35f82a6e0c0459dd5862a026d7da71eac0bdf5604f9e6bfcc0f175da9.bin',
    'sha256':'eea4a1e35f82a6e0c0459dd5862a026d7da71eac0bdf5604f9e6bfcc0f175da9','bytes':19222}
ACCOUNTING={'key':prior_sources.PRIVATE+'733ffdf2eefa5aa5072c81782375232c0520819528c217355e288e75c76b563c.bin',
    'sha256':'733ffdf2eefa5aa5072c81782375232c0520819528c217355e288e75c76b563c','bytes':985798}
SYMBOLS=('AAPL','JPM','GCT','BRK-B')
REQUEST='chatgpt-share-structure-originals-6082'
STATUS=source.request_key(REQUEST,'probe')


def main():
    s3,lam=boto3.client('s3',region_name='us-east-1'),boto3.client('lambda',region_name='us-east-1')
    events,scheduler=boto3.client('events',region_name='us-east-1'),boto3.client('scheduler',region_name='us-east-1')
    with report('ops_6082_share_structure_original_probe') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_share_structure_sources.py')],cwd=ROOT,check=True)
        captured=json.loads(source.read(s3,BASELINE));population=json.loads(source.read(s3,captured['inventory']))
        assert captured['status']=='complete' and set(SYMBOLS)<=set(population['candidate_provider_labels'])
        before=runtime(lam,s3,events,scheduler,FUNCTION);assert before==captured['runtime']
        accounting=json.loads(prior_sources.read(s3,ACCOUNTING));assert accounting['status']=='complete'
        cfg=lam.get_function_configuration(FunctionName=FUNCTION)
        env=cfg.get('Environment',{}).get('Variables',{})
        credential=env.get('FMP_API_KEY') or env.get('FMP_KEY')
        if not credential:credential=boto3.client('ssm',region_name='us-east-1').get_parameter(Name='/justhodl/fmp/api-key',WithDecryption=True)['Parameter']['Value']
        assert isinstance(credential,str) and credential
        commit=subprocess.check_output(['git','log','-1','--format=%H','--','aws/ops/staged/ops_6082_share_structure_original_probe.py'],cwd=ROOT,text=True).strip()
        progress={'contract':'capital-structure-original-probe.v1','request_id':REQUEST,'source_commit':commit,
            'generated_at':baseline.now(),'baseline':BASELINE,'prior_accounting_sources':ACCOUNTING,
            'reported_symbols':list(SYMBOLS),'planned_sources':28,'status':'claimed','captures':{}}
        source.journal(s3,STATUS,progress,True)
        try:
            results={};rate=source.Rate();new=0;reused=0
            for spec in source.specifications(SYMBOLS):
                previous=accounting['captures'].get(spec['url'])
                if previous:
                    prior=json.loads(prior_sources.read(s3,previous));body=prior_sources.read(s3,prior['original'])
                    value=source.adopt(s3,REQUEST,spec,SYMBOLS,prior,body,ACCOUNTING,baseline.now);reused+=1
                else:
                    value=source.capture(s3,REQUEST,spec,SYMBOLS,credential,rate,baseline.now);new+=1
                results[spec['url']]=value
                progress['captures'][spec['url']]=value['retained_capture']
                progress.update(provider_requests=new,reused_sources=reused,status='capturing');source.journal(s3,STATUS,progress)
            assert len(results)==28
            for value in results.values():
                capsule=json.loads(source.read(s3,value['retained_capture']))
                assert source.inspect(source.read(s3,capsule['original']),capsule['spec'])==capsule['inventory']
            summary={':'.join([v['spec']['symbol'],v['spec']['endpoint'],v['spec']['period'] or 'snapshot']):
                {'rows':v['inventory']['rows'],'field_counts':v['inventory']['field_counts'],
                 'null_field_counts':v['inventory']['null_field_counts'],'zero_field_counts':v['inventory']['zero_field_counts'],
                 'identity_issues':v['inventory']['identity_issues'],'received_at':v['received_at'],
                 'original':v['original'],'capsule':v['retained_capture']} for v in results.values()}
            summary_ref=source.retain(s3,source.encode(summary))
            counts={'complete_sources':len(results),'provider_requests':new,'reused_sources':reused,
                'provider_rows':sum(v['inventory']['rows'] for v in results.values()),
                'empty_arrays':sum(v['inventory']['rows']==0 for v in results.values()),
                'provider_bytes':sum(v['original']['bytes'] for v in results.values()),
                'source_statuses':dict(Counter(v['inventory']['status'] for v in results.values()))}
            progress.update(status='complete',completed_at=baseline.now(),counts=counts,summary=summary_ref,
                producer_invocations=0,consumer_invocations=0,private_account_reads=0,signal_writes=0,public_writes=0,
                paid_ai_calls=0,notifications_sent=0,schedules_changed=0,population_qualified=False,
                free_float_change_qualified=False,forecast_qualified=False,sizing_qualified=False)
            manifest=source.retain(s3,source.encode(progress))
            protected={STATUS,BASELINE['key'],ACCOUNTING['key'],manifest['key'],summary_ref['key'],
                *(v['original']['key'] for v in results.values()),*(v['retained_capture']['key'] for v in results.values()),
                *(v['request_status_key'] for v in results.values())}
            for key in sorted(protected):
                assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
            assert runtime(lam,s3,events,scheduler,FUNCTION)==before
            source.journal(s3,STATUS,{**progress,'manifest':manifest,'protected_artifacts_checked':len(protected)})
            r.kv(source_commit=commit,manifest=manifest,summary=summary_ref,counts=counts,source_details=summary,
                protected_artifacts_checked=len(protected),all_original_responses_reparsed=True,
                producer_invocations=0,consumer_invocations=0,private_account_reads=0,signal_writes=0,
                public_writes=0,paid_ai_calls=0,notifications_sent=0,schedules_changed=0,
                population_qualified=False,free_float_change_qualified=False,forecast_qualified=False,sizing_qualified=False)
        except Exception as exc:
            source.journal(s3,STATUS,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
