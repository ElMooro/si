"""Verify the first scheduled accounting-source refresh without publication.

Re-read exact retained originals, replay frozen compiler/issuer output and run
the independent arithmetic checker. Report native current versus private ready
separately. No provider requests, native/consumer invocations or account reads.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json,sys,urllib.request
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import financial_statement_campaign as acquisition
import statement_research_source as source
import statement_research_store_v2 as store
import statement_research_arithmetic_v2 as arithmetic
import statement_producer as producer

RUN=36157251200
REQUEST='scheduled-accounting-source:'+str(RUN)
STATUS=acquisition.request_key(REQUEST,'refresh')
NATIVE_COMMIT='f1872d3f1801a88f8515702d73b0b63f4a7a494c'


def main():
    client=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=16,retries={'max_attempts':2}))
    bucket=acquisition.BUCKET
    with report('ops_6104_scheduled_accounting_source_acceptance') as r:
        raw=store.bounded(client.get_object(Bucket=bucket,Key=STATUS)['Body'])
        state=source.strict(raw);whole=acquisition.retain(client,raw)
        assert state['request_id']==REQUEST and state['status']=='complete'
        assert state['result']['ready_advanced'] is True and state['result']['reason']=='qualified'
        ready_raw=store.bounded(client.get_object(Bucket=bucket,Key=producer.READY)['Body'])
        ready=source.strict(ready_raw);whole_ready=acquisition.retain(client,ready_raw)
        assert ready['request_id']==REQUEST and ready['contract']=='financial-statement-qualified-ready.v2'
        assert ready['replay']==state['result']['replay'] and ready['source_manifest_sha256']==state['source_manifest']['sha256']
        assert ready['fmp_requests']==3000 and ready['sec_identity_requests']==1
        for key in ('producer_invocations','consumer_invocations','private_account_reads','signal_writes','notifications_sent','paid_ai_calls'):
            assert ready[key]==0
        read=store.reader(client,bucket);reference=ready['replay']
        compiled=store.replay(reference,read);packet=compiled['packet'];run=store.verified_run(reference,read)
        inputs=store.checked(run['input'],'inputs',read)
        proof=arithmetic.verify(inputs['source_manifest'],inputs['identity_capture'],compiled,read)
        assert proof==ready['qualification']
        assert packet['reported_names']==500 and packet['provider_responses']==3000
        public={reference['manifest_key'],run['input']['key'],run['output']['key'],packet['identity_index']['original']['key'],
            *(row['record']['key'] for row in packet['issuers']),*(row['key'] for row in run['compilers'].values())}
        def verify_public(key):
            request=urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'JustHodl-research-acceptance/1.0','Cache-Control':'no-cache'})
            with urllib.request.urlopen(request,timeout=30) as response:
                assert store.bounded(response)==store.bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(verify_public,sorted(public)):pass
        private={STATUS,producer.READY,whole['key'],whole_ready['key'],state['source_manifest']['key'],state['identity_capture']['key']}
        for key in private:
            assert denied_with_retry('https://justhodl.ai/'+key)
            assert denied_with_retry('https://'+bucket+'.s3.amazonaws.com/'+key)
        native=boto3.client('lambda',region_name='us-east-1').get_function_configuration(FunctionName='justhodl-forensic-screen')
        receipt=source.strict(store.bounded(client.get_object(Bucket=bucket,Key='data/ops/releases/justhodl-forensic-screen.json')['Body']))
        assert receipt['commit']==NATIVE_COMMIT and receipt['code_sha256']==native['CodeSha256']
        current=source.strict(store.bounded(client.get_object(Bucket=bucket,Key='data/forensic-screen.json')['Body']))
        r.kv(source_workflow_run=RUN,whole_source_journal=whole,whole_ready_document=whole_ready,replay=reference,
            qualification=proof,reported_names=packet['reported_names'],provider_responses=packet['provider_responses'],
            provider_rows=packet['provider_rows'],ready_generated_at=ready['generated_at'],
            native_current_generated_at=current.get('generated_at'),native_current_matches_ready=current.get('replay')==reference,
            native_commit=receipt['commit'],native_code_sha256=native['CodeSha256'],
            public_artifacts_checked=len(public),private_artifacts_checked=len(private),
            complete_original_replay_verified=True,provider_requests=0,producer_invocations=0,consumer_invocations=0,
            private_account_reads=0,current_head_writes=0,signal_writes=0,paid_ai_calls=0,notifications_sent=0,schedules_changed=0,
            forecast_qualified=False,sizing_qualified=False)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
