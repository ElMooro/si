"""Complete acceptance of the exact snapshot diagnosed by 6106.

6104 reached public delivery after successful source replay/arithmetic but did
not retain the failing path. 6106 separately checked every path, preserving its
origin/public hashes. This new request binds that proof to another complete
source reconstruction and records stage results. No provider/engine invocation.
"""
from pathlib import Path
import json,subprocess,sys,time,urllib.request
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import financial_statement_campaign as capture
import statement_research_source as source
import statement_research_store_v2 as store
import statement_research_arithmetic_v2 as arithmetic
import statement_producer as producer

REQUEST='chatgpt-accounting-scheduled-snapshot-acceptance-6107'
STATUS=capture.request_key(REQUEST,'acceptance')
DIAGNOSTIC={'key':capture.PRIVATE+'87fa5b5ab4620101a936c1bc26b3b925c2b31b978c357cb8dd0f0bcdaa34ddb6.bin',
    'sha256':'87fa5b5ab4620101a936c1bc26b3b925c2b31b978c357cb8dd0f0bcdaa34ddb6','bytes':347052}
REFERENCE={'manifest_key':'data/statement-research/runs/d45b617f4df8e2d2eab788dd97f89bf29cac7838684827efce521fdf1e97376a.json',
    'output_sha256':'37612b97e1abaebe97d9fbef9466c569ef67267bab9e4cce9e345fbd44f458e9'}
NATIVE_COMMIT='f1872d3f1801a88f8515702d73b0b63f4a7a494c'


def verify_arithmetic(inputs,compiled,read):
    manifest=source.strict(source.original(inputs['source_manifest'],read))
    refs=list(manifest['captures'].values());read.prefetch([ref['key'] for ref in refs])
    captures=[source.strict(source.original(ref,read)) for ref in refs]
    read.prefetch([value['original']['key'] for value in captures])
    return arithmetic.verify(inputs['source_manifest'],inputs['identity_capture'],compiled,read)


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_statement_scheduled_acceptance.py')],cwd=ROOT,check=True)
    client=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=16,retries={'max_attempts':2}))
    bucket=capture.BUCKET
    with report('ops_6107_accounting_scheduled_snapshot_acceptance') as r:
        assert '**Status:** success' in (ROOT/'aws/ops/reports/latest/ops_6106_accounting_public_artifact_diagnosis.md').read_text(encoding='utf-8')
        diagnostic=source.strict(capture.read(client,DIAGNOSTIC))
        assert diagnostic['status']=='complete' and diagnostic['replay']==REFERENCE
        assert diagnostic['public_original_bytes_match'] is True and not diagnostic['mismatches'] and diagnostic['checked']==511
        original=source.strict(capture.read(client,diagnostic['whole_source_journal']))
        ready_raw=capture.read(client,diagnostic['whole_ready_document']);ready=source.strict(ready_raw)
        assert original['status']=='complete' and original['request_id']=='scheduled-accounting-source:36157251200'
        assert original['result']['ready_advanced'] is True and original['result']['replay']==ready['replay']==REFERENCE
        assert ready['fmp_requests']==3000 and ready['sec_identity_requests']==1
        for key in ('producer_invocations','consumer_invocations','private_account_reads','signal_writes','notifications_sent','paid_ai_calls'):assert ready[key]==0
        progress={'request_id':REQUEST,'status':'claimed','diagnostic':DIAGNOSTIC,'replay':REFERENCE,'stages':{}}
        capture.journal(client,STATUS,progress,True)
        try:
            r.kv(diagnostic=DIAGNOSTIC,replay=REFERENCE,whole_source_journal=diagnostic['whole_source_journal'],whole_ready_document=diagnostic['whole_ready_document'])
            started=time.monotonic();read=store.reader(client,bucket);compiled=store.replay(REFERENCE,read)
            packet=compiled['packet'];run=store.verified_run(REFERENCE,read);inputs=store.checked(run['input'],'inputs',read)
            assert inputs['source_manifest']==original['source_manifest'] and inputs['identity_capture']==original['identity_capture']
            progress['stages']['source_replay_seconds']=round(time.monotonic()-started,3)
            capture.journal(client,STATUS,{**progress,'status':'originals_replayed'});r.kv(source_replay_seconds=progress['stages']['source_replay_seconds'])
            # Issuer-shard verification can evict source bodies from the frozen
            # 96MB LRU. Reload originals concurrently before the separate oracle;
            # all hashes/formulas remain checked by their unchanged validators.
            started=time.monotonic();proof=verify_arithmetic(inputs,compiled,read)
            assert proof==ready['qualification'] and packet['reported_names']==500 and packet['provider_responses']==3000
            assert producer.current_matches({**packet,'replay':REFERENCE},REFERENCE)
            progress['stages']['independent_check_seconds']=round(time.monotonic()-started,3)
            capture.journal(client,STATUS,{**progress,'status':'arithmetic_verified','qualification':proof})
            public={REFERENCE['manifest_key'],run['input']['key'],run['output']['key'],packet['identity_index']['original']['key'],
                *(row['record']['key'] for row in packet['issuers']),*(row['key'] for row in run['compilers'].values())}
            rows=diagnostic['results'];assert len(rows)==511 and {row['key'] for row in rows}==public
            for row in rows:
                assert row['matched'] is True and row['status']==200 and row['origin_bytes']==row['public_bytes']
                assert row['origin_sha256']==row['public_sha256']==row['key'].rsplit('/',1)[1].split('.')[0]
            protected={STATUS,DIAGNOSTIC['key'],diagnostic['whole_source_journal']['key'],diagnostic['whole_ready_document']['key'],
                inputs['source_manifest']['key'],inputs['identity_capture']['key'],producer.READY}
            for key in protected:
                assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+bucket+'.s3.amazonaws.com/'+key)
            native=boto3.client('lambda',region_name='us-east-1').get_function_configuration(FunctionName='justhodl-forensic-screen')
            receipt=source.strict(store.bounded(client.get_object(Bucket=bucket,Key='data/ops/releases/justhodl-forensic-screen.json')['Body']))
            assert receipt['commit']==NATIVE_COMMIT and receipt['code_sha256']==native['CodeSha256']
            current_raw=store.bounded(client.get_object(Bucket=bucket,Key=producer.CURRENT)['Body']);current=source.strict(current_raw)
            with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+producer.CURRENT,headers={'Cache-Control':'no-cache'}),timeout=30) as response:
                assert store.bounded(response)==current_raw,'Current public accounting packet differs from origin'
            result={'source_workflow_run':36157251200,'replay':REFERENCE,'qualification':proof,'stages':progress['stages'],
                'reported_names':packet['reported_names'],'provider_responses':packet['provider_responses'],'provider_rows':packet['provider_rows'],
                'ready_generated_at':ready['generated_at'],'current_ready_matches_snapshot':producer.raw(client,bucket,producer.READY)==ready_raw,
                'native_current_generated_at':current.get('generated_at'),'native_current_matches_ready':current.get('replay')==REFERENCE,
                'public_artifacts_checked':len(public),'public_artifacts_delivery_proof':DIAGNOSTIC,
                'private_artifacts_checked':len(protected),'complete_original_replay_verified':True,
                'native_commit':receipt['commit'],'native_code_sha256':native['CodeSha256'],
                'provider_requests':0,'producer_invocations':0,'consumer_invocations':0,'private_account_reads':0,
                'current_head_writes':0,'paid_ai_calls':0,'notifications_sent':0,'signal_writes':0,'schedules_changed':0,
                'prior_public_mismatch_cause_identified':False,'forecast_qualified':False,'sizing_qualified':False}
            capture.journal(client,STATUS,{**progress,'status':'complete','result':result});r.kv(**result)
        except Exception as exc:
            capture.journal(client,STATUS,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
