"""Accept completed 6107 replay, then verify identified public HTTP delivery.

6110 diagnosed strict JSON timing numbers as Decimal; preserve them as decimal
strings in reports. 6108 diagnosed that the failed final journal omitted its temporary proof field.
The exact frozen 6107 code records arithmetic timing only after proof equality.
6107 replay and independent arithmetic completed before its default Python UA
received an edge 403. Identified research requests return 200. Preserve that
failed journal; do not repeat any source acquisition, replay or invocation.
"""
from pathlib import Path
from decimal import Decimal
import hashlib,json,math,subprocess,sys,urllib.error,urllib.request
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6107_accounting_scheduled_snapshot_acceptance as prior
import financial_statement_campaign as capture
import statement_research_source as source
import statement_research_store_v2 as store
import statement_producer as producer

REQUEST='chatgpt-accounting-control-journal-acceptance-6111'
STATUS=capture.request_key(REQUEST,'acceptance')


def public_request(key):
    if key!=producer.CURRENT:raise ValueError('Exact reviewed public accounting packet required')
    return urllib.request.Request('https://justhodl.ai/'+key,headers={
        'User-Agent':'JustHodl-research-acceptance/1.0','Cache-Control':'no-cache'})



REPLAY_SCRIPT_SHA256='af245b28538b23a7c3217244e5408b3433518b38b432e2c253437a7a3554141d'


def completed_proof(previous,ready,script):
    # In this exact 6107 code, both independent equality assertions precede
    # independent_check_seconds. Its later HTTPError failure rewrites the
    # journal from progress without the temporary qualification property.
    # Reconstruct that previously checked proof from its immutable READY;
    # never pretend the final failed journal directly contains it.
    if hashlib.sha256(script).hexdigest()!=REPLAY_SCRIPT_SHA256:raise ValueError('Reviewed completed-stage code differs')
    if (previous.get('status')!='failed' or previous.get('error_type')!='HTTPError'
            or previous.get('request_id')!=prior.REQUEST or previous.get('diagnostic')!=prior.DIAGNOSTIC
            or previous.get('replay')!=prior.REFERENCE or ready.get('replay')!=prior.REFERENCE):
        raise ValueError('Exact diagnosed completed replay required')
    for name in ('source_replay_seconds','independent_check_seconds'):
        value=previous.get('stages',{}).get(name)
        if type(value) not in (int,float,Decimal) or not math.isfinite(value) or value<=0:raise ValueError('Completed verified stage required')
    proof=ready.get('qualification')
    if not isinstance(proof,dict) or not proof:raise ValueError('Exact retained arithmetic proof required')
    return proof


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_statement_control_journal_acceptance.py')],cwd=ROOT,check=True)
    client=boto3.client('s3',region_name='us-east-1');bucket=capture.BUCKET
    with report('ops_6111_accounting_control_journal_acceptance') as r:
        failed=(ROOT/'aws/ops/reports/latest/ops_6107_accounting_scheduled_snapshot_acceptance.md').read_bytes()
        assert b'**Status:** failure' in failed and b'HTTP Error 403' in failed and b'public' in prior.__doc__.encode()
        raw=store.bounded(client.get_object(Bucket=bucket,Key=prior.STATUS)['Body']);previous=source.strict(raw)
        assert previous['status']=='failed' and previous['error_type']=='HTTPError' and previous['request_id']==prior.REQUEST
        assert previous['replay']==prior.REFERENCE and previous['diagnostic']==prior.DIAGNOSTIC
        assert previous['stages']['source_replay_seconds']>0 and previous['stages']['independent_check_seconds']>0
        progress={'request_id':REQUEST,'status':'claimed','replay':prior.REFERENCE,
            'whole_failed_journal':capture.retain(client,raw),'whole_failed_report':capture.retain(client,failed),
            'original_replay_source_commit':'577be62b6bae7b244c96b614f5d922687f458477','original_replay_script_sha256':REPLAY_SCRIPT_SHA256}
        capture.journal(client,STATUS,progress,True)
        try:
            diagnostic=source.strict(capture.read(client,prior.DIAGNOSTIC))
            ready_raw=capture.read(client,diagnostic['whole_ready_document']);ready=source.strict(ready_raw)
            original=source.strict(capture.read(client,diagnostic['whole_source_journal']))
            proof=completed_proof(previous,ready,Path(prior.__file__).read_bytes())
            assert original['status']=='complete'
            assert original['request_id']=='scheduled-accounting-source:36157251200' and original['result']['ready_advanced']
            assert original['result']['replay']==ready['replay']==diagnostic['replay']==prior.REFERENCE
            assert ready['fmp_requests']==3000 and ready['sec_identity_requests']==1
            for field in ('producer_invocations','consumer_invocations','private_account_reads','signal_writes','notifications_sent','paid_ai_calls'):assert ready[field]==0
            read=store.reader(client,bucket);run=store.verified_run(prior.REFERENCE,read)
            packet=store.checked(run['output'],'outputs',read);inputs=store.checked(run['input'],'inputs',read)
            assert inputs['source_manifest']==original['source_manifest'] and inputs['identity_capture']==original['identity_capture']
            assert producer.current_matches({**packet,'replay':prior.REFERENCE},prior.REFERENCE)
            public={prior.REFERENCE['manifest_key'],run['input']['key'],run['output']['key'],packet['identity_index']['original']['key'],
                *(row['record']['key'] for row in packet['issuers']),*(row['key'] for row in run['compilers'].values())}
            rows=diagnostic['results'];assert diagnostic['public_original_bytes_match'] and not diagnostic['mismatches']
            assert len(rows)==len(public)==511 and {row['key'] for row in rows}==public
            for row in rows:
                assert row['matched'] and row['status']==200 and row['origin_bytes']==row['public_bytes']
                assert row['origin_sha256']==row['public_sha256']==row['key'].rsplit('/',1)[1].split('.')[0]
            native=boto3.client('lambda',region_name='us-east-1').get_function_configuration(FunctionName='justhodl-forensic-screen')
            receipt=source.strict(store.bounded(client.get_object(Bucket=bucket,Key='data/ops/releases/justhodl-forensic-screen.json')['Body']))
            assert receipt['commit']==prior.NATIVE_COMMIT and receipt['code_sha256']==native['CodeSha256']
            head=client.get_object(Bucket=bucket,Key=producer.CURRENT);current_raw=store.bounded(head['Body']);current=source.strict(current_raw)
            try:response=urllib.request.urlopen(public_request(producer.CURRENT),timeout=30)
            except urllib.error.HTTPError as exc:response=exc
            status=response.status;headers={k.lower():v for k,v in response.headers.items() if k.lower() in ('content-type','cache-control','server','cf-cache-status','cf-mitigated')}
            public_raw=store.bounded(response);progress['current_delivery']={'status':status,'headers':headers,
                'origin':capture.retain(client,current_raw),'response':capture.retain(client,public_raw)}
            capture.journal(client,STATUS,{**progress,'status':'public_response_retained'})
            assert status==200 and public_raw==current_raw,'Identified public current packet differs from origin'
            assert client.head_object(Bucket=bucket,Key=producer.CURRENT)['ETag']==head['ETag'],'Current head changed during verification'
            protected={STATUS,prior.STATUS,prior.DIAGNOSTIC['key'],producer.READY,inputs['source_manifest']['key'],inputs['identity_capture']['key'],
                diagnostic['whole_source_journal']['key'],diagnostic['whole_ready_document']['key'],
                progress['whole_failed_journal']['key'],progress['whole_failed_report']['key'],
                *(ref['key'] for ref in (progress['current_delivery']['origin'],progress['current_delivery']['response']))}
            for key in protected:
                assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+bucket+'.s3.amazonaws.com/'+key)
            result={'source_workflow_run':36157251200,'replay':prior.REFERENCE,'qualification':proof,
                'stages_seconds':{key:str(value) for key,value in previous['stages'].items()},'completed_replay_journal':progress['whole_failed_journal'],
                'complete_original_replay_verified':True,'replay_repeated':False,'proof_reconstructed_from_completed_stages':True,'public_artifacts_checked':len(public),
                'public_artifacts_delivery_proof':prior.DIAGNOSTIC,'current_delivery':progress['current_delivery'],
                'reported_names':packet['reported_names'],'provider_responses':packet['provider_responses'],'provider_rows':packet['provider_rows'],
                'ready_generated_at':ready['generated_at'],'current_ready_matches_snapshot':producer.raw(client,bucket,producer.READY)==ready_raw,
                'native_current_generated_at':current.get('generated_at'),'native_current_matches_ready':current.get('replay')==prior.REFERENCE,
                'native_commit':receipt['commit'],'native_code_sha256':native['CodeSha256'],'private_artifacts_checked':len(protected),
                'provider_requests':0,'producer_invocations':0,'consumer_invocations':0,'private_account_reads':0,'current_head_writes':0,
                'paid_ai_calls':0,'notifications_sent':0,'signal_writes':0,'schedules_changed':0,'forecast_qualified':False,'sizing_qualified':False}
            capture.journal(client,STATUS,{**progress,'status':'complete','result':result});r.kv(**result)
        except Exception as exc:
            capture.journal(client,STATUS,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
