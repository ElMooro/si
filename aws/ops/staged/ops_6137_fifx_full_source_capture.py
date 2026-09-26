"""Retain one bounded full-history response per FI/FX source, privately only.

No keys, paid API, native invocation, public publication or schedule changes.
MOVE reuses the complete qualified Bond Vol acquisition; it is not re-requested.
"""
from pathlib import Path
import json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/lambdas/justhodl-bond-vol/source','aws/shared','aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
import ops_6136_fifx_original_baseline as baseline
import fifx_source_capture as capture
import bond_vol_store as bond
import retained_access_evidence as access

BASELINE={'key':baseline.PRIVATE+'8aeb2a72d8ce26ad902440da38aecd4904963863545d8a7f49145e4d3b0fc0e3.bin',
    'sha256':'8aeb2a72d8ce26ad902440da38aecd4904963863545d8a7f49145e4d3b0fc0e3','bytes':31229}
REQUEST='chatgpt-fifx-full-source-capture-6137'
STATUS=baseline.PRIVATE+'requests/'+baseline.sha(REQUEST.encode())+'.json'

def checked(s3,ref):
    if (not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256'))) or ref.get('key')!=baseline.PRIVATE+ref['sha256']+'.bin' or type(ref.get('bytes')) is not int
        or not 0<ref['bytes']<=64*1024*1024):raise ValueError('Whole protected source coordinates required')
    raw=baseline.bounded(s3.get_object(Bucket=baseline.BUCKET,Key=ref['key'])['Body'])
    if baseline.sha(raw)!=ref['sha256'] or len(raw)!=ref['bytes']:raise ValueError('Protected original differs')
    return raw

def journal(s3,progress,claim=False):
    raw=baseline.encoded(progress)
    s3.put_object(Bucket=baseline.BUCKET,Key=STATUS,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    if baseline.bounded(s3.get_object(Bucket=baseline.BUCKET,Key=STATUS)['Body'])!=raw:raise ValueError('Capture journal readback differs')

def capture_one(s3,progress,name,spec):
    progress['sources'][name]={'status':'attempt_recorded','spec':spec};progress['provider_request_attempts']+=1;journal(s3,progress)
    try:raw,receipt=capture.acquire(spec)
    except Exception as exc:
        progress['sources'][name].update(status='unavailable',error_type=type(exc).__name__);journal(s3,progress);return
    original=baseline.retain(s3,raw);receipt_ref=baseline.retain(s3,baseline.encoded(receipt))
    result={'status':'retained_response','spec':spec,'original':original,'receipt':receipt,'whole_receipt':receipt_ref}
    try:
        if receipt['http_status']!=200:raise ValueError('Source did not return success')
        result['inspection']=capture.inspect_csv(raw,name) if spec['provider']=='fred_csv' else capture.inspect_quote(raw,name)
    except Exception as exc:result.update(status='retained_unqualified_response',error_type=type(exc).__name__)
    progress['sources'][name]=result;journal(s3,progress)

def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_fifx_source_capture.py')],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('s3','lambda','events','scheduler'))
    with report('ops_6137_fifx_full_source_capture') as r:
        progress={'contract':'fifx-full-source-capture.v1','status':'claimed','request_id':REQUEST,'started_at':baseline.now(),
            'baseline':BASELINE,'provider_request_attempts':0,'sources':{}}
        journal(s3,progress,True)
        try:
            old=json.loads(checked(s3,BASELINE));before=baseline.runtime(lam,s3,events,scheduler,baseline.FUNCTION)
            if before!=old['native_predecessor']['runtime']:raise ValueError('Native predecessor changed; re-review required')
            arn=lam.get_function_configuration(FunctionName=baseline.FUNCTION)['FunctionArn']
            triggers=baseline.triggers.collect(lam,scheduler,s3,arn,baseline.BUCKET)
            if triggers!=old['trigger_inventory']:raise ValueError('Native trigger bindings changed')
            # Bind the existing complete MOVE original to its immutable run and view.
            read=bond.reader(s3,baseline.BUCKET);raw=read(bond.model.CURRENT);packet=json.loads(raw);run=bond.binding(packet,read)
            inputs=bond.checked(run['input'],'inputs',read)
            progress['bond_vol_source']={'current':baseline.retain(s3,raw),'run':baseline.retain(s3,read(packet['replay']['manifest_key'])),
                'input':baseline.retain(s3,read(run['input']['key'])),'view':baseline.retain(s3,read(run['view']['key']))}
            if inputs['quote']:
                raw=bond.checked(inputs['quote'],'originals',read,'bin');receipt=bond.checked(inputs['quote_receipt'],'receipts',read)
                if receipt['sha256']!=baseline.sha(raw) or receipt['bytes']!=len(raw) or receipt['source_url']!=bond.catalog.MOVE_URL:raise ValueError('Existing MOVE receipt differs')
                inspection=capture.inspect_quote(raw,'^MOVE')
                progress['sources']['^MOVE']={'status':'retained_existing_native_acquisition','original':baseline.retain(s3,raw),
                    'receipt':receipt,'whole_receipt':baseline.retain(s3,baseline.encoded(receipt)),'inspection':inspection,
                    'native_identity_status':packet['move']['status'],'additional_provider_requests':0}
            else:progress['sources']['^MOVE']={'status':'existing_native_source_unavailable','additional_provider_requests':0}
            plan=capture.request_plan(progress['started_at']);progress['plan']=plan;journal(s3,progress)
            for name,spec in plan.items():capture_one(s3,progress,name,spec)
            if baseline.runtime(lam,s3,events,scheduler,baseline.FUNCTION)!=before:raise ValueError('Native runtime changed during preservation')
            if baseline.triggers.collect(lam,scheduler,s3,arn,baseline.BUCKET)!=triggers:raise ValueError('Trigger bindings changed during preservation')
            progress.update(status='captured',completed_at=baseline.now());journal(s3,progress);manifest=baseline.retain(s3,baseline.encoded(progress))
            protected={STATUS,manifest['key'],*(ref['key'] for ref in progress['bond_vol_source'].values())}
            for row in progress['sources'].values():
                for key in ('original','whole_receipt'):
                    if key in row:protected.add(row[key]['key'])
            outcomes=[access.check(key) for key in sorted(protected)];evidence=baseline.retain(s3,baseline.encoded(outcomes));outcomes.append(access.check(evidence['key']))
            privacy=access.summarize(outcomes)
            if not privacy['all_denied']:raise ValueError('Private originals exposed anonymously')
            journal(s3,{**progress,'status':'complete','manifest':manifest,'privacy':privacy})
            r.kv(manifest=manifest,sources=progress['sources'],access_evidence=evidence,**privacy,
                provider_request_attempts=progress['provider_request_attempts'],requested_sources=len(progress['sources']),
                native_package_unchanged=True,producer_invocations=0,consumer_invocations=0,paid_api_calls=0,
                public_writes=0,schedules_changed=0,notifications_sent=0,forecast_qualified=False,arithmetic_qualified=False)
        except Exception as exc:
            journal(s3,{**progress,'status':'failed','error_type':type(exc).__name__});raise

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
