"""Retain complete buyback predecessors and source population before redesign.

Read only against live packages and reviewed public-source packets. No provider
request, producer/consumer invocation, account read, public write or notification.
Every predecessor is retained privately before its coverage is interpreted.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,json,re,subprocess,sys,urllib.request
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_5998_option_population_retained_acceptance import denied_with_retry
import buyback_source_inventory as inventory

BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/buyback-original-research/'
FUNCTIONS=('justhodl-buyback-engine','justhodl-buyback-scanner')
REQUEST='chatgpt-buyback-original-source-baseline-6105'
sha=lambda raw:hashlib.sha256(raw).hexdigest()
STATUS=PRIVATE+'requests/'+sha(REQUEST.encode())+'.json'
encoded=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
now=lambda:datetime.now(timezone.utc).isoformat()


def read(client,key):
    if key not in (*inventory.INPUTS,STATUS) and not re.fullmatch(re.escape(PRIVATE)+r'[a-f0-9]{64}\.bin',key):
        raise ValueError('Reviewed buyback evidence read required')
    return bounded(client.get_object(Bucket=BUCKET,Key=key)['Body'],32*1024*1024)


def retain(client,body):
    if not isinstance(body,bytes) or not 0<len(body)<=32*1024*1024:raise ValueError('Whole bounded original required')
    ref={'key':PRIVATE+sha(body)+'.bin','sha256':sha(body),'bytes':len(body)}
    try:client.put_object(Bucket=BUCKET,Key=ref['key'],Body=body,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    assert read(client,ref['key'])==body
    return ref


def journal(client,value,claim=False):
    body=encoded(value)
    client.put_object(Bucket=BUCKET,Key=STATUS,Body=body,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    assert read(client,STATUS)==body


def main():
    s3,lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    with report('ops_6105_buyback_original_source_baseline') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_buyback_source_inventory.py')],cwd=ROOT,check=True)
        commit=subprocess.check_output(['git','log','-1','--format=%H','--','aws/ops/staged/ops_6105_buyback_original_source_baseline.py'],cwd=ROOT,text=True).strip()
        progress={'contract':'buyback-source-baseline.v1','request_id':REQUEST,'source_commit':commit,
            'started_at':now(),'status':'claimed','native_predecessors':{},'captures':{},'repo_predecessors':{}}
        journal(s3,progress,True)
        try:
            for function in FUNCTIONS:
                evidence=runtime(lam,s3,events,scheduler,function)
                if function=='justhodl-buyback-engine':assert evidence['receipt']['commit']=='b726f543e69f059f128976b4fba667ecd3061984'
                location=lam.get_function(FunctionName=function)['Code']['Location']
                raw=bounded(urllib.request.urlopen(location,timeout=40))
                assert base64.b64encode(hashlib.sha256(raw).digest()).decode()==evidence['code_sha256']
                progress['native_predecessors'][function]={'runtime':evidence,'whole_zip':retain(s3,raw)}
                journal(s3,progress)
            packets={}
            for key in inventory.INPUTS:
                try:
                    response=s3.get_object(Bucket=BUCKET,Key=key);body=bounded(response['Body'],32*1024*1024)
                    ref=retain(s3,body);packet=json.loads(body)
                    packets[key]=packet
                    progress['captures'][key]={'status':'retained','source_key':key,'original':ref,'etag':response['ETag'],
                        'captured_at':now(),'source_generated_at':packet.get('generated_at') if isinstance(packet,dict) else None}
                except Exception as exc:
                    if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('404','NoSuchKey'):raise
                    packets[key]=None;progress['captures'][key]={'status':'missing','source_key':key,'captured_at':now()}
                journal(s3,progress)
            result=inventory.inventory(packets,progress['started_at'][:10]);inventory_ref=retain(s3,encoded(result))
            paths=subprocess.check_output(['git','grep','-l','-F','-e','data/buyback-engine.json','-e','data/buyback-scanner.json','--',
                'aws/lambdas/*/source/*.py','*.html','*.js','assets/*.js'],cwd=ROOT,text=True).splitlines()
            paths=sorted(set(paths)|{'aws/lambdas/'+fn+'/config.json' for fn in FUNCTIONS}|{'aws/ops/checks/buyback_source_inventory.py'})
            for path in paths:progress['repo_predecessors'][path]=retain(s3,(ROOT/path).read_bytes())
            summary={'current_rows':result['current_row_count'],'authorization_rows':result['authorization_rows'],
                'reported_labels':len(result['complete_reported_labels']),'legacy_request_count':len(result['legacy_requested_labels']),
                'outside_legacy_request':len(result['outside_legacy_request']),'missing_current_rows':len(result['legacy_requests_missing_current_rows']),
                'rows_without_statement_coordinates':sum(not row['source_statement_coordinates_present'] for row in result['current_rows']),
                'exact_sec_archive_url_shapes':sum(row['exact_sec_archive_url_shape'] for row in result['authorizations']),
                'missing_inputs':result['missing_inputs'],'field_population':result['field_population']}
            progress.update(status='retained',inventory=inventory_ref,summary=summary)
            journal(s3,progress);manifest=retain(s3,encoded(progress))
            protected={STATUS,manifest['key'],inventory_ref['key'],*(row['original']['key'] for row in progress['captures'].values() if row.get('original')),
                *(row['whole_zip']['key'] for row in progress['native_predecessors'].values()),*(row['key'] for row in progress['repo_predecessors'].values())}
            for key in sorted(protected):
                assert denied_with_retry('https://justhodl.ai/'+key)
                assert denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
            for function,prior in progress['native_predecessors'].items():assert runtime(lam,s3,events,scheduler,function)==prior['runtime']
            journal(s3,{**progress,'status':'complete','completed_at':now(),'manifest':manifest,'protected_artifacts_checked':len(protected)})
            r.kv(source_commit=commit,manifest=manifest,inventory=inventory_ref,summary=summary,
                native_predecessors=progress['native_predecessors'],captures=progress['captures'],repo_predecessors=progress['repo_predecessors'],
                protected_artifacts_checked=len(protected),all_current_rows_conserved=True,all_reported_authorization_rows_conserved=True,
                provider_requests=0,producer_invocations=0,consumer_invocations=0,private_account_reads=0,public_writes=0,
                signal_writes=0,paid_ai_calls=0,notifications_sent=0,schedules_changed=0,
                unreported_filings_complete=False,original_filing_bytes_verified=False,buyback_execution_qualified=False,
                ownership_dilution_qualified=False,forecast_qualified=False,sizing_qualified=False)
        except Exception as exc:
            journal(s3,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
