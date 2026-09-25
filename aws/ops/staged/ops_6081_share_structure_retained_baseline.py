"""Preserve every reviewed share-structure input and the complete population.

Read-only with respect to live engines: no producer/consumer invocation, provider
request, private account, signal, notification, public packet or schedule write.
Whole originals and the durable request are retained in the protected prefix.
"""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json, re, subprocess, sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime, bounded
from ops_5998_option_population_retained_acceptance import denied_with_retry
from ops_6053_short_interest_source_baseline import bindings
import share_structure_inventory as inventory

BUCKET='justhodl-dashboard-live'
FUNCTION='justhodl-share-flows'
PRIVATE='audit-private/20260909-originals/share-structure-research/'
REQUEST='chatgpt-share-structure-baseline-6081'
sha=lambda value:hashlib.sha256(value).hexdigest()
STATUS=PRIVATE+'requests/'+sha(REQUEST.encode())+'.json'
FAILED_STATUS=PRIVATE+'requests/'+sha(b'chatgpt-share-structure-baseline-6080')+'.json'
now=lambda:datetime.now(timezone.utc).isoformat()
encoded=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def read(s3,key):
    if key not in inventory.INPUTS and key not in (STATUS,FAILED_STATUS) and not re.fullmatch(re.escape(PRIVATE)+r'[a-f0-9]{64}\.bin',key):
        raise ValueError('Reviewed share-structure evidence read required')
    return bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'],32*1024*1024)


def protect(s3,body):
    if not isinstance(body,bytes) or not 0<len(body)<=32*1024*1024:raise ValueError('Whole bounded source required')
    ref={'key':PRIVATE+sha(body)+'.bin','sha256':sha(body),'bytes':len(body)}
    try:s3.put_object(Bucket=BUCKET,Key=ref['key'],Body=body,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    assert read(s3,ref['key'])==body
    return ref


def journal(s3,value,claim=False):
    body=encoded(value)
    s3.put_object(Bucket=BUCKET,Key=STATUS,Body=body,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    assert read(s3,STATUS)==body


def main():
    s3,lam=boto3.client('s3',region_name='us-east-1'),boto3.client('lambda',region_name='us-east-1')
    events,scheduler=boto3.client('events',region_name='us-east-1'),boto3.client('scheduler',region_name='us-east-1')
    with report('ops_6081_share_structure_retained_baseline') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_share_structure_inventory.py')],cwd=ROOT,check=True)
        commit=subprocess.check_output(['git','log','-1','--format=%H','--','aws/ops/staged/ops_6081_share_structure_retained_baseline.py'],cwd=ROOT,text=True).strip()
        before=runtime(lam,s3,events,scheduler,FUNCTION)
        schedules=bindings(scheduler,events,lam.get_function_configuration(FunctionName=FUNCTION)['FunctionArn'])
        progress={'contract':'share-structure-baseline.v1','request_id':REQUEST,'source_commit':commit,
            'generated_at':now(),'status':'claimed','captures':{},'repo_predecessors':{}}
        journal(s3,progress,True)
        try:
            failed_raw=read(s3,FAILED_STATUS);failed=json.loads(failed_raw)
            assert failed['status']=='failed' and failed['error_type']=='ValueError'
            assert failed['request_id']=='chatgpt-share-structure-baseline-6080'
            assert failed['source_commit']=='7ea50a01526a403fd136a8c3aa85d7b0ddee2211'
            diagnostic=(ROOT/'aws/ops/reports/latest/ops_6080_share_structure_baseline.md').read_bytes()
            assert b'Explicit valuation universe mapping required' in diagnostic
            assert set(failed['captures'])==set(inventory.INPUTS)
            subprocess.run(['git','diff','--quiet',failed['source_commit'],'HEAD','--',
                'aws/lambdas/'+FUNCTION,'aws/shared/impact_mapper.py'],cwd=ROOT,check=True)
            progress.update(adopted_failed_request=protect(s3,failed_raw),retained_failure_report=protect(s3,diagnostic),
                captured_snapshot_started_at=failed['generated_at'],captures=failed['captures'])
            journal(s3,progress)
            packets={}
            for key,captured in progress['captures'].items():
                if captured['status']=='missing':packets[key]=None;continue
                assert captured['status']=='retained' and captured['source_key']==key
                ref=captured['original'];body=read(s3,ref['key'])
                assert len(body)==ref['bytes'] and sha(body)==ref['sha256']
                packets[key]=json.loads(body)
            native='aws/lambdas/'+FUNCTION+'/source/lambda_function.py'
            result=inventory.inventory(packets,progress['captured_snapshot_started_at'][:10],(ROOT/native).read_text(encoding='utf-8'))
            result_ref=protect(s3,encoded(result))
            # Discover the complete tracked literal consumer set without importing
            # or invoking any Lambda. Each whole predecessor is retained.
            paths=subprocess.check_output(['git','grep','-l','-F','data/share-flows.json','--',
                'aws/lambdas/*/source/*.py','*.html','*.js','assets/*.js'],cwd=ROOT,text=True).splitlines()
            paths=sorted(set(paths)|{native,'aws/lambdas/'+FUNCTION+'/config.json',
                'aws/shared/impact_mapper.py','aws/ops/checks/share_structure_inventory.py'})
            for path in paths:progress['repo_predecessors'][path]=protect(s3,(ROOT/path).read_bytes())
            summary={key:result[key] for key in ('legacy_request_count','complete_reported_label_count',
                'candidate_provider_label_count','current_row_age_days','field_population','legacy_flag_population','missing_inputs')}
            summary.update(current_rows=result['current']['rows'],
                current_rows_older_than_seven_days=len(result['current_rows_older_than_seven_days']),
                invalid_row_dates=len(result['invalid_row_dates']),future_row_dates=len(result['future_row_dates']),
                requested_labels_missing_current_rows=len(result['requested_labels_missing_current_rows']),
                current_labels_not_in_legacy_request=len(result['current_labels_not_in_legacy_request']),
                candidate_labels_absent_from_legacy_request=len(result['candidate_labels_absent_from_legacy_request']),
                shape_issues=len(result['shape_issues']))
            progress.update(inventory=result_ref,summary=summary,runtime=before,bindings=schedules,status='complete',completed_at=now(),
                provider_requests=0,producer_invocations=0,consumer_invocations=0,private_account_reads=0,signal_writes=0,
                paid_ai_calls=0,public_writes=0,notifications_sent=0,schedules_changed=0)
            manifest=protect(s3,encoded(progress))
            assert runtime(lam,s3,events,scheduler,FUNCTION)==before
            after=bindings(scheduler,events,lam.get_function_configuration(FunctionName=FUNCTION)['FunctionArn'])
            for field in ('schedules','classic_default_bus_rules'):assert after[field]==schedules[field]
            protected={STATUS,FAILED_STATUS,progress['adopted_failed_request']['key'],progress['retained_failure_report']['key'],manifest['key'],result_ref['key'],*(v['original']['key'] for v in progress['captures'].values() if v.get('original')),
                *(v['key'] for v in progress['repo_predecessors'].values())}
            for key in sorted(protected):
                assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
            journal(s3,{**progress,'manifest':manifest,'protected_artifacts_checked':len(protected)})
            r.kv(source_commit=commit,manifest=manifest,inventory=result_ref,summary=summary,captures=progress['captures'],
                runtime=before,bindings=schedules,repo_predecessors=progress['repo_predecessors'],protected_artifacts_checked=len(protected),
                adopted_failed_request=progress['adopted_failed_request'],retained_failure_report=progress['retained_failure_report'],
                public_inputs_refetched=0,complete_original_inputs_reparsed=True,
                provider_requests=0,producer_invocations=0,consumer_invocations=0,private_account_reads=0,signal_writes=0,
                paid_ai_calls=0,public_writes=0,notifications_sent=0,schedules_changed=0,
                current_sec_identity_verified=False,historical_security_continuity_verified=False,
                free_float_change_qualified=False,buyback_execution_qualified=False,forecast_qualified=False,sizing_qualified=False)
        except Exception as exc:
            journal(s3,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
