"""Read-only diagnosis of the first complete native holdings execution.

Read only managed REPORT fields and retained research manifests, never raw logs.
No invocation, collection, engine publication, account, notification or schedule mutation.
"""
from pathlib import Path
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import json,re,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
from ops_5966_sector_tilt_runtime_diagnosis import parse_runtime
import etf_holdings_store as store
import etf_holdings_model as model
BUCKET='justhodl-dashboard-live'
COMMIT='a073f17597a303e1cf27cee408d0d42837b257a7'
FN='justhodl-etf-constituents'
REQUEST='chatgpt-'+FN+'-'+COMMIT[:12]+'-1'


def inventory(inputs,output,snapshots):
    assert inputs['kind']=='holdings' and output['contract']==model.CONTRACT
    assert set(inputs['collections'])==set(output['funds'])
    originals={};states=Counter()
    for pair in inputs['collections'].values():
        for collection in pair.values():
            states[collection['status']]+=1
            for page in [collection.get('selection'),*collection.get('pages',[])]:
                if page and page.get('original'):
                    ref=page['original'];originals[ref['key']]=ref['bytes']
            if collection.get('rejected_original'):
                ref=collection['rejected_original'];originals[ref['key']]=ref['bytes']
    assert len(snapshots)==2*len(output['funds'])
    contexts={r['key']:r['bytes'] for r in inputs['contexts'].values() if r}
    return {'configured_funds':len(output['funds']),'collections':dict(states),
        'provider_requests':inputs['provider_requests'],'original_provider_bytes':inputs['original_provider_bytes'],
        'distinct_provider_originals':len(originals),'distinct_provider_original_bytes':sum(originals.values()),
        'predecessor_contexts':len(inputs['contexts']),'distinct_predecessor_originals':len(contexts),
        'distinct_predecessor_bytes':sum(contexts.values()),'snapshot_count':len(snapshots),
        'normalized_rows_current_and_prior':sum(s['indexed_rows'] for s in snapshots),
        'row_artifacts':sum(len(s['parts']) for s in snapshots),'index_artifacts':sum(len(s['index_parts']) for s in snapshots),
        'largest_snapshots':[{'ticker':s['ticker'],'processed_date':s['processed_date'],'effective_dates':s['effective_dates'],
            'rows':s['indexed_rows'],'parts':len(s['parts'])} for s in sorted(snapshots,key=lambda s:s['indexed_rows'],reverse=True)[:12]]}


def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1');logs=boto3.client('logs',region_name='us-east-1')
    with report('ops_5979_etf_holdings_runtime_diagnosis') as r:
        status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=store.request_key('holdings',REQUEST))['Body']))
        assert status['request_id']==REQUEST and status['contract']=='etf-holdings-request.v1'
        execution=status['execution_id'];assert re.fullmatch(r'[a-f0-9-]{36}',execution)
        cfg=lam.get_function_configuration(FunctionName=FN)
        receipt=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key='data/ops/releases/'+FN+'.json')['Body']))
        assert receipt['commit']==COMMIT and receipt['code_sha256']==cfg['CodeSha256']
        start=model.clock(status['started_at']);end=datetime.now(timezone.utc)
        assert 0<(end-start).total_seconds()<7200
        args={'logGroupName':'/aws/lambda/'+FN,'filterPattern':'"'+execution+'"','startTime':int(start.timestamp()*1000)-5000,
            'endTime':min(int(end.timestamp()*1000),int(start.timestamp()*1000)+960000),'limit':50}
        found=[];seen=set()
        for _ in range(4):
            page=logs.filter_log_events(**args)
            for row in page.get('events',[]):
                parsed=parse_runtime(row.get('message',''),execution)
                if parsed:found.append({'timestamp_ms':row['timestamp'],**parsed})
            token=page.get('nextToken')
            if not token or token in seen:break
            seen.add(token);args['nextToken']=token
        r.kv(commit=COMMIT,function=FN,request={k:status.get(k) for k in ('request_id','execution_id','started_at','status','phase','completed_at')},
            configuration={k:cfg[k] for k in ('MemorySize','Timeout','CodeSha256','LastUpdateStatus')},managed_runtime_reports=found)
        read=store.reader(s3,BUCKET);runs=[]
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET,Prefix=model.PREFIX+'runs/'):
            for item in page.get('Contents',[]):
                if item['LastModified']>=start:runs.append(item['Key'])
            assert len(runs)<=4,'Diagnosis limited to the observed execution window'
        candidates=[]
        def snapshot(ref):
            raw=store.bounded(s3.get_object(Bucket=BUCKET,Key=ref['key'])['Body'])
            assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256']
            return json.loads(raw)
        for key in runs:
            raw=read(key);assert key==model.PREFIX+'runs/'+model.sha(raw)+'.json'
            run=json.loads(raw);assert run['contract']=='etf-holdings-replay.v1' and run['kind']=='holdings'
            inputs=store.checked(run['input'],model.PREFIX,'inputs',read)
            output=store.checked(run['output'],model.PREFIX,'outputs',read)
            assert output['generated_at']==run['generated_at'] and output['generated_at']==inputs['generated_at']
            assert all(output[k] is False for k in model.PERMISSIONS)
            refs=[f[role]['snapshot'] for f in output['funds'].values() for role in ('current','prior')]
            with ThreadPoolExecutor(max_workers=8) as pool:snapshots=list(pool.map(snapshot,refs))
            candidates.append({'run_key':key,'input':run['input'],'output':run['output'],'generated_at':run['generated_at'],
                'quality':output['quality'],'inventory':inventory(inputs,output,snapshots),
                'recovery_originals_and_compiler_manifest_retained':True,'full_original_replay_this_diagnosis':False})
        current={}
        for key in (model.CURRENT,model.LOOK_CURRENT):
            try:
                raw=read(key);doc=json.loads(raw)
                current[key]={'contract':doc.get('contract'),'generated_at':doc.get('generated_at'),'bytes':len(raw),'sha256':model.sha(raw)}
            except Exception as exc:
                if not store.missing(exc):raise
                current[key]={'status':'missing'}
        r.kv(retained_candidates=candidates,current_publications=current,engine_invocations=0,provider_requests=0,
            private_account_reads=0,notifications_sent=0,portfolio_writes=0,raw_log_messages_returned=0)
        assert found,'A managed runtime REPORT is required; elapsed time alone is not terminal evidence'


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
