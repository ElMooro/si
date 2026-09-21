"""Read only the one reviewed native Tilt execution's AWS runtime outcome.

No invoke, source refresh, schedule change, account read, notification or write
to an engine packet. Only the ops report is written by the runner.
"""
from pathlib import Path
from datetime import datetime,timezone
import json,re,sys,hashlib
import boto3
ROOT=Path(__file__).resolve().parents[3];sys.path.insert(0,str(ROOT/'aws/ops'))
from ops_report import report
BUCKET='justhodl-dashboard-live';FN='justhodl-sector-tilt'
COMMIT='cc78b3e809e7d9c7c3375db420dc26b2d078d2b8'
REQUEST='chatgpt-justhodl-sector-tilt-cc78b3e809e7-1'
KEY='data/sector-tilt-research/requests/'+hashlib.sha256(REQUEST.encode()).hexdigest()+'.json'

def parse_runtime(message,execution):
    if not isinstance(message,str) or execution not in message:return None
    if 'REPORT RequestId:' not in message:return None
    fields={}
    for name,pattern in [('duration_ms',r'\bDuration:\s*([\d.]+)\s*ms'),('billed_duration_ms',r'\bBilled Duration:\s*([\d.]+)\s*ms'),('memory_mb',r'\bMemory Size:\s*(\d+)\s*MB'),('max_memory_mb',r'\bMax Memory Used:\s*(\d+)\s*MB')]:
        match=re.search(pattern,message)
        if match:fields[name]=float(match.group(1))
    for name,pattern in [('status',r'\bStatus:\s*([A-Za-z_]+)'),('error_type',r'\bError Type:\s*([A-Za-z0-9_.]+)')]:
        match=re.search(pattern,message)
        if match:fields[name]=match.group(1)
    return fields

def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1');logs=boto3.client('logs',region_name='us-east-1')
    with report('ops_5966_sector_tilt_runtime_diagnosis') as r:
        obj=s3.get_object(Bucket=BUCKET,Key=KEY);raw=obj['Body'].read(1024*1024+1);assert len(raw)<=1024*1024
        status=json.loads(raw);assert status['request_id']==REQUEST and status['contract']=='sector-tilt-request.v1'
        execution=status['execution_id'];assert re.fullmatch(r'[a-f0-9-]{36}',execution)
        cfg=lam.get_function_configuration(FunctionName=FN)
        receipt=json.loads(s3.get_object(Bucket=BUCKET,Key='data/ops/releases/'+FN+'.json')['Body'].read())
        assert receipt['commit']==COMMIT and receipt['code_sha256']==cfg['CodeSha256']
        start=datetime.fromisoformat(status['started_at'].replace('Z','+00:00'));end=datetime.now(timezone.utc)
        assert start.tzinfo and 0<(end-start).total_seconds()<3600,'Diagnosis is bounded to the just-observed execution'
        args={'logGroupName':'/aws/lambda/'+FN,'filterPattern':'"'+execution+'"','startTime':int(start.timestamp()*1000)-5000,'endTime':min(int(end.timestamp()*1000),int(start.timestamp()*1000)+900000),'limit':50}
        found=[];seen=set()
        for _ in range(4):
            page=logs.filter_log_events(**args)
            for row in page.get('events',[]):
                parsed=parse_runtime(row.get('message',''),execution)
                if parsed:found.append({'timestamp_ms':row['timestamp'],**parsed})
            token=page.get('nextToken')
            if not token or token in seen:break
            seen.add(token);args['nextToken']=token
        current=s3.get_object(Bucket=BUCKET,Key='data/sector-tilt.json')['Body'].read(8*1024*1024+1);assert len(current)<=8*1024*1024
        packet=json.loads(current)
        r.kv(commit=COMMIT,function=FN,request={k:status.get(k) for k in ('request_id','execution_id','started_at','status','phase','completed_at')},
             configuration={k:cfg[k] for k in ('MemorySize','Timeout','CodeSha256','LastUpdateStatus')},managed_runtime_reports=found,
             current_publication={'contract':packet.get('contract'),'generated_at':packet.get('generated_at') or packet.get('as_of'),'sha256':hashlib.sha256(current).hexdigest()},
             engine_invocations=0,provider_requests=0,private_account_reads=0,notifications_sent=0,portfolio_writes=0,raw_log_messages_returned=0)
        assert found,'No managed runtime report found; never infer terminal status from an elapsed clock alone'

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
