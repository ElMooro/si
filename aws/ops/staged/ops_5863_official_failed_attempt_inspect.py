"""Read retained Official Pulse failure evidence; never invoke a collector."""
from datetime import datetime,timezone
from pathlib import Path
import json,sys,traceback
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-official-pulse/source')]
from ops_report import report
import official_research as model,official_store as store

def main():
    client=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live';read=store.raw_reader(client,bucket)
    with report('ops_5863_official_failed_attempt_inspect') as r:
        r.kv(collectors_invoked=0,paid_ai_calls=0,notifications_sent=0,private_account_reads=0,portfolio_writes=0)
        keys=[]
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix=model.PREFIX+'attempts/'):
            keys.extend(page.get('Contents',[]))
        for row in sorted(keys,key=lambda v:v['LastModified'],reverse=True)[:5]:
            attempt=json.loads(read(row['Key']));inputs=json.loads(read(attempt['input']['key']))
            r.kv(attempt_key=row['Key'],failure_class=attempt.get('failure_class'),generated_at=attempt['generated_at'],
                source_status_codes=attempt.get('source_status_codes'),retained_source_names=sorted(inputs.get('originals',{})),
                canonical_generated_at=inputs.get('canonical',{}).get('generated_at'))
            try:
                output,histories=model.build(inputs,read,attempt['generated_at'])
                r.kv(replay='reproduced',quality=output['quality'],histories=len(histories))
            except Exception as exc:
                frames=traceback.extract_tb(exc.__traceback__)
                # Parser messages are predefined; never emit provider response bodies or URLs.
                r.kv(replay_failure_class=type(exc).__name__,parser_error=str(exc)[:220],
                    frames=[{'file':Path(v.filename).name,'function':v.name,'line':v.lineno} for v in frames if Path(v.filename).name.startswith(('official_','canonical_'))])
        if not keys:
            logs=boto3.client('logs',region_name='us-east-1')
            start=int(datetime(2026,9,19,18,24,tzinfo=timezone.utc).timestamp()*1000)
            rows=logs.filter_log_events(logGroupName='/aws/lambda/justhodl-official-pulse',startTime=start,filterPattern='"[official-research]"',limit=30).get('events',[])
            safe=[v['message'].strip() for v in rows if '[official-research]' in v['message']]
            r.kv(no_retained_attempt=True,sanitized_engine_diagnostics=safe)

if __name__=='__main__':
    try:main()
    except Exception:
        print('Official failed-attempt inspection failed; see committed report.')
        sys.exit(1)
