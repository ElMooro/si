"""Read only the public Term Premium producer's acquisition and system reports."""
from pathlib import Path
from datetime import datetime,timedelta,timezone
import json,re,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
import ops_6292_term_premium_normal_acceptance as accepted
from ops_report import report
from market_runtime_evidence import runtime
store=accepted.store;model=accepted.model;FN=accepted.FN;BUCKET=accepted.BUCKET


def system_reports(logs,started_at):
    stamp=model.clock(started_at);end=min(stamp+timedelta(minutes=10),datetime.now(timezone.utc))
    if end<stamp:raise ValueError('Past own producer execution required')
    rows=[]
    for page in logs.get_paginator('filter_log_events').paginate(logGroupName='/aws/lambda/'+FN,
            startTime=int(stamp.timestamp()*1000),endTime=int(end.timestamp()*1000),filterPattern='"REPORT RequestId:"'):
        for event in page.get('events',[]):
            message=event['message'];identity=re.search(r'REPORT RequestId:\s+([a-f0-9-]{36})(?:\s|$)',message)
            if not identity:raise ValueError('Only native system REPORT records allowed')
            row={'request_id':identity[1],'timestamp_ms':event['timestamp']}
            for key,label,unit in [('duration_ms','Duration','ms'),('billed_duration_ms','Billed Duration','ms'),
                                 ('memory_mb','Memory Size','MB'),('max_memory_mb','Max Memory Used','MB')]:
                match=re.search(r'(?:^|\t)'+label+r':\s*([0-9]+(?:\.[0-9]+)?)\s*'+unit,message)
                if match:row[key]=float(match[1])
            for key,label in [('status','Status'),('error_type','Error Type')]:
                match=re.search(r'(?:^|\t)'+label+r':\s*([A-Za-z0-9_.-]+)',message)
                if match:row[key]=match[1]
            rows.append(row)
    return rows


def main():
    clients=[boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')]
    with report('ops_6293_term_premium_execution_diagnostic') as r:
        before=runtime(*clients,FN)
        if before!=json.loads(accepted.BASELINE.read_bytes()):raise ValueError('Accepted native package/cadence differs')
        journal,paths=accepted.latest_source_journal(clients[1])
        if not paths:raise ValueError('Own scheduled acquisition journal required')
        raw=store.bounded(clients[1].get_object(Bucket=BUCKET,Key=paths[0])['Body'])
        if store.sha(raw)!=journal['sha256']:raise ValueError('Acquisition journal changed during diagnosis')
        original=store.strict(raw).get('acquired_original');source=None
        if original is not None:
            key=accepted.own_predecessor(original,clients[1]);paths.append(key)
            source={'original':original,'whole_original_hash_verified':True}
        reports=system_reports(boto3.client('logs',region_name='us-east-1'),journal['started_at'])
        privacy=accepted.access.summarize([accepted.access.check(key) for key in paths])
        if not privacy['all_denied']:raise ValueError('Own protected evidence is accessible anonymously')
        if runtime(*clients,FN)!=before:raise ValueError('Runtime changed during diagnosis')
        r.kv(actual_runtime=before,own_acquisition_journal=journal,acquired_original=source,system_reports=reports,privacy=privacy,
             native_invocations=0,provider_requests=0,consumer_reads=0,account_reads=0,public_writes=0,schedule_changes=0,
             scope='Only this public producer package, its latest own acquisition journal/original, and system REPORT records in its ten-minute scheduled execution window. No application log bodies or consumer outputs.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
