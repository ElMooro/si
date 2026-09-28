"""Read only the producer's fixed safe exception-class diagnostic, never log bodies."""
from pathlib import Path
import re,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged')]
from ops_report import report
from market_runtime_evidence import runtime
import ops_6294_china_execution_diagnostic as scoped

PREFIX='[china-research] retained publication refused:'


def failure_classes(logs):
    results=[]
    for page in logs.get_paginator('filter_log_events').paginate(
            logGroupName='/aws/lambda/'+scoped.FN,startTime=int(scoped.START.timestamp()*1000),
            endTime=int(scoped.END.timestamp()*1000),filterPattern='"'+PREFIX+'"'):
        for event in page.get('events',[]):
            # This exact source print contains type(exc).__name__ only. Reject
            # added payloads, URLs, stack traces and unrelated log messages.
            message=event['message'].strip()
            match=re.fullmatch(re.escape(PREFIX)+r' ([A-Za-z_][A-Za-z0-9_]{0,79})',message)
            if not match or not scoped.START.timestamp()*1000 <= event['timestamp'] <= scoped.END.timestamp()*1000:
                raise ValueError('Only the reviewed safe exception-class record is allowed')
            results.append({'timestamp_ms':event['timestamp'],'exception_class':match[1]})
    return results


def main():
    clients=[boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')]
    with report('ops_6296_china_failure_class') as r:
        before=runtime(*clients,scoped.FN)
        if before['receipt']!={'status':'matched','commit':'cac0fa83b8df15aefac7419d42dbcf1d08b442b2'} or before['source_files_checked']!=4:
            raise ValueError('Original diagnosed package required')
        classes=failure_classes(boto3.client('logs',region_name='us-east-1'))
        if runtime(*clients,scoped.FN)!=before:raise ValueError('Producer changed during diagnostic')
        r.kv(actual_runtime=before,safe_exception_classes=classes,native_invocations=0,provider_requests=0,public_writes=0,
             account_reads=0,consumer_reads=0,raw_log_bodies_reported=0,
             scope='Only the exact reviewed type(exc).__name__ diagnostic from this public producer in September 28 14:30–14:40 UTC. No arbitrary application log body, stack trace or exception text is accepted or reported.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
