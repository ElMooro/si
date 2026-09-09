"""Read only allowlisted runtime diagnostics for the failed research candidate."""
import json
import re
import sys
import time
from collections import Counter
from pathlib import Path
import boto3
from botocore.config import Config

ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/ops'))
from ops_report import report

FUNCTION='justhodl-research-backtest'
DEST=ROOT/'aws/ops/reports/5247_research_failure_metadata.json'


def main():
    config=Config(connect_timeout=15,read_timeout=45,retries={'max_attempts':2})
    lam=boto3.client('lambda',region_name='us-east-1',config=config)
    logs=boto3.client('logs',region_name='us-east-1',config=config)
    fields=('FunctionName','Version','Runtime','Timeout','MemorySize','CodeSha256','State','LastUpdateStatus','LastModified')
    current=lam.get_function_configuration(FunctionName=FUNCTION)
    result={'function':FUNCTION,'scope':'READ_ONLY_RUNTIME_METADATA','configuration':{k:current.get(k) for k in fields},
            'versions':[],'events':[],'raw_logs_reported':0,'application_payloads_reported':0}
    versions=[row for page in lam.get_paginator('list_versions_by_function').paginate(FunctionName=FUNCTION) for row in page['Versions']]
    for row in versions[-8:]:result['versions'].append({k:row.get(k) for k in fields})
    result['live_alias']={k:v for k,v in lam.get_alias(FunctionName=FUNCTION,Name='live').items() if k in ('Name','FunctionVersion','RoutingConfig')}
    start=int((time.time()-3*3600)*1000);token=None;counts=Counter();seen=set();pages=0;exhausted=False
    while pages<30:
        args={'logGroupName':'/aws/lambda/'+FUNCTION,'startTime':start,'limit':1000}
        if token:args['nextToken']=token
        page=logs.filter_log_events(**args);pages+=1
        for event in page.get('events',[]):
            message=event['message'];row={'timestamp':event['timestamp']}
            for marker in ('[backtest] starting','[backtest] universe:','[backtest] price coverage','[backtest] SPY history:',
                           '[backtest] DONE','[read] SOURCE_UNAVAILABLE','[fmp] PROVIDER_UNAVAILABLE',
                           'RESEARCH_DEADLINE_EXCEEDED_NO_PUBLICATION','Task timed out','Runtime.ExitError','Runtime.OutOfMemory'):
                if marker in message:counts[marker]+=1;row.setdefault('markers',[]).append(marker)
            errors=re.findall(r'\[ERROR\]\s+([A-Za-z0-9_.]*(?:Error|Exception))\b',message)
            if errors:row['error_types']=sorted(set(errors))
            locations=re.findall(r'File "/(?:var/task|var/runtime)/([A-Za-z0-9_./-]+\.py)", line ([0-9]+)',message)
            if locations:row['locations']=[{'path':p,'line':int(n)} for p,n in locations]
            version=re.search(r'^START RequestId: [A-Za-z0-9-]+ Version: ([0-9]+|\$LATEST)',message)
            if version:row['executed_version']=version.group(1)
            if message.startswith('REPORT RequestId:'):
                row['runtime_report']={key:float(value) for key,value in re.findall(r'(?:^|\t)(Duration|Billed Duration|Memory Size|Max Memory Used): ([0-9.]+)',message)}
                status=re.search(r'Status: ([a-z_]+)',message)
                if status:row['runtime_status']=status.group(1)
            if len(row)>1 and not any(m in row.get('markers',[]) for m in ('[read] SOURCE_UNAVAILABLE','[fmp] PROVIDER_UNAVAILABLE')):
                result['events'].append(row)
        token=page.get('nextToken')
        if not token or token in seen:
            exhausted=True;break
        seen.add(token)
    result.update(marker_counts=dict(counts),pages_read=pages,listing_exhausted=exhausted,ok=True)
    DEST.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'ok':True,'events':len(result['events']),'report':str(DEST.relative_to(ROOT))}))


if __name__=='__main__':
    with report('ops_5247_research_failure_metadata') as rep:
        try:main()
        except Exception as error:
            rep.fail('Read-only diagnostic stopped: '+type(error).__name__)
            sys.exit(1)
