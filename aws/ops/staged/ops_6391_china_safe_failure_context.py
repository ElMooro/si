"""Read only reviewed safe China failure contexts from its original daily window.

No current/private/account/consumer packets, retained private originals, provider
requests, native invokes, data writes or schedule changes. Log bodies outside
the exact reviewed diagnostic schema are refused and never printed.
"""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,re,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-china-liquidity/source')]
from market_runtime_evidence import runtime,BUCKET
import china_store as store
FN='justhodl-china-liquidity'
SOURCE_COMMIT='b84b1513c115572e3cfffb5a5c66389134ea895f'
START=datetime(2026,9,30,14,30,tzinfo=timezone.utc)
END=datetime(2026,9,30,14,45,tzinfo=timezone.utc)
PREFIX='[china-research] failure context:'
STAGES={'validate_event','read_predecessors','retain_predecessors','validate_configuration','calculate_sources','identify_compilers','project_outputs',
 'retain_native_outputs','retain_projection_outputs','retain_plan','encode_publication','credential_guard','retain_publication','publish_head',
 'publish_history','publish_afre_cache','verify_publication_readback'}
CODES={'AccessDenied','PreconditionFailed','ConditionalRequestConflict','NoSuchKey','SlowDown','RequestTimeout','ServiceUnavailable'}

class ReceiptOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**request):
        if request!={'Bucket':BUCKET,'Key':'data/ops/releases/'+FN+'.json'}:raise ValueError('Only exact public release receipt is allowed')
        return self.client.get_object(**request)

def _parse_event(event):
    timestamp=event.get('timestamp');message=event.get('message')
    if type(timestamp) is not int or not int(START.timestamp()*1000)<=timestamp<int(END.timestamp()*1000):raise ValueError('Original daily diagnostic window required')
    if type(message) is not str or len(message.encode('utf-8'))>4096 or not message.strip().startswith(PREFIX+' '):raise ValueError('Reviewed bounded diagnostic required')
    text=message.strip()[len(PREFIX)+1:];row=store.strict(text.encode('utf-8'))
    required={'contract','stage','exception_class','reason'}
    optional={'started_at','service_error_code','retained_attempts','staged_outputs','last_attempt_sha256','publication_key'}
    if type(row) is not dict or not required<=set(row) or not set(row)<=required|optional:raise ValueError('Exact diagnostic schema required')
    if row['contract']!='china-publication-failure.v1' or row['stage'] not in STAGES:raise ValueError('Reviewed diagnostic stage required')
    if type(row['exception_class']) is not str or not re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,79}',row['exception_class']):raise ValueError('Safe exception class required')
    if row['reason'] not in store.SAFE_FAILURE_REASONS|{'Unclassified; raw exception withheld'}:raise ValueError('Only reviewed safe reason allowed')
    if 'started_at' in row:
        if type(row['started_at']) is not str:raise ValueError('Typed native start required')
        at=datetime.fromisoformat(row['started_at'].replace('Z','+00:00'))
        if at.tzinfo is None or not START<=at<END or at.timestamp()*1000>timestamp+1:raise ValueError('Bounded native execution clock required')
    if 'service_error_code' in row and row['service_error_code'] not in CODES:raise ValueError('Reviewed service error required')
    if 'retained_attempts' in row and (type(row['retained_attempts']) is not int or not 0<=row['retained_attempts']<=64):raise ValueError('Bounded typed attempt count required')
    if 'staged_outputs' in row:
        outputs=row['staged_outputs']
        if type(outputs) is not list or not all(type(k) is str and k in store.KEYS for k in outputs) or outputs!=sorted(set(outputs)):raise ValueError('Only original public output identities allowed')
    if 'last_attempt_sha256' in row and (type(row['last_attempt_sha256']) is not str or not re.fullmatch('[a-f0-9]{64}',row['last_attempt_sha256'])):raise ValueError('Exact attempt hash required')
    if 'publication_key' in row and row['publication_key'] not in store.KEYS:raise ValueError('Only original public output identity allowed')
    return {'timestamp_ms':timestamp,'diagnostic':row,'message_sha256':hashlib.sha256(message.encode('utf-8')).hexdigest(),'message_bytes':len(message.encode('utf-8'))}

def parse_event(event):
    try:return _parse_event(event)
    except Exception:raise ValueError('Unreviewed China diagnostic event refused') from None

def collect(logs):
    rows=[];seen={}
    for page in logs.get_paginator('filter_log_events').paginate(logGroupName='/aws/lambda/'+FN,startTime=int(START.timestamp()*1000),endTime=int(END.timestamp()*1000),filterPattern='"'+PREFIX+'"'):
        for event in page.get('events',[]):
            row=parse_event(event);identity=event.get('eventId')
            if type(identity) is not str or not identity:raise ValueError('Whole log event identity required')
            if identity in seen:
                if seen[identity]!=row:raise ValueError('Conflicting diagnostic event identity')
                continue
            if len(rows)>=128:raise ValueError('Complete diagnostic population exceeds reviewed bound')
            seen[identity]=row;rows.append(row)
    return sorted(rows,key=lambda row:(row['timestamp_ms'],row['message_sha256']))

def validate(actual):
    if actual['receipt']!={'status':'matched','commit':SOURCE_COMMIT} or type(actual['source_files_checked']) is not int or actual['source_files_checked']!=4:raise ValueError('Exact diagnostic producer required')
    baseline=json.loads((ROOT/'docs/audit/2026-09-27/china-original-baseline.json').read_bytes())['actual_producer'];cfg=baseline['runtime']
    expected={key:cfg[name] for key,name in {'function_name':'FunctionName','runtime':'Runtime','handler':'Handler','timeout':'Timeout','memory_mb':'MemorySize','architectures':'Architectures','role':'Role'}.items()}
    expected.update(ephemeral_storage_mb=cfg['EphemeralStorage']['Size'],schedules=baseline['schedules'])
    if store.encode({key:actual[key] for key in expected})!=store.encode(expected):raise ValueError('Original producer resources or cadence differ')

def main():
    import boto3
    from ops_report import report
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN+'/source'],cwd=ROOT,text=True).strip()
    if expected!=SOURCE_COMMIT:raise ValueError('Diagnostic source changed; review before reading logs')
    lam,s3,events,scheduler,logs=[boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler','logs')]
    clients=(lam,ReceiptOnly(s3),events,scheduler)
    with report('ops_6391_china_safe_failure_context') as report:
        before=runtime(*clients,FN);validate(before);rows=collect(logs);after=runtime(*clients,FN);validate(after)
        if store.encode(before)!=store.encode(after):raise ValueError('Native producer changed during diagnostic')
        report.kv(evidence={'source_commit':SOURCE_COMMIT,'native_before':before,'native_after':after,'window_start':START.isoformat(),'window_end':END.isoformat(),
          'status':'reviewed_failure_contexts_found' if rows else 'no_reviewed_failure_context_found','complete_matching_diagnostics':rows,
          'native_invocations':0,'provider_requests':0,'current_packet_reads':0,'private_reads':0,'account_reads':0,'consumer_reads':0,'native_writes':0,'schedule_changes':0,
          'publication_or_recovery_verified':False,'scope':'Only exact reviewed safe failure-context events in the original September 30 14:30-14:45 UTC window. No raw exception text, arbitrary logs, current packets or retained private source reads.'})

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
