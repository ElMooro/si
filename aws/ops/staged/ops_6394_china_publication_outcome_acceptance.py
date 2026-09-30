"""Exact native release and safe original-window China publication witnesses.

No actual current/private/account/consumer objects, provider requests, native
invokes, writes or schedule changes. A producer witness is not independent
source verification or current-pointer proof.
"""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,re,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-china-liquidity/source')]
from market_runtime_evidence import runtime,BUCKET
import china_store as store
FN='justhodl-china-liquidity'
START=datetime(2026,10,1,14,30,tzinfo=timezone.utc)
END=datetime(2026,10,1,14,45,tzinfo=timezone.utc)
PREFIX='[china-research] publication outcome:'
FLAGS=('multiple_head_atomic','independent_source_replay_verified','current_pointer_independently_verified','schedule_causation_verified','investment_authority')


class ReceiptOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**kw):
        if kw!={'Bucket':BUCKET,'Key':'data/ops/releases/'+FN+'.json'}:raise ValueError('Exact public native receipt only')
        return self.client.get_object(**kw)


def validate_native(actual,expected):
    if not re.fullmatch('[a-f0-9]{40}',expected):raise ValueError('Exact native source commit required')
    if actual['receipt']!={'status':'matched','commit':expected} or type(actual['source_files_checked']) is not int or actual['source_files_checked']!=4:
        raise ValueError('Exact four-source native package required')
    baseline=json.loads((ROOT/'docs/audit/2026-09-27/china-original-baseline.json').read_bytes())['actual_producer'];cfg=baseline['runtime']
    wanted={key:cfg[name] for key,name in {'function_name':'FunctionName','runtime':'Runtime','handler':'Handler','timeout':'Timeout','memory_mb':'MemorySize','architectures':'Architectures','role':'Role'}.items()}
    wanted.update(ephemeral_storage_mb=cfg['EphemeralStorage']['Size'],schedules=baseline['schedules'])
    if store.encode({key:actual[key] for key in wanted})!=store.encode(wanted):raise ValueError('Original resource or schedule setting differs')


def logging_scope(lam,actual):
    cfg=lam.get_function_configuration(FunctionName=FN);logging=cfg.get('LoggingConfig') or {}
    if cfg.get('FunctionName')!=FN or cfg.get('CodeSha256')!=actual['code_sha256'] or type(logging) is not dict:
        raise ValueError('Exact logging producer required')
    group=logging.get('LogGroup','/aws/lambda/'+FN);form=logging.get('LogFormat','Text')
    if group!='/aws/lambda/'+FN or form not in ('Text','JSON'):raise ValueError('Reviewed native log configuration required')
    return {'group':group,'format':form,'message_contract':'Exact plain source-print prefix and complete china-publication-outcome.v1 only'}


def _parse_event(event,compilers):
    at=event.get('timestamp');text=event.get('message')
    if type(at) is not int or not int(START.timestamp()*1000)<=at<int(END.timestamp()*1000):raise ValueError('Original complete execution window required')
    if type(text) is not str or len(text.encode('utf-8'))>8192 or not text.strip().startswith(PREFIX+' '):raise ValueError('Bounded source witness required')
    row=store.strict(text.strip()[len(PREFIX)+1:].encode('utf-8'))
    if type(row) is not dict or set(row)!={'contract','status','calculation_at','verified_at','request_id','function_version','compiler_sha256','provider_attempts','outputs',*FLAGS}:
        raise ValueError('Exact witness schema required')
    if row['contract']!='china-publication-outcome.v1' or row['status']!='producer_readbacks_verified' or any(row[k] is not False for k in FLAGS):
        raise ValueError('Only producer readback evidence with withheld independent authority allowed')
    started=store.measurements.clock(row['calculation_at']);finished=store.measurements.clock(row['verified_at'])
    if not START<=started<=finished<END or finished.timestamp()*1000>at+1:raise ValueError('Bounded original witness clocks required')
    if type(compilers) is not dict or set(compilers)!=set(store.COMPILERS) or any(type(v) is not str or not re.fullmatch('[a-f0-9]{64}',v) for v in compilers.values()):
        raise ValueError('Reviewed native compiler identities required')
    if store.encode(row['compiler_sha256'])!=store.encode(compilers):raise ValueError('Witness compiler identities differ')
    request_id=row['request_id'];version=row['function_version']
    if request_id is not None and (type(request_id) is not str or not re.fullmatch('[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}',request_id)):
        raise ValueError('Typed bounded request identity required')
    if version is not None and (type(version) is not str or not re.fullmatch(r'(?:\$LATEST|[1-9][0-9]*)',version)):raise ValueError('Typed bounded version identity required')
    if type(row['provider_attempts']) is not int or not 1<=row['provider_attempts']<=64:raise ValueError('Typed bounded attempt count required')
    outputs=row['outputs']
    if type(outputs) is not list or not 2<=len(outputs)<=3:raise ValueError('Complete declared output population required')
    for item in outputs:
        if (type(item) is not dict or set(item)!={'key','bytes','sha256'} or item['key'] not in store.KEYS
            or type(item['bytes']) is not int or not 0<item['bytes']<=store.LIMIT
            or type(item['sha256']) is not str or not re.fullmatch('[a-f0-9]{64}',item['sha256'])):raise ValueError('Exact public output identity required')
    keys=[item['key'] for item in outputs]
    if keys!=sorted(set(keys)) or not {store.HEAD,store.KEYS[1]}<=set(keys):raise ValueError('Complete unique head and history witnesses required')
    return {'timestamp_ms':at,'witness':row,'message_bytes':len(text.encode('utf-8')),'message_sha256':hashlib.sha256(text.encode('utf-8')).hexdigest()}


def parse_event(event,compilers):
    try:return _parse_event(event,compilers)
    except Exception:raise ValueError('Unreviewed China publication witness refused') from None


def collect(logs,compilers):
    rows=[];seen={}
    for page in logs.get_paginator('filter_log_events').paginate(logGroupName='/aws/lambda/'+FN,startTime=int(START.timestamp()*1000),endTime=int(END.timestamp()*1000),filterPattern='"'+PREFIX+'"'):
        for event in page.get('events',[]):
            row=parse_event(event,compilers);identity=event.get('eventId')
            if type(identity) is not str or not identity:raise ValueError('Complete event identity required')
            if identity in seen:
                if seen[identity]!=row:raise ValueError('Conflicting event identity')
                continue
            if len(rows)>=128:raise ValueError('Complete witness population exceeds reviewed bound')
            seen[identity]=row;rows.append(row)
    return sorted(rows,key=lambda row:(row['timestamp_ms'],row['message_sha256']))


def observe(now,logs_factory,compilers):
    if now.tzinfo is None:raise ValueError('Zoned diagnostic clock required')
    if now.astimezone(timezone.utc)<END:return {'status':'pending_original_window','complete_matching_witnesses':[],'application_log_query_count':0}
    rows=collect(logs_factory(),compilers)
    return {'status':'producer_readback_witnesses_found' if rows else 'no_reviewed_witness_found','complete_matching_witnesses':rows,'application_log_query_count':1}


def main():
    import boto3
    from ops_report import report
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN+'/source'],cwd=ROOT,text=True).strip()
    if expected=='b84b1513c115572e3cfffb5a5c66389134ea895f':raise ValueError('New publication-witness source release required')
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    clients=(lam,ReceiptOnly(s3),events,scheduler)
    with report('ops_6394_china_publication_outcome_acceptance') as report:
        before=runtime(*clients,FN);validate_native(before,expected);logging=logging_scope(lam,before);compilers=store.compiler_hashes()
        observed=observe(datetime.now(timezone.utc),lambda:boto3.client('logs',region_name='us-east-1'),compilers)
        after=runtime(*clients,FN);validate_native(after,expected)
        if store.encode(before)!=store.encode(after) or logging_scope(lam,after)!=logging:raise ValueError('Native producer changed during outcome inspection')
        report.kv(evidence={'expected_commit':expected,'native_before':before,'native_after':after,'logging':logging,'expected_compilers':compilers,
          'window_start':START.isoformat(),'window_end':END.isoformat(),'observation':observed,
          'native_invocations':0,'provider_requests':0,'current_packet_reads':0,'private_reads':0,'account_reads':0,'consumer_reads':0,
          'native_writes':0,'schedule_changes':0,'publication_independently_verified':False,'source_replay_verified':False,'investment_authority':False,
          'scope':'Only exact native control/package evidence and the complete reviewed producer outcome messages from the original October 1 execution window. No log query before the window closes. Producer readbacks do not establish independent original-source correctness, a current pointer or trigger causation.'})


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
