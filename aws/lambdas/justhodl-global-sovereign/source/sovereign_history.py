"""Preserve complete predecessors and refuse stale/conflicting history writes.

The two conditional projections are not an atomic transaction. A retained
attempt records intended bytes, not proof that both public writes completed.
This storage contract does not validate the predecessor's sovereign model.
"""
from datetime import datetime,date,timezone
import hashlib,json

HEAD='data/global-sovereign.json'
HISTORY='data/global-sovereign-history.json'
PRIVATE='audit-private/20260909-originals/global-sovereign-research/'
MAX=32*1024*1024
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def clock(value):
    value=datetime.fromisoformat(value.replace('Z','+00:00'))
    if value.tzinfo is None:raise ValueError('Aware publication clock required')
    return value.astimezone(timezone.utc)


def strict(raw):
    def pairs(rows):
        result={}
        for key,value in rows:
            if key in result:raise ValueError('Duplicate predecessor key')
            result[key]=value
        return result
    def invalid(value):raise ValueError('Nonfinite predecessor JSON')
    doc=json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid);encode(doc)
    return doc


def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if not 0<len(raw)<=MAX:raise ValueError('Whole bounded predecessor required')
    return raw


def read(client,bucket,key):
    if key not in (HEAD,HISTORY):raise ValueError('Only public sovereign predecessors allowed')
    try:obj=client.get_object(Bucket=bucket,Key=key)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('NoSuchKey','404'):return None
        raise
    if not obj.get('ETag'):raise ValueError('Predecessor version required')
    raw=bounded(obj['Body']);return {'raw':raw,'doc':strict(raw),'etag':obj['ETag']}


def daily_rows(rows):
    if not isinstance(rows,list):raise ValueError('Whole history list required')
    result={}
    for row in rows:
        if not isinstance(row,dict):raise ValueError('History row required')
        key=row.get('date');day=date.fromisoformat(key)
        if day.isoformat()!=key or key in result:raise ValueError('Unique ISO history date required')
        result[key]=row
    return result


def begin(client,bucket,started_at):
    at=clock(started_at);head=read(client,bucket,HEAD);history=read(client,bucket,HISTORY)
    if head and (not isinstance(head['doc'],dict) or clock(head['doc']['generated_at'])>at):raise ValueError('Invalid or newer current publication')
    if history and any(date.fromisoformat(day)>at.date() for day in daily_rows(history['doc'])):raise ValueError('Future history date')
    return {'started_at':started_at,'head':head,'history':history}


def retain(client,bucket,raw):
    if not 0<len(raw)<=MAX:raise ValueError('Whole retention bound')
    ref={'key':PRIVATE+sha(raw)+'.bin','sha256':sha(raw),'bytes':len(raw)}
    try:client.put_object(Bucket=bucket,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if bounded(client.get_object(Bucket=bucket,Key=ref['key'])['Body'])!=raw:raise ValueError('Retained predecessor differs')
    return ref


def publish(client,bucket,state,payload,history=None):
    at=clock(payload['generated_at']);started=clock(state['started_at'])
    if not 0<=(at-started).total_seconds()<=300:raise ValueError('Publication outside native runtime')
    if state['head'] and at<=clock(state['head']['doc']['generated_at']):raise ValueError('Publication does not advance')
    if any(payload.get(k) is not False for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')):raise ValueError('Unqualified predecessor cannot grant authority')
    old=daily_rows(state['history']['doc']) if state['history'] else {}
    if history is not None:
        new=daily_rows(history);today=at.date().isoformat()
        if any(day not in new or (day!=today and encode(row)!=encode(new[day])) for day,row in old.items()):raise ValueError('Historical rows changed or disappeared')
        if any(day not in old and day!=today for day in new):raise ValueError('Backfilled history prohibited')
        if encode(payload.get('eurodollar_hub_history'))!=encode(history):raise ValueError('Complete displayed history must match stored history')
    writes={HEAD:encode(payload)}
    if history is not None:writes[HISTORY]=encode(history)
    predecessors={HEAD:state['head'],HISTORY:state['history']}
    attempt={'contract':'sovereign-publication-attempt.v1','generated_at':payload['generated_at'],
        'snapshot_atomic':False,'status':'planned_bytes_only','original_source_replay':False,
        'current_payload_basis':'complete_output_without_publication_record',
        'predecessors':{key:retain(client,bucket,value['raw']) if value else None for key,value in predecessors.items()},
        'intended':{key:retain(client,bucket,raw) for key,raw in writes.items()}}
    receipt=retain(client,bucket,encode(attempt))
    writes[HEAD]=encode({**payload,'publication_record':{'manifest_key':receipt['key'],'output_sha256':sha(writes[HEAD]),
        'scope':'Derived bytes without this reference; conditional history/current projections are not an atomic transaction or source replay.'}})
    if len(writes[HEAD])>MAX:raise ValueError('Whole published head exceeds read bound')
    for key,prior in predecessors.items():
        actual=read(client,bucket,key)
        if (actual is None)!=(prior is None) or actual and (actual['etag']!=prior['etag'] or actual['raw']!=prior['raw']):raise ValueError('Concurrent predecessor changed')
    # History first preserves the established projection order. No retry/rollback
    # can overwrite a concurrent writer if the subsequent current CAS fails.
    for key in (HISTORY,HEAD):
        if key not in writes:continue
        prior=predecessors[key]
        client.put_object(Bucket=bucket,Key=key,Body=writes[key],ContentType='application/json',CacheControl='public, max-age=1800',
                         **({'IfMatch':prior['etag']} if prior else {'IfNoneMatch':'*'}))
    return receipt
