"""Preserve complete predecessor bytes and reject lost updates.

This binds derived publications, not original-source or investment qualification.
The history projection and current head are separate conditional S3 writes; they
are explicitly not a multi-object transaction. Immutable attempts survive failure.
"""
from datetime import datetime,timezone
import hashlib,json,re

CURRENT='data/bond-desk.json'
HISTORY='data/history/bond-desk.json'
PREFIX='data/bond-desk-research/publications/'
MAX=32*1024*1024
encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
sha=lambda raw:hashlib.sha256(raw).hexdigest()


def clock(value):
    if not isinstance(value,str):raise ValueError('Explicit publication time required')
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('Timezone required')
    return result.astimezone(timezone.utc)


def strict(raw):
    def pairs(rows):
        out={}
        for k,v in rows:
            if k in out:raise ValueError('Duplicate publication key')
            out[k]=v
        return out
    def invalid(value):raise ValueError('Nonfinite publication')
    doc=json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid)
    if not isinstance(doc,dict):raise ValueError('Whole publication object required')
    encode(doc);return doc


def read(client,bucket,key):
    if key not in (CURRENT,HISTORY) and not re.fullmatch(re.escape(PREFIX)+r'(?:predecessors|histories|outputs|attempts)/[a-f0-9]{64}\.json',key):
        raise ValueError('Unapproved publication read')
    try:obj=client.get_object(Bucket=bucket,Key=key)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('404','NoSuchKey'):return None
        raise
    try:raw=obj['Body'].read(MAX+1)
    finally:obj['Body'].close()
    if not 0<len(raw)<=MAX or not isinstance(obj.get('ETag'),str) or not obj['ETag']:raise ValueError('Complete conditional source required')
    return {'raw':raw,'etag':obj['ETag'],'doc':strict(raw)}


def begin(client,bucket,at):
    stamp=clock(at);head=read(client,bucket,CURRENT);history=read(client,bucket,HISTORY)
    if head and clock(head['doc'].get('generated_at'))>stamp:raise ValueError('Existing publication is newer than this run')
    rows=history['doc'] if history else {}
    for day,row in rows.items():
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',day) or not isinstance(row,dict) or 'anxiety' not in row:
            raise ValueError('Existing history must remain fully interpretable')
        if datetime.strptime(day,'%Y-%m-%d').date()>stamp.date():raise ValueError('History contains a future run')
    return {'started_at':at,'head':head,'history':history}


def retain(client,bucket,raw,kind):
    if not 0<len(raw)<=MAX or kind not in ('predecessors','histories','outputs','attempts'):raise ValueError('Bounded complete publication required')
    strict(raw);ref={'key':PREFIX+kind+'/'+sha(raw)+'.json','sha256':sha(raw),'bytes':len(raw)}
    try:client.put_object(Bucket=bucket,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='application/json',CacheControl='public, max-age=31536000, immutable')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    saved=read(client,bucket,ref['key'])
    if saved is None or saved['raw']!=raw:raise ValueError('Retained publication differs')
    return ref


def unchanged(client,bucket,key,old):
    current=read(client,bucket,key)
    if (current is None)!=(old is None) or (old and (current['etag']!=old['etag'] or current['raw']!=old['raw'])):
        raise ValueError('Concurrent publication changed; no merge or retry')


def conditional(client,bucket,key,raw,old):
    client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType='application/json',CacheControl='public, max-age=1800',
        **({'IfMatch':old['etag']} if old else {'IfNoneMatch':'*'}))
    current=read(client,bucket,key)
    if current is None or current['raw']!=raw:raise ValueError('Publication readback changed; never restore stale bytes')


def publish(client,bucket,snapshot,doc,history):
    at=clock(doc.get('generated_at'));started=clock(snapshot['started_at'])
    if at<started or (at-started).total_seconds()>180:raise ValueError('Publication outside native runtime')
    if snapshot['head'] and at<=clock(snapshot['head']['doc']['generated_at']):raise ValueError('Publication does not advance')
    if any(doc.get(k) is not False for k in ('calls_eligible','sizing_eligible','execution_eligible')) or doc.get('world_anxiety') is not None or doc.get('decision')!={'verb':'WAIT','meaning':'abstain'}:
        raise ValueError('Only unqualified abstention may publish')
    day=at.date().isoformat();old=snapshot['history']['doc'] if snapshot['history'] else {}
    if set(history)!=set(old)|{day} or any(encode(history[k])!=encode(v) for k,v in old.items() if k!=day):raise ValueError('History population changed')
    if history[day]!={'anxiety':None,'appetite':None,'eqbond':None}:raise ValueError('Unqualified history invented a signal')
    expected=[{'date':k,'value':v['anxiety']} for k,v in sorted(history.items())]
    if encode(doc.get('anxiety_history'))!=encode(expected):raise ValueError('Whole history projection differs')
    # Serialization precedes any write; rejected NaN/partial history cannot mutate storage.
    body=encode(doc);history_raw=encode(history)
    unchanged(client,bucket,CURRENT,snapshot['head']);unchanged(client,bucket,HISTORY,snapshot['history'])
    previous={name:retain(client,bucket,value['raw'],'predecessors') if value else None
              for name,value in (('head',snapshot['head']),('history',snapshot['history']))}
    out=retain(client,bucket,body,'outputs');hist=retain(client,bucket,history_raw,'histories')
    record={'contract':'bond-desk-publication.v1','generated_at':doc['generated_at'],'started_at':snapshot['started_at'],
        'predecessors':previous,'output':out,'history':hist,'snapshot_atomic':False,'original_source_replayed_here':False,
        'scope':'Complete derived publication and predecessor bytes; component evidence has separate qualification.',
        'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}
    ref=retain(client,bucket,encode(record),'attempts')
    current={**doc,'publication_record':{'manifest_key':ref['key'],'output_sha256':out['sha256'],
        'scope':'Derived-byte binding; separate history/current conditional writes, not original arithmetic replay.'}}
    # If either conditional write loses a race, raise without a rollback or retry.
    # The immutable attempt retains both predecessors and both intended outputs.
    conditional(client,bucket,HISTORY,history_raw,snapshot['history'])
    conditional(client,bucket,CURRENT,encode(current),snapshot['head'])
    return current
