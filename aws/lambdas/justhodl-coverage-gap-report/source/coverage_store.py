"""Bounded private input retention and conditional inventory publication.

No clients at import. Keys are the four existing inputs, original report path,
and a fixed protected content-addressed namespace. No provider/model calls.
"""
from datetime import datetime, timezone
from pathlib import Path
import hashlib,json,math,re,time,zlib
from coverage_model import INPUTS, CONTRACT, project

HEAD='data/audit/coverage-gap.json'
PREFIX='audit-private/20260909-originals/coverage-inventory/'
MAX_BYTES=64*1024*1024
MAX_TOTAL=128*1024*1024


def encoded(value):
    return (json.dumps(value,allow_nan=False,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode('utf-8')


def strict(raw,encoding=''):
    if not isinstance(raw,bytes) or len(raw)>MAX_BYTES:
        raise ValueError('Complete bounded input required')
    gzip=raw.startswith(b'\x1f\x8b')
    if encoding not in ('','identity','gzip') or encoding=='gzip' and not gzip or encoding=='identity' and gzip:
        raise ValueError('Input encoding differs')
    if gzip:
        dec=zlib.decompressobj(16+zlib.MAX_WBITS);value=dec.decompress(raw,MAX_BYTES+1)
        if len(value)>MAX_BYTES or dec.unconsumed_tail or not dec.eof or dec.unused_data:
            raise ValueError('Whole bounded gzip member required')
        raw=value
    def pairs(rows):
        out={}
        for key,value in rows:
            if key in out:raise ValueError('Duplicate source field')
            out[key]=value
        return out
    def number(value):
        out=float(value)
        if not math.isfinite(out):raise ValueError('Nonfinite source number')
        return out
    def reject(_):raise ValueError('Nonfinite source constant')
    return json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_float=number,parse_constant=reject)


def error_code(exc):
    return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))


def now():return datetime.now(timezone.utc).isoformat()


class PublicationUncertain(RuntimeError):
    pass


def read(client,bucket,key):
    private=isinstance(key,str) and re.fullmatch(re.escape(PREFIX)+r'(sources|inputs|outputs|compilers)/[a-f0-9]{64}\.bin',key)
    if key not in (*INPUTS.values(),HEAD) and not private:
        raise ValueError('Undeclared storage key')
    result=client.get_object(Bucket=bucket,Key=key);body=result['Body']
    try:raw=body.read(MAX_BYTES+1)
    finally:body.close()
    if (not isinstance(raw,bytes) or len(raw)>MAX_BYTES or type(result.get('ContentLength')) is not int
        or result['ContentLength']!=len(raw)):
        raise ValueError('Whole stored input differs')
    return raw,result

def retain(client,bucket,raw,kind):
    if kind not in ('sources','inputs','outputs','compilers') or not isinstance(raw,bytes) or len(raw)>MAX_BYTES:
        raise ValueError('Whole reviewed artifact required')
    digest=hashlib.sha256(raw).hexdigest();key=PREFIX+kind+'/'+digest+'.bin'
    try:
        client.put_object(Bucket=bucket,Key=key,Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='private, no-store')
    except Exception as exc:
        if error_code(exc) not in ('PreconditionFailed','ConditionalRequestConflict','409','412'):raise
    if read(client,bucket,key)[0]!=raw:raise ValueError('Retained bytes differ')
    return {'key':key,'sha256':digest,'bytes':len(raw)}

def publish(client,bucket,compiler_paths,context):
    if set(compiler_paths)!={'lambda_function.py','coverage_model.py','coverage_store.py'}:
        raise ValueError('Complete named local compiler set required')
    if context is None or not callable(getattr(context,'get_remaining_time_in_millis',None)):
        raise ValueError('Real remaining-time budget required')
    remaining=context.get_remaining_time_in_millis()
    if type(remaining) not in (int,float) or not math.isfinite(remaining) or not 40000<remaining<=900000:
        raise ValueError('Insufficient native time budget')
    deadline=time.monotonic()+min(95,remaining/1000-20)
    def budget():
        if time.monotonic()>=deadline:raise ValueError('Inventory publication budget exhausted')
    budget();started=now()
    try:old,metadata=read(client,bucket,HEAD)
    except Exception as exc:
        if error_code(exc) not in ('NoSuchKey','404'):raise
        prior={};etag=None;prior_ref=None
    else:
        prior=strict(old,str(metadata.get('ContentEncoding') or '').strip().lower())
        etag=metadata.get('ETag')
        if not isinstance(prior,dict) or not isinstance(etag,str) or not etag:
            raise ValueError('Complete versioned prior report required')
        prior_ref=retain(client,bucket,old,'outputs')
    attempts={};docs={};total=0
    for name,key in INPUTS.items():
        budget();requested=now()
        try:raw,meta=read(client,bucket,key)
        except Exception as exc:
            code=error_code(exc)
            status='source_missing' if code in ('NoSuchKey','404') else 'source_denied' if code in ('AccessDenied','403') else 'source_read_unavailable'
            attempts[name]={'source_key':key,'requested_at':requested,'received_at':now(),'status':status,'original_ref':None}
            docs[name]=None;continue
        received=now();total+=len(raw)
        if total>MAX_TOTAL:raise ValueError('Complete input set exceeds retention bound')
        budget();ref=retain(client,bucket,raw,'sources');encoding=str(meta.get('ContentEncoding') or '').strip().lower()
        attempts[name]={'source_key':key,'requested_at':requested,'received_at':received,'status':'received','original_ref':ref,'content_encoding':encoding}
        try:docs[name]=strict(raw,encoding)
        except (ValueError,UnicodeError,OverflowError,RecursionError,zlib.error):docs[name]=None
    budget();generated=now();packet={**prior,**project(attempts,docs,generated)}
    compilers={}
    for name,path in compiler_paths.items():
        budget();compilers[name]=retain(client,bucket,Path(path).read_bytes(),'compilers')
    manifest={'contract':CONTRACT,'acquisition_started_at':started,'generated_at':generated,'attempts':attempts,
              'previous_publication':prior_ref,'source_files':compilers,'stored_input_bytes':total,
              'scope':'Exact acquired platform artifacts; not original provider releases or first-availability evidence.'}
    packet['replay']={'input_ref':retain(client,bucket,encoded(manifest),'inputs'),'source_files':compilers,
                      'previous_publication':prior_ref,'originals_public':False,'independent_replay_verified':False}
    body=encoded(packet);result_ref=retain(client,bucket,body,'outputs');budget()
    try:
        client.put_object(Bucket=bucket,Key=HEAD,Body=body,ContentType='application/json',CacheControl='no-cache',
                               **({'IfMatch':etag} if etag is not None else {'IfNoneMatch':'*'}))
    except Exception as exc:
        raise PublicationUncertain('Conditional report acknowledgement unavailable; no retry or rollback assumed') from exc
    return packet,result_ref
