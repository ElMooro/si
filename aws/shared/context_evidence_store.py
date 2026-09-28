"""Bounded immutable context retention and conditional publication.

The caller declares its fixed existing inputs, output and protected audit prefix.
No model/provider, account discovery, notifications or downloaded code execution.
Public projection is a separately reviewed pure function.
"""
from datetime import datetime,timezone
from pathlib import Path
import hashlib,json,math,re,time,zlib

MAX_BYTES=16*1024*1024
MAX_TOTAL=64*1024*1024
AUDIT='audit-private/20260909-originals/'


def sha(raw):return hashlib.sha256(raw).hexdigest()
def encode(value):return (json.dumps(value,ensure_ascii=False,sort_keys=True,allow_nan=False,separators=(',',':'))+'\n').encode('utf-8')
def now():return datetime.now(timezone.utc).isoformat()
def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))


def clock(value):
    try:
        stamp=datetime.fromisoformat(value.replace('Z','+00:00')) if isinstance(value,str) else None
        return stamp.astimezone(timezone.utc) if stamp is not None and stamp.tzinfo is not None else None
    except ValueError:return None


def strict(raw,encoding=''):
    if not isinstance(raw,bytes) or len(raw)>MAX_BYTES:raise ValueError('Whole bounded context required')
    gzip=raw.startswith(b'\x1f\x8b');decoded=raw
    if encoding not in ('','identity','gzip') or encoding=='gzip' and not gzip or encoding=='identity' and gzip:raise ValueError('Context encoding mismatch')
    if gzip:
        decoder=zlib.decompressobj(16+zlib.MAX_WBITS);decoded=decoder.decompress(raw,MAX_BYTES+1)
        if len(decoded)>MAX_BYTES or decoder.unconsumed_tail or not decoder.eof or decoder.unused_data:raise ValueError('Whole bounded gzip member required')
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate JSON key')
            out[key]=value
        return out
    def number(value):
        value=float(value)
        if not math.isfinite(value):raise ValueError('Nonfinite context number')
        return value
    def constant(_):raise ValueError('Nonfinite context JSON')
    return json.loads(decoded.decode('utf-8'),object_pairs_hook=pairs,parse_float=number,parse_constant=constant)


def identity(raw,prefix,kind):
    if (not isinstance(prefix,str) or not re.fullmatch(re.escape(AUDIT)+r'[a-z0-9/-]+/',prefix)
        or '..' in prefix or kind not in ('sources','inputs','outputs','compilers')
        or not isinstance(raw,bytes) or len(raw)>MAX_BYTES):raise ValueError('Reviewed protected context artifact required')
    return {'key':prefix+kind+'/'+sha(raw)+'.bin','sha256':sha(raw),'bytes':len(raw)}


def validate_ref(ref,prefix,kind):
    if (not isinstance(ref,dict) or set(ref)!=set(('key','bytes','sha256')) or
        not isinstance(ref['sha256'],str) or not re.fullmatch('[a-f0-9]{64}',ref['sha256']) or
        type(ref['bytes']) is not int or not 0<=ref['bytes']<=MAX_BYTES or
        ref['key']!=prefix+kind+'/'+ref['sha256']+'.bin'):
        raise ValueError('Exact protected context identity required')
    # Validate the static namespace and artifact class independently of its hash.
    identity(b'',prefix,kind)
    return ref


class ContextStore:
    def __init__(self,client,bucket,head,inputs,prefix,contract,compiler_paths):
        if not isinstance(inputs,dict) or not inputs or len(set(inputs.values()))!=len(inputs):raise ValueError('Unique declared inputs required')
        if any(not isinstance(k,str) or not re.fullmatch(r'data/[a-z0-9-]+\.json',k) for k in (*inputs.values(),head)):raise ValueError('Literal declared context keys required')
        identity(b'',prefix,'sources')
        if not isinstance(compiler_paths,dict) or not compiler_paths or any(not re.fullmatch(r'[a-z0-9_]+\.py',name) for name in compiler_paths):raise ValueError('Named local compiler files required')
        self.client=client;self.bucket=bucket;self.head=head;self.inputs=dict(inputs);self.prefix=prefix;self.contract=contract;self.compilers=dict(compiler_paths)
    def read(self,key):
        retained=isinstance(key,str) and re.fullmatch(re.escape(self.prefix)+r'(?:sources|inputs|outputs|compilers)/[a-f0-9]{64}\.bin',key)
        if key not in (*self.inputs.values(),self.head) and not retained:raise ValueError('Undeclared context key')
        obj=self.client.get_object(Bucket=self.bucket,Key=key);stream=obj['Body']
        try:raw=stream.read(MAX_BYTES+1)
        finally:stream.close()
        if len(raw)>MAX_BYTES or type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw):raise ValueError('Whole stored context differs')
        return raw,obj
    def retain(self,raw,kind):
        ref=identity(raw,self.prefix,kind)
        try:self.client.put_object(Bucket=self.bucket,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='private, no-store')
        except Exception as exc:
            if code(exc) not in ('PreconditionFailed','ConditionalRequestConflict','409','412'):raise
        if self.read(ref['key'])[0]!=raw:raise ValueError('Retained context bytes differ')
        return ref
    def publish(self,project):
        started=time.monotonic();started_at=now()
        try:previous,meta=self.read(self.head)
        except Exception as exc:
            if code(exc) not in ('NoSuchKey','404'):raise
            previous_ref=None;etag=None
        else:
            etag=meta.get('ETag')
            if not isinstance(etag,str) or not etag:raise ValueError('Prior context head identity missing')
            previous_ref=self.retain(previous,'outputs')
        attempts={};sources={};total=0
        for name,key in self.inputs.items():
            if time.monotonic()-started>230:raise ValueError('Context acquisition budget exhausted')
            requested=now()
            try:raw,meta=self.read(key)
            except Exception:
                attempts[name]={'source_key':key,'status':'source_read_unavailable','requested_at':requested,'received_at':now()};continue
            received=now();total+=len(raw)
            if total>MAX_TOTAL:raise ValueError('Complete context scope exceeds retention bound')
            ref=self.retain(raw,'sources');sources[ref['key']]=raw
            attempts[name]={'source_key':key,'status':'received','requested_at':requested,'received_at':received,
                'original_ref':ref,'content_encoding':str(meta.get('ContentEncoding') or '').strip().lower()}
        generated=now();packet=project(attempts,sources,generated)
        if not isinstance(packet,dict) or packet.get('measurement_contract')!=self.contract or packet.get('generated_at')!=generated:raise ValueError('Exact projection contract required')
        compilers={name:self.retain(Path(path).read_bytes(),'compilers') for name,path in self.compilers.items()}
        manifest={'contract':self.contract,'acquisition_started_at':started_at,'generated_at':generated,'attempts':attempts,
            'previous_publication':previous_ref,'source_files':compilers,'stored_source_bytes':total,
            'limits':{'per_source_bytes':MAX_BYTES,'aggregate_source_bytes':MAX_TOTAL}}
        packet['replay']={'input_ref':self.retain(encode(manifest),'inputs'),'source_files':compilers,'previous_publication':previous_ref,'originals_public':False}
        packet['acquisition_started_at']=started_at;body=encode(packet);ref=self.retain(body,'outputs')
        if time.monotonic()-started>250:raise ValueError('Context publication budget exhausted')
        condition={'IfMatch':etag} if etag is not None else {'IfNoneMatch':'*'}
        self.client.put_object(Bucket=self.bucket,Key=self.head,Body=body,ContentType='application/json',CacheControl='max-age=600',**condition)
        return packet,ref
