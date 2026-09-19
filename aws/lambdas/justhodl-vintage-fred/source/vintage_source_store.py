"""Bounded original ALFRED collection, replay, immutable runs and CAS publication."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo

import fred_vintage_model as model

BUCKET='justhodl-dashboard-live'
MAX_BYTES=64*1024*1024
MAX_PAGES=20
SEGMENTED_SERIES={'NFCI','STLFSI4'}
_rate_lock=threading.Lock()
_last_request=0.0


def now():return datetime.now(timezone.utc).isoformat()
def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))


def archive_cutoff(stamp=None):
    value=stamp or datetime.now(timezone.utc)
    if value.tzinfo is None:raise ValueError('timezone-aware source clock required')
    return value.astimezone(ZoneInfo('America/Chicago')).date().isoformat()


def bounded(stream):
    raw=stream.read(MAX_BYTES+1)
    if len(raw)>MAX_BYTES:raise ValueError('archive object exceeds explicit bound')
    return raw


def get(client,bucket,key):
    try:obj=client.get_object(Bucket=bucket,Key=key)
    except Exception as exc:
        if code(exc) in ('404','NoSuchKey'):return None,None,None
        raise
    raw=bounded(obj['Body']);return json.loads(raw),raw,obj['ETag']


def immutable(client,bucket,key,raw,kind='application/json'):
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*')
    except Exception as exc:
        if code(exc) not in ('412','PreconditionFailed','409','ConditionalRequestConflict'):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('immutable artifact differs')


def load_original(client,bucket,ref):
    zipped=bounded(client.get_object(Bucket=bucket,Key=ref['evidence']['key'])['Body'])
    with gzip.GzipFile(fileobj=io.BytesIO(zipped)) as stream:raw=bounded(stream)
    return {**ref,'raw':raw}


def acquire(client,bucket,req,api_key,deadline):
    global _last_request
    cache_key=model.PREFIX+'cache/'+model.digest(req)+'.json'
    cached,_,etag=get(client,bucket,cache_key)
    if cached:
        age=(datetime.now(timezone.utc)-model.clock(cached['acquired_at'])).total_seconds()
        # Current definition and open window revalidate daily. Closed windows
        # revalidate weekly; the original acquisition time never advances on a cache hit.
        end=req['params']['realtime_end']
        closed=end not in (model.MAX_DATE,archive_cutoff())
        ceiling=7*86400 if closed else 20*3600
        if 0<=age<ceiling:
            retained=load_original(client,bucket,cached)
            if retained['request']!=req:raise ValueError('cached request identity differs')
            model.original(retained,req['params']['series_id'],req['endpoint'])
            return retained
    url='https://api.stlouisfed.org/fred/'+req['endpoint']+'?'+urllib.parse.urlencode({**req['params'],'api_key':api_key})
    opener=urllib.request.build_opener()  # bypass any legacy synthetic urllib shim
    for attempt in range(3):
        if time.monotonic()>deadline-25:raise TimeoutError('source acquisition deadline')
        with _rate_lock:
            pause=max(0,.65-(time.monotonic()-_last_request))
            if pause:time.sleep(pause)
            _last_request=time.monotonic()
        try:
            request=urllib.request.Request(url,headers={'User-Agent':'JustHodl original archive research'})
            with opener.open(request,timeout=20) as response:raw=bounded(response)
            acquired=now();body=json.loads(raw)
            if 'error_code' in body:raise ValueError('provider rejected archive request')
            sha=hashlib.sha256(raw).hexdigest();key=model.PREFIX+'originals/'+model.digest(req)+'/'+sha+'.json.gz'
            immutable(client,bucket,key,gzip.compress(raw,mtime=0),'application/gzip')
            ref={'request':req,'acquired_at':acquired,
                 'evidence':{'key':key,'sha256':sha,'bytes':len(raw),'request_sha256':model.digest(req)}}
            model.original({**ref,'raw':raw},req['params']['series_id'],req['endpoint'])
            try:client.put_object(Bucket=bucket,Key=cache_key,Body=model.encoded(ref),ContentType='application/json',
                                  **({'IfMatch':etag} if etag else {'IfNoneMatch':'*'}))
            except Exception as exc:
                if code(exc) not in ('412','PreconditionFailed','409','ConditionalRequestConflict'):raise
            return {**ref,'raw':raw}
        except urllib.error.HTTPError as exc:
            if exc.code not in (429,500,502,503,504) or attempt==2:raise
            time.sleep(2*(attempt+1))
        except (TimeoutError,urllib.error.URLError):
            if attempt==2:raise
            time.sleep(2*(attempt+1))
    raise RuntimeError('archive retry exhausted')


def publish_current(client,bucket,key,output):
    for _ in range(4):
        previous,raw,etag=get(client,bucket,key)
        if previous:
            prior=previous.get('generated_at') or previous.get('updated')
            if prior and model.clock(prior)>model.clock(output['generated_at']):return False
            if previous.get('collection_started_at') and model.clock(previous['collection_started_at'])>model.clock(output['collection_started_at']):return False
            old_end=(previous.get('coverage') or {}).get('archive_end') or previous.get('archive_end')
            new_end=(output.get('coverage') or {}).get('archive_end') or output.get('archive_end')
            if old_end and new_end and old_end>new_end:return False
            if previous.get('contract') not in (model.CONTRACT,model.CATALOG,'fred-vintage-index.v1'):
                immutable(client,bucket,model.PREFIX+'legacy-unvalidated/'+hashlib.sha256(raw).hexdigest()+'.json',raw)
        try:
            client.put_object(Bucket=bucket,Key=key,Body=model.encoded(output),ContentType='application/json',CacheControl='no-store',
                              **({'IfMatch':etag} if etag else {'IfNoneMatch':'*'}))
            return True
        except Exception as exc:
            if code(exc) not in ('412','PreconditionFailed','409','ConditionalRequestConflict'):raise
    raise RuntimeError('current archive publication race retry limit')


def collect_series(client,bucket,sid,api_key,deadline,collection_id,archive_end,compiler,started,archive_start=None,definition=None):
    definition=definition or acquire(client,bucket,model.request(sid),api_key,deadline)
    meta=json.loads(definition['raw'])
    first=max('1990-01-01',min(d['realtime_start'] for d in meta['seriess']))
    if archive_start is not None:first=archive_start
    pages=[]
    for start,end in model.windows(first,archive_end):
        offset=0
        for _ in range(MAX_PAGES):
            page=acquire(client,bucket,model.request(sid,'series/observations',start,end,offset),api_key,deadline)
            body=json.loads(page['raw']);pages.append(page)
            n=len(body['observations']);count=body['count'];offset+=n
            if offset==count:break
            if not n or offset>count:raise ValueError('invalid source continuation')
        else:raise ValueError('source exceeds bounded page budget; not a complete archive')
    stamp=now();output=model.compile_series(sid,definition,pages,stamp,collection_id,archive_end,started,archive_start)
    strip=lambda entry:{k:v for k,v in entry.items() if k!='raw'}
    manifest={'contract':'fred-vintage-replay.v1','series':sid,'generated_at':stamp,'collection_id':collection_id,
              'archive_end':archive_end,'collection_started_at':started,'definition':strip(definition),'pages':[strip(p) for p in pages],
              'compiler':compiler,'output_sha256':model.digest(output)}
    if archive_start is not None:manifest['archive_start']=archive_start
    key=model.PREFIX+'runs/'+model.digest(manifest)+'.json';immutable(client,bucket,key,model.encoded(manifest))
    # Re-read retained originals before anything becomes current. Never use a
    # decoded cache response as a substitute for its verified original bytes.
    rebuilt=model.compile_series(sid,load_original(client,bucket,manifest['definition']),
        [load_original(client,bucket,p) for p in manifest['pages']],stamp,collection_id,archive_end,started,archive_start)
    if model.digest(rebuilt)!=manifest['output_sha256']:raise ValueError('retained archive replay differs')
    output['replay']={'manifest_key':key,'output_sha256':manifest['output_sha256'],'compiler':compiler}
    output_key=model.PREFIX+'outputs/'+model.digest(output)+'.json'
    immutable(client,bucket,output_key,model.encoded(output))
    return {'key':output_key,'sha256':model.digest(output),'replay':output['replay'],
            'coverage':output['coverage'],'generated_at':stamp,'acquired_at':output['acquired_at'],'n_vintages':output['n_vintages']}


def collect_segmented(client,bucket,sid,api_key,deadline,collection_id,archive_end,compiler,started):
    """Bound memory per 180-day archive interval, checkpoint complete closed segments."""
    definition=acquire(client,bucket,model.request(sid),api_key,deadline)
    meta=json.loads(definition['raw']);first=max('1990-01-01',min(d['realtime_start'] for d in meta['seriess']))
    cursor=model.day(first);last=model.day(archive_end);segments=[]
    while cursor<=last:
        end=min(cursor+timedelta(days=179),last);start=cursor.isoformat();stop=end.isoformat()
        identity={'series':sid,'archive_start':start,'archive_end':stop,'compiler':compiler,
                  'definition_sha256':definition['evidence']['sha256'],'contract':'fred-vintage-segment-cache.v1'}
        key=model.PREFIX+'segment-cache/'+model.digest(identity)+'.json'
        cached,_,etag=get(client,bucket,key);entry=None
        if cached and cached.get('identity')==identity:
            age=(datetime.now(timezone.utc)-model.clock(cached['generated_at'])).total_seconds()
            source_age=(datetime.now(timezone.utc)-model.clock(cached['entry']['acquired_at'])).total_seconds()
            if 0<=age<(7*86400 if stop<archive_end else 20*3600) and 0<=source_age<7*86400:
                candidate=cached['entry'];doc,_,_=get(client,bucket,candidate['key'])
                model.validate_segment(candidate,doc,sid)
                entry=candidate
        if entry is None:
            entry=collect_series(client,bucket,sid,api_key,deadline,collection_id,stop,compiler,started,start,definition)
            cached={'identity':identity,'entry':entry,'generated_at':now()}
            try:client.put_object(Bucket=bucket,Key=key,Body=model.encoded(cached),ContentType='application/json',
                                  **({'IfMatch':etag} if etag else {'IfNoneMatch':'*'}))
            except Exception as exc:
                if code(exc) not in ('412','PreconditionFailed','409','ConditionalRequestConflict'):raise
        segments.append(entry);cursor=end+timedelta(days=1)
    stamp=now();output=model.compile_catalog(sid,definition,segments,stamp,collection_id,archive_end,started)
    manifest={'contract':'fred-vintage-catalog-replay.v1','series':sid,'generated_at':stamp,'collection_id':collection_id,
        'archive_end':archive_end,'collection_started_at':started,'definition':{k:v for k,v in definition.items() if k!='raw'},
        'segments':segments,'compiler':compiler,'output_sha256':model.digest(output)}
    key=model.PREFIX+'runs/'+model.digest(manifest)+'.json';immutable(client,bucket,key,model.encoded(manifest))
    output['replay']={'manifest_key':key,'output_sha256':manifest['output_sha256'],'compiler':compiler}
    output_key=model.PREFIX+'outputs/'+model.digest(output)+'.json';immutable(client,bucket,output_key,model.encoded(output))
    return {'key':output_key,'sha256':model.digest(output),'replay':output['replay'],'coverage':output['coverage'],
        'generated_at':stamp,'acquired_at':output['acquired_at'],'n_vintages':output['n_vintages']}


def run(client,bucket,api_key):
    if not api_key:raise ValueError('managed FRED credential unavailable')
    started=now();collection_id=model.digest({'started_at':started,'series':model.SERIES})
    archive_end=archive_cutoff()
    source=Path(model.__file__).read_bytes();sha=hashlib.sha256(source).hexdigest()
    compiler={'key':model.PREFIX+'compilers/'+sha+'.py','sha256':sha}
    immutable(client,bucket,compiler['key'],source,'text/plain')
    deadline=time.monotonic()+780;results={};errors={}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures={executor.submit(collect_segmented if sid in SEGMENTED_SERIES else collect_series,
            client,bucket,sid,api_key,deadline,collection_id,archive_end,compiler,started):sid for sid in model.SERIES}
        for future in as_completed(futures):
            sid=futures[future]
            try:results[sid]=future.result()
            except Exception as exc:
                # Exception URLs may contain authentication. Publish only type/status.
                errors[sid]=('HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else
                             type(exc).__name__+(': '+str(exc)[:180] if isinstance(exc,ValueError) else ''))
    output={'contract':'fred-vintage-index.v1','version':'2.1.0','generated_at':now(),'updated':now(),
        'collection_id':collection_id,'collection_started_at':started,'archive_end':archive_end,
        'series':list(model.SERIES),'n_series':len(results),'requested_series':len(model.SERIES),
        'detail':{sid:({'status':'source_replayed',**results[sid]} if sid in results else {'status':'unavailable','error':errors[sid]}) for sid in model.SERIES},
        'errors':errors,'point_in_time':False,'historical_feature_replay_ready':False,
        'method':'Original provider archive periods and historical definitions. Immutable per-series runs; use the references in this index for one collection. No first-release-only or intraday-availability claim.'}
    if not results:raise RuntimeError('no source archives compiled; prior publications retained')
    # A single pointer publishes the complete per-series result set. Compatibility
    # aliases are subsequently updated; consumers must match collection ids.
    index_key=model.PREFIX+'collections/'+model.digest(output)+'.json';immutable(client,bucket,index_key,model.encoded(output))
    if not publish_current(client,bucket,'data/vintage/_index.json',output):
        return {'ok':True,'published':False,'reason':'newer collection current'}
    for sid,entry in results.items():
        doc,_,_=get(client,bucket,entry['key'])
        if model.digest(doc)!=entry['sha256']:raise ValueError('retained output differs')
        publish_current(client,bucket,'data/vintage/'+sid+'.json',doc)
    return {'ok':True,'published':True,'series_captured':len(results),'total':len(model.SERIES),
            'errors':errors,'collection_key':index_key,'collection_id':collection_id}
