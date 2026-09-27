"""Whole original retention, replay and conditional publication for RSS research."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
from pathlib import Path
from copy import deepcopy
import ast,json,math,re,urllib.request,urllib.error,urllib.parse
import geo_news_model as model

HEAD='data/geopolitical-risk.json';HISTORY='geo/geopolitical-risk-history.json';SOVEREIGN='data/global-sovereign.json'
PRIVATE='audit-private/20260909-originals/geopolitical-risk-research/'
LIMIT=64*1024*1024;HTTP_LIMIT=8*1024*1024
COMPILERS=('lambda_function.py','geo_feeds.json','geo_news_model.py','geo_news_store.py')


class CaptureError(ValueError):pass


def strict(raw):
    def pairs(items):
        out={}
        for k,v in items:
            if k in out:raise CaptureError('Duplicate JSON key')
            out[k]=v
        return out
    def invalid(value):raise CaptureError('Nonfinite JSON value')
    def number(value):
        v=float(value)
        if not math.isfinite(v):invalid(value)
        return v
    return json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_constant=invalid,parse_float=number)


def whole(stream,limit=LIMIT,length=None):
    parts=[];size=0
    try:
        while True:
            part=stream.read(min(65536,limit+1-size))
            if not part:break
            if not isinstance(part,bytes):raise CaptureError('Byte stream required')
            size+=len(part);parts.append(part)
            if size>limit:raise CaptureError('Complete byte bound exceeded')
    finally:stream.close()
    raw=b''.join(parts)
    if length is not None and (not str(length).isdigit() or int(length)!=len(raw)):raise CaptureError('Complete response length differs')
    return raw


def hashes():return {name:model.sha((Path(__file__).parent/name).read_bytes()) for name in COMPILERS}


def configuration():
    tree=ast.parse((Path(__file__).parent/'lambda_function.py').read_text(encoding='utf-8'))
    countries=[ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='COUNTRIES' for t in n.targets)]
    if len(countries)!=1:raise CaptureError('One complete country configuration required')
    return strict((Path(__file__).parent/'geo_feeds.json').read_bytes()),countries[0]


def retain(s3,bucket,raw):
    if not isinstance(raw,bytes) or len(raw)>LIMIT:raise CaptureError('Whole bounded retention required')
    ref={'key':PRIVATE+model.sha(raw)+'.bin','sha256':model.sha(raw),'bytes':len(raw)}
    try:s3.put_object(Bucket=bucket,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if retained(s3,bucket,ref)!=raw:raise CaptureError('Whole retained bytes differ')
    return ref


def retained(s3,bucket,ref):
    if (not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256')))
            or ref.get('key')!=PRIVATE+ref['sha256']+'.bin' or type(ref.get('bytes')) is not int or not 0<=ref['bytes']<=LIMIT):
        raise CaptureError('Invalid protected identity')
    obj=s3.get_object(Bucket=bucket,Key=ref['key']);raw=whole(obj['Body'],length=obj.get('ContentLength'))
    if type(obj.get('ContentLength')) is not int or len(raw)!=ref['bytes'] or model.sha(raw)!=ref['sha256']:
        raise CaptureError('Complete retained identity differs')
    return raw


def read(s3,bucket,key):
    if key not in (HEAD,HISTORY,SOVEREIGN):raise CaptureError('Unreviewed stored input')
    try:obj=s3.get_object(Bucket=bucket,Key=key)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('NoSuchKey','404'):return {'status':'missing'}
        raise
    raw=whole(obj['Body'],length=obj.get('ContentLength'))
    if type(obj.get('ContentLength')) is not int or not obj.get('ETag'):raise CaptureError('Whole versioned input required')
    ref=retain(s3,bucket,raw);p=strict(raw)
    if not isinstance(p,dict):raise CaptureError('Complete predecessor object required')
    return {'status':'retained','original':ref,'etag':obj['ETag'],'last_modified':obj['LastModified'].isoformat(),
            'version_id':obj.get('VersionId'),'packet':p}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None


def acquire(s3,bucket,feed,allowed_hosts,opener=None):
    open_url=opener or urllib.request.build_opener(NoRedirect()).open
    url=feed['url'];attempts=[]
    for index in range(4):
        at=datetime.now(timezone.utc).isoformat()
        req=urllib.request.Request(url,headers={'User-Agent':'JustHodl research admin@justhodl.ai','Accept':'application/rss+xml,application/atom+xml,application/xml,text/xml'})
        try:response=open_url(req,timeout=12)
        except urllib.error.HTTPError as exc:response=exc
        except (urllib.error.URLError,OSError,TimeoutError) as exc:
            row={'url':url,'acquired_at':at,'status':'transport_error','error_type':type(exc).__name__}
            attempts.append(row);break
        headers=response.headers or {};status=response.getcode()
        if type(status) is not int or not 100<=status<=599:raise CaptureError('HTTP status required')
        raw=whole(response,HTTP_LIMIT,headers.get('Content-Length'))
        row={'url':url,'acquired_at':at,'status':'http_response','http_status':status,
             'content_encoding':headers.get('Content-Encoding',''),
             'headers':{k:headers[k] for k in ('Content-Type','Content-Length','Content-Encoding','Date','Last-Modified','ETag','Retry-After','Location') if k in headers},
             'original':retain(s3,bucket,raw),'body_sha256':model.sha(raw),'body_bytes':len(raw)}
        attempts.append(row)
        if status not in (301,302,303,307,308):break
        target=urllib.parse.urljoin(url,headers.get('Location',''));parts=urllib.parse.urlsplit(target)
        permitted=(target!=url and parts.scheme in ('http','https') and parts.hostname in allowed_hosts and not parts.username and not parts.password
                   and not parts.fragment and not (url.startswith('https:') and parts.scheme!='https'))
        if index==3 or not permitted:
            row['redirect_not_followed']=True;break
        url=target
    result={'feed_id':feed['feed_id'],**{k:v for k,v in row.items() if k not in ('url','headers')},'attempts':attempts}
    result['attempt_manifest']=retain(s3,bucket,model.encode(result))
    return result


def history_projection(previous,packet,calculation_ref,manifest_ref):
    result=deepcopy(previous or {'days':{}})
    if not isinstance(result.get('days',{}),dict):raise CaptureError('Legacy history must remain a complete object')
    runs=result.setdefault('research_runs',{})
    if not isinstance(runs,dict):raise CaptureError('Complete research history required')
    row={'generated_at':packet['generated_at'],'contract':model.CONTRACT,'calculation':calculation_ref,'acquisition_manifest':manifest_ref,
         'country_measurements':packet['rankings'],'coverage':packet['sources']}
    ident=calculation_ref['sha256']
    if ident in runs and runs[ident]!=row:raise CaptureError('Existing research run identity differs')
    runs[ident]=row
    result.update(research_contract='geopolitical-news-history.v1',legacy_days_comparable_with_new_measurements=False)
    return result


def run(s3,bucket,corpus,countries,at=None,opener=None,workers=8):
    started_at=datetime.now(timezone.utc).isoformat()
    if (corpus,countries)!=configuration():raise CaptureError('Exact reviewed feed/country configuration required')
    # Complete predecessors are retained before a single provider request.
    inputs={key:read(s3,bucket,key) for key in (HEAD,HISTORY,SOVEREIGN)}
    if inputs[HISTORY]['status']=='retained' and not isinstance(inputs[HISTORY]['packet'].get('days',{}),dict):
        raise CaptureError('Legacy history is malformed')
    feeds=model.corpus(corpus);hosts={urllib.parse.urlsplit(f['url']).hostname for f in feeds}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        captures=list(pool.map(lambda f:acquire(s3,bucket,f,hosts,opener),feeds))
    # Freeze the counting window after acquisition. A headline actually
    # published while downloads ran must not be mistaken for a future item.
    at=at or datetime.now(timezone.utc).isoformat()
    input_refs={k:{x:y for x,y in v.items() if x!='packet'} for k,v in inputs.items()}
    manifest={'contract':'geopolitical-news-acquisition.v1','generated_at':at,'acquisition_started_at':started_at,'compiler_sha256':hashes(),
              'feeds':feeds,'captures':captures,'inputs':input_refs,'country_aliases':countries,
              'inputs_atomic':False,'point_in_time_verified':False,'forecast_qualified':False}
    manifest_ref=retain(s3,bucket,model.encode(manifest))
    packet=model.build(feeds,captures,countries,at,inputs[SOVEREIGN].get('packet'),lambda c:retained(s3,bucket,c['original']))
    calculated=retain(s3,bucket,model.encode(packet))
    history=history_projection(inputs[HISTORY].get('packet'),packet,calculated,manifest_ref)
    history_raw=model.encode(history);history_ref=retain(s3,bucket,history_raw)
    packet['publication_context']={'contract':'geopolitical-news-publication.v1','manifest':manifest_ref,'calculation':calculated,
        'planned_complete_history':history_ref,
        'compiler_sha256':hashes(),'complete_predecessors':input_refs,'publication_atomic':False,
        'original_source_replay_verified':False,'point_in_time_verified':False}
    planned={HISTORY:history_raw,HEAD:model.encode(packet)}
    projected={key:retain(s3,bucket,raw) for key,raw in planned.items()}
    journal={'manifest':manifest_ref,'projected_outputs':projected,'publication_order':[HISTORY,HEAD]}
    retain(s3,bucket,model.encode(journal))
    published=[]
    for key,raw in planned.items():
        condition={'IfMatch':inputs[key]['etag']} if inputs[key]['status']=='retained' else {'IfNoneMatch':'*'}
        try:s3.put_object(Bucket=bucket,Key=key,Body=raw,ContentType='application/json',CacheControl='public, max-age=600' if key==HEAD else 'no-store',**condition)
        except Exception as exc:
            if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('409','412','ConditionalRequestConflict','PreconditionFailed'):
                return {'published':False,'state':'concurrent_writer_preserved','completed_paths':published}
            raise
        published.append(key)
    return {'published':True,'generated_at':at,'configured_feeds':len(feeds),'country_rows':len(packet['rankings']),
            'manifest_sha256':manifest_ref['sha256'],'portfolio_action':'WAIT'}


def replay(s3,bucket,packet):
    context=packet.get('publication_context') or {}
    if context.get('contract')!='geopolitical-news-publication.v1' or context.get('compiler_sha256')!=hashes():
        raise CaptureError('Exact native compiler declaration required')
    if any(context.get(k) is not False for k in ('publication_atomic','original_source_replay_verified','point_in_time_verified')):
        raise CaptureError('Unsupported publication/replay authority declaration')
    manifest=strict(retained(s3,bucket,context['manifest']))
    if manifest.get('contract')!='geopolitical-news-acquisition.v1' or manifest.get('compiler_sha256')!=hashes():raise CaptureError('Exact acquisition compiler required')
    if set(manifest['inputs'])!={HEAD,HISTORY,SOVEREIGN} or any(v.get('status') not in ('retained','missing') for v in manifest['inputs'].values()):
        raise CaptureError('Complete native input identities required')
    corpus,countries=configuration()
    if manifest['feeds']!=model.corpus(corpus) or manifest['country_aliases']!=countries:raise CaptureError('Complete configured corpus/country identities differ')
    for capture in manifest['captures']:
        descriptor=strict(retained(s3,bucket,capture['attempt_manifest']))
        if descriptor!={k:v for k,v in capture.items() if k!='attempt_manifest'}:raise CaptureError('Complete HTTP attempts differ')
        for attempt in capture['attempts']:
            if 'original' in attempt:retained(s3,bucket,attempt['original'])
    original_inputs={k:strict(retained(s3,bucket,v['original'])) if v['status']=='retained' else None for k,v in manifest['inputs'].items()}
    expected=model.build(manifest['feeds'],manifest['captures'],manifest['country_aliases'],manifest['generated_at'],original_inputs[SOVEREIGN],lambda c:retained(s3,bucket,c['original']))
    raw=model.encode(expected)
    if raw!=retained(s3,bucket,context['calculation']) or packet!={**expected,'publication_context':context}:raise CaptureError('Complete public calculation differs')
    if context['complete_predecessors']!=manifest['inputs']:raise CaptureError('Predecessor identities differ')
    history=history_projection(original_inputs[HISTORY],expected,context['calculation'],context['manifest'])
    if model.encode(history)!=retained(s3,bucket,context['planned_complete_history']):raise CaptureError('Complete planned history differs')
    return {'status':'complete_original_response_calculation_replayed','feed_acquisitions':len(manifest['captures']),
            'country_rows':len(expected['rankings']),'entries':len(expected['entries']),
            'retained_legacy_history_dates':len(history.get('days',{})),'research_history_runs':len(history['research_runs']),
            'provider_requests':0,'public_writes':0,'point_in_time_verified':False,'forecast_qualified':False}
