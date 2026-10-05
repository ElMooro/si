"""Official regional Fed histories, retained by dataset and exact definition."""
from datetime import datetime,timezone
from collections import Counter
from pathlib import Path
import base64,csv,gzip,hashlib,json,re,urllib.request,urllib.error,zipfile,xml.etree.ElementTree as ET
import regional_fed_parser as parser

CONTRACT='regional-fed-reviewed-series.v1'
CATALOGUE_RAW=Path(__file__).with_name('regional-fed-series.json').read_bytes()
CATALOGUE_HASH=hashlib.sha256(CATALOGUE_RAW).hexdigest();CATALOGUE=json.loads(CATALOGUE_RAW)
assert CATALOGUE['contract']=='regional-fed-reviewed-catalogue.v1'
IDENTITIES={v['id'].casefold():v for v in CATALOGUE['series'].values()}
assert len(IDENTITIES)==len(CATALOGUE['series'])
ROOT='data/series-cache/regional-fed-source/';MAX_WIRE=parser.MAX_WIRE;MAX_STORED=MAX_WIRE*2
class SourceUnavailable(ValueError):pass
sha=lambda b:hashlib.sha256(b).hexdigest()
encoded=lambda p:json.dumps(p,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()

def definition(sid):
    if not isinstance(sid,str) or len(sid)>300 or sid.casefold() not in IDENTITIES:raise ValueError('Choose an exact reviewed regional Fed series, comparison and seasonal adjustment')
    return dict(IDENTITIES[sid.casefold()])

def directory(dataset=None,q='',limit=50,offset=0):
    if dataset is not None and dataset not in CATALOGUE['datasets']:raise ValueError('Unknown regional Fed dataset')
    if type(limit) is not int or not 1<=limit<=500 or type(offset) is not int or offset<0:raise ValueError('Invalid regional Fed directory page')
    terms=str(q).casefold().split();rows=[]
    for d in CATALOGUE['series'].values():
        if dataset is not None and d['dataset']!=dataset:continue
        if not all(t in (d['id']+' '+d['name']+' '+d['provider_name']).casefold() for t in terms):continue
        rows.append(dict(id=d['id'],provider='regionalfed',provider_name=d['provider_name'],kind='series',chartable=True,name=d['name'],unit=d['unit'],currency=None,freq='M',first=None,last=None,n=None,live_history_verified=False,contract=CONTRACT,definition_sha256=CATALOGUE_HASH,measurement_kind=d['measurement_kind']))
    return dict(provider='regionalfed',rows=rows[offset:offset+limit],total=len(rows),limit=limit,offset=offset,contract=CONTRACT,catalogue_scope='Exact source definitions; directory is not historical-coverage proof')

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):raise urllib.error.HTTPError(req.full_url,code,'Source redirect not followed',headers,fp)

def allowed_url(url):
    return url in {d['url'] for d in CATALOGUE['datasets'].values()} or re.fullmatch(r'https://www\.kansascityfed\.org/documents/[0-9]+/[A-Za-z0-9_.-]+\.xlsx',url or '') is not None

def read_http(url):
    if not allowed_url(url):raise ValueError('Unreviewed regional Fed source URL')
    with urllib.request.build_opener(NoRedirect()).open(urllib.request.Request(url,headers={'User-Agent':'JustHodl-official-series/1.0 (+https://justhodl.ai)'}),timeout=40) as response:
        raw=response.read(MAX_WIRE+1)
        if response.status!=200 or len(raw)>MAX_WIRE:raise ValueError('Regional Fed response status or size invalid')
        return raw,{k:v for k,v in response.headers.items() if k.lower() in ('content-type','etag','last-modified')}

def _get(store,bucket,key,bound):
    try:response=store.get_object(Bucket=bucket,Key=key)
    except Exception as exc:
        if getattr(exc,'response',{}).get('Error',{}).get('Code') in ('NoSuchKey','404','NotFound'):return None
        raise SourceUnavailable('Regional Fed shared cache unavailable; no source request') from exc
    body=response['Body']
    try:raw=body.read(bound+1)
    finally:body.close()
    if len(raw)>bound:raise SourceUnavailable('Regional Fed retained object exceeds bounds')
    return raw

def _put(store,bucket,key,raw,**kw):
    try:store.put_object(Bucket=bucket,Key=key,Body=raw,**kw)
    except Exception as exc:raise SourceUnavailable('Regional Fed source retention failed; no unretained chart') from exc

def namespace(dataset):return ROOT+dataset+'/'
def dataset_hash(dataset):return sha(encoded(CATALOGUE['datasets'][dataset]))

def _parse(dataset,blobs):
    if dataset=='chicago-cfnai':
        if len(blobs)!=1:raise ValueError('CFNAI requires one complete source table')
        return parser.parse_cfnai(blobs[0])
    if dataset=='kc-manufacturing':
        if len(blobs)!=2:raise ValueError('Kansas City requires source discovery page and workbook')
        parser.kc_workbook_url(blobs[0]);return parser.parse_kc(blobs[1])
    raise ValueError('Unreviewed regional Fed dataset')

def parse(dataset,blobs):
    try:return _parse(dataset,blobs)
    except (csv.Error,zipfile.BadZipFile,ET.ParseError) as exc:raise ValueError('Regional Fed table format invalid ('+type(exc).__name__+')') from exc

def validate_receipts(dataset,receipts,blobs):
    ds=CATALOGUE['datasets'][dataset]
    urls=[ds['url']]+([parser.kc_workbook_url(blobs[0])] if dataset=='kc-manufacturing' and blobs else [])
    if len(receipts)!=len(blobs) or len(blobs)!=len(urls):raise ValueError('Source receipt collection incomplete')
    for receipt,raw,url in zip(receipts,blobs,urls):
        digest=sha(raw);key=namespace(dataset)+'responses/'+digest+'.json'
        if (receipt.get('contract')!=CONTRACT or receipt.get('dataset')!=dataset or receipt.get('dataset_definition_sha256')!=dataset_hash(dataset)
            or receipt.get('url')!=url or receipt.get('http_status')!=200 or receipt.get('sha256')!=digest or receipt.get('bytes')!=len(raw)
            or receipt.get('retained_key')!=key or receipt.get('retained_url')!='https://justhodl.ai/'+key or receipt.get('complete_response_retained') is not True):raise ValueError('Regional Fed source receipt failed integrity checks')
        clock=datetime.fromisoformat(receipt['received_at'])
        if clock.tzinfo is None:raise ValueError('Regional Fed receipt timezone required')

def retained_blob(raw,receipt):
    packet=json.loads(raw)
    if packet.get('body_encoding')!='base64' or packet.get('sha256')!=receipt['sha256'] or packet.get('bytes')!=receipt['bytes']:raise ValueError('Retained original envelope invalid')
    body=base64.b64decode(packet['body_base64'],validate=True)
    if len(body)>MAX_WIRE or len(body)!=receipt['bytes'] or sha(body)!=receipt['sha256']:raise ValueError('Retained original bytes failed integrity checks')
    return body

def snapshot(dataset,store,bucket,reader=None,now=None):
    ds=CATALOGUE['datasets'][dataset];root=namespace(dataset);now=now or datetime.now(timezone.utc)
    if now.tzinfo is None:raise ValueError('Acquisition timezone required')
    if _get(store,bucket,root+'blocked.json',32000) is not None:raise SourceUnavailable('Regional Fed dataset stopped after source refusal; access review required')
    current=_get(store,bucket,root+'current.json',64000)
    if current is not None:
        manifest=json.loads(current)
        if manifest.get('contract')!=CONTRACT or manifest.get('dataset')!=dataset or manifest.get('dataset_definition_sha256')!=dataset_hash(dataset):raise SourceUnavailable('Regional Fed retained dataset definition differs; review required')
        receipts=manifest['source_receipts'];blobs=[]
        if not isinstance(receipts,list) or not 1<=len(receipts)<=2:raise ValueError('Retained receipt count invalid')
        for r in receipts:
            digest=r.get('sha256','')
            if not re.fullmatch('[0-9a-f]{64}',digest):raise ValueError('Retained digest invalid')
            raw=_get(store,bucket,root+'responses/'+digest+'.json',MAX_STORED)
            if raw is None:raise SourceUnavailable('Retained original absent')
            blobs.append(retained_blob(raw,r))
        validate_receipts(dataset,receipts,blobs);acquired=datetime.fromisoformat(manifest['acquired_at'])
        if acquired.tzinfo is None or any(r['received_at']!=manifest['acquired_at'] for r in receipts):raise ValueError('Retained manifest clock must match its source receipts')
        age=(now-acquired).total_seconds()
        if age<0:raise SourceUnavailable('Regional Fed receipt is in the future')
        if age<ds['max_age_s']:return blobs,receipts,manifest['acquired_at']
    claim=root+'request-claims/'+now.date().isoformat()+'.json'
    try:store.put_object(Bucket=bucket,Key=claim,Body=encoded({'dataset':dataset,'requested_at':now.isoformat()}),ContentType='application/json',IfNoneMatch='*')
    except Exception as exc:raise SourceUnavailable('Regional Fed dataset request already claimed or admission unavailable; no repeat request') from exc
    blobs=[];receipts=[];urls=[ds['url']]
    try:
        for url in urls:
            if not allowed_url(url):raise ValueError('Unreviewed source URL')
            raw,headers=(reader or read_http)(url)
            if not isinstance(raw,bytes) or len(raw)>MAX_WIRE:raise ValueError('Source response exceeds bound')
            blobs.append(raw)
            if dataset=='kc-manufacturing' and len(blobs)==1:urls.append(parser.kc_workbook_url(raw))
            digest=sha(raw);key=root+'responses/'+digest+'.json'
            receipts.append({'contract':CONTRACT,'dataset':dataset,'dataset_definition_sha256':dataset_hash(dataset),'url':url,'http_status':200,'received_at':now.isoformat(),'sha256':digest,'bytes':len(raw),'retained_key':key,'retained_url':'https://justhodl.ai/'+key,'complete_response_retained':True,'headers':{k:v for k,v in headers.items() if k.lower() in ('content-type','etag','last-modified')}})
        parse(dataset,blobs);validate_receipts(dataset,receipts,blobs)
        for r,raw in zip(receipts,blobs):
            wrapper=encoded({'body_encoding':'base64','body_base64':base64.b64encode(raw).decode(),'sha256':r['sha256'],'bytes':len(raw)})
            _put(store,bucket,r['retained_key'],wrapper,ContentType='application/json',CacheControl='public, max-age=31536000, immutable')
        manifest={'contract':CONTRACT,'dataset':dataset,'dataset_definition_sha256':dataset_hash(dataset),'source_receipts':receipts,'acquired_at':now.isoformat()}
        _put(store,bucket,root+'current.json',encoded(manifest),ContentType='application/json',CacheControl='no-cache')
        return blobs,receipts,now.isoformat()
    except urllib.error.HTTPError as exc:
        _put(store,bucket,root+'blocked.json',encoded({'dataset':dataset,'http_status':exc.code,'received_at':now.isoformat(),'retry_allowed':False}),ContentType='application/json',CacheControl='no-cache')
        raise SourceUnavailable('Regional Fed HTTP '+str(exc.code)+'; no retry or alternate source') from exc
    except (ValueError,OSError,KeyError) as exc:raise SourceUnavailable('Regional Fed source validation/acquisition failed ('+type(exc).__name__+'); shared daily claim retained') from exc

def packet(sid,blobs,receipts,acquired):
    d=definition(sid);dataset=d['dataset'];validate_receipts(dataset,receipts,blobs);records=parse(dataset,blobs)[d['source_key']]
    clock=datetime.fromisoformat(acquired)
    if clock.tzinfo is None or any(r['received_at']!=acquired for r in receipts):raise ValueError('Acquisition clock must match source receipts')
    month=clock.astimezone(timezone.utc).strftime('%Y-%m')
    for r in records:
        if r['period']>month:r.update(value=None,rejection='future_reference_month_at_receipt')
    records.sort(key=lambda r:r['period']);obs=[[r['anchor'],r['value']] for r in records];n=sum(v is not None for _,v in obs);rejected=sum(r['rejection'] is not None for r in records);extract=encoded(records)
    return {'contract':CONTRACT,'id':d['id'],'requested_id':sid,'provider':'regionalfed','provider_name':d['provider_name'],'name':d['name'],'unit':d['unit'],'currency':None,'freq':'M','definition':d,'definition_sha256':CATALOGUE_HASH,'dataset_definition':CATALOGUE['datasets'][dataset],'dataset_definition_sha256':dataset_hash(dataset),'source':receipts[-1]['url'],'source_receipts':receipts,'acquired_at':acquired,'source_published_at':None,'obs':obs,'n':n,'first':obs[0][0] if obs else None,'last':obs[-1][0] if obs else None,'last_valid':next((date for date,value in reversed(obs) if value is not None),None),
        'source_extract':{'scope':'Every original selected-series cell and reference period, including missingness and formula-cache flags; complete source response envelopes at retained receipt URLs','sha256':sha(extract),'bytes':len(extract),'body_encoding':'gzip+base64','body_base64':base64.b64encode(gzip.compress(extract,mtime=0)).decode()},
        'measurement_evidence':{'columns':['source_row','original_reference_month','chart_anchor','value','original_numeric_lexeme','rejection','binary64_rounding'],'rows':[[r['ordinal'],r['period'],r['anchor'],r['value'],r['original_value'],r['rejection'],r['binary64_rounding']] for r in records]},
        'quality':{'status':'unavailable' if not n else 'partial' if rejected else 'observations','error':None,'received_rows':len(records),'rejected_rows':rejected,'plot_rounding_rows':sum(r['binary64_rounding'] for r in records),'cached_formula_rows':sum(r['cached_formula_value'] for r in records),'source_flag_counts':dict(Counter(flag for r in records for flag in r['source_flags']))},
        'history':{'response_complete':True,'full_upstream_history_verified':False,'point_in_time_vintages_verified':False,'latest_reference_month_verified':False,'release_clock_verified':False,'missing_dates_filled':False,'market_ohlc_qualified':False,'traded_volume_qualified':False,'intraday_quote':False,'period_precision':'month','chart_anchor':'first day of reference month; not publication time','measurement_kind':d['measurement_kind'],'interpretation':d['interpretation'],'vintage':d['vintage']},'equivalence_to_watchlist_provider_verified':False,'calls_eligible':False,'sizing_eligible':False}

def fetch(sid,store,bucket,reader=None,now=None):
    d=definition(sid)
    try:
        blobs,receipts,acquired=snapshot(d['dataset'],store,bucket,reader,now);out=packet(sid,blobs,receipts,acquired)
    except (SourceUnavailable,ValueError,KeyError,TypeError,UnicodeError) as exc:
        out={'contract':CONTRACT,'id':d['id'],'requested_id':sid,'provider':'regionalfed','provider_name':d['provider_name'],'name':d['name'],'unit':d['unit'],'currency':None,'freq':'M','definition':d,'definition_sha256':CATALOGUE_HASH,'source':CATALOGUE['datasets'][d['dataset']]['source_page'],'source_receipts':[],'obs':[],'n':0,'first':None,'last':None,'last_valid':None,'acquired_at':None,'quality':{'status':'unavailable','error':str(exc)[:200],'received_rows':0,'rejected_rows':0},'history':{'response_complete':False,'full_upstream_history_verified':False,'point_in_time_vintages_verified':False,'market_ohlc_qualified':False,'traded_volume_qualified':False,'intraday_quote':False},'equivalence_to_watchlist_provider_verified':False,'calls_eligible':False,'sizing_eligible':False}
    if len(encoded(out))>3900000:raise ValueError('Regional Fed evidence exceeds response budget; no partial packet returned')
    return out

def cache_valid(value,sid):
    try:d=definition(sid)
    except ValueError:return False
    return isinstance(value,dict) and value.get('contract')==CONTRACT and value.get('id')==d['id'] and value.get('definition_sha256')==CATALOGUE_HASH and value.get('definition')==d and value.get('history',{}).get('response_complete') is True and not value.get('quality',{}).get('error')
