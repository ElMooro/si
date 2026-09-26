"""Exact original FRED requests and strict complete-series restoration."""
from datetime import datetime, timezone
from urllib.parse import urlencode
import hashlib, json, re, urllib.request
import sentinel_model as model

MAX=32*1024*1024
PRIVATE='audit-private/20260909-originals/us10y-sentinel-research/'


def capture(client,bucket,request,raw,acquired):
    """Do not redistribute the complete S&P source history through public URLs."""
    digest=hashlib.sha256(raw).hexdigest();identity=hashlib.sha256(request.encode()).hexdigest()
    key=PRIVATE+'originals/'+identity+'/'+digest+'.json'
    try:
        client.put_object(Bucket=bucket,Key=key,Body=raw,IfNoneMatch='*',ContentType='application/json',CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('Original source readback differs')
    return {'contract':'private-source-evidence.v1','key':key,'sha256':digest,'bytes':len(raw),
        'provider':'fred','source_url':request,'acquired_at':acquired,'captured':True,'access':'runner_iam'}


def strict(raw):
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate source JSON key')
            out[key]=value
        return out
    def invalid(value):raise ValueError('Nonfinite source JSON number')
    value=json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid)
    model.encoded(value)
    return value


def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Whole bounded source required')
    return raw


def url(sid,kind):
    if sid not in model.SERIES or kind not in ('definition','observations'):raise ValueError('Unreviewed FRED request')
    query={'series_id':sid,'file_type':'json'}
    if kind=='observations':query.update(observation_start=model.SERIES[sid]['start'],sort_order='asc',limit=100000,units='lin')
    return 'https://api.stlouisfed.org/fred/series'+('/observations' if kind=='observations' else '')+'?'+urlencode(query)


def acquire(client,bucket,key,fetch=urllib.request.urlopen):
    if not isinstance(key,str) or not key:raise ValueError('Existing FRED credential unavailable')
    inputs={}
    # One request per existing series/definition in the normal scheduled run.
    # No retries, substitutions, truncated windows or auxiliary market providers.
    for sid in model.SERIES:
        inputs[sid]={}
        for kind in ('definition','observations'):
            public=url(sid,kind)
            request=urllib.request.Request(public+'&'+urlencode({'api_key':key}),
                headers={'User-Agent':'JustHodl-Sentinel-research/1.0'})
            try:raw=bounded(fetch(request,timeout=25))
            except Exception as exc:raise RuntimeError('FRED acquisition failed: '+sid+' '+kind+' '+type(exc).__name__) from None
            acquired=datetime.now(timezone.utc).isoformat()
            strict(raw)
            ref=capture(client,bucket,public,raw,acquired)
            inputs[sid][kind]={'evidence':ref,'acquired_at':acquired}
    return inputs


def original(sid,kind,item,stamp,read):
    ref=item['evidence'];digest=ref.get('sha256','');request=url(sid,kind)
    expected=PRIVATE+'originals/'+hashlib.sha256(request.encode()).hexdigest()+'/'+digest+'.json'
    if (ref.get('contract')!='private-source-evidence.v1' or ref.get('captured') is not True
        or ref.get('provider')!='fred' or ref.get('source_url')!=request or ref.get('key')!=expected
        or not re.fullmatch('[a-f0-9]{64}',digest) or type(ref.get('bytes')) is not int
        or ref.get('access')!='runner_iam' or not 0<ref['bytes']<=MAX or ref.get('acquired_at')!=item['acquired_at']
        or not model.clock(item['acquired_at'])<=model.clock(stamp)):
        raise ValueError('Exact original FRED request and acquisition identity required')
    raw=read(expected)
    if len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=digest:raise ValueError('Original response bytes differ')
    return strict(raw)


def daily(sid,definition,observations,stamp):
    rows=definition.get('seriess');catalog=model.SERIES[sid]
    if (not isinstance(rows,list) or len(rows)!=1 or rows[0].get('id')!=sid
        or rows[0].get('units')!=catalog['unit'] or rows[0].get('frequency')!=catalog['frequency']
        or rows[0].get('seasonal_adjustment')!='Not Seasonally Adjusted'):
        raise ValueError('Reviewed FRED definition differs: '+sid)
    source=observations.get('observations')
    if (not isinstance(source,list) or type(observations.get('count')) is not int
        or observations['count']!=len(source) or not 0<len(source)<=100000
        or observations.get('offset')!=0 or observations.get('units')!='lin'
        or observations.get('order_by')!='observation_date' or observations.get('sort_order')!='asc'
        or observations.get('output_type')!=1):
        raise ValueError('Complete untransformed ordered FRED response required: '+sid)
    result=[];previous=None;missing=0
    for row in source:
        date=row.get('date');value=row.get('value')
        try:parsed=datetime.strptime(date,'%Y-%m-%d').date()
        except (TypeError,ValueError):raise ValueError('Invalid original observation date') from None
        if (parsed.isoformat()!=date or date<catalog['start'] or parsed>model.clock(stamp).date()
            or (previous is not None and date<=previous)):
            raise ValueError('Repeated, unordered, future or out-of-request original date')
        previous=date
        if value=='.':missing+=1;continue
        if not isinstance(value,str) or not re.fullmatch(r'-?\d+(?:\.\d+)?',value):raise ValueError('Invalid original numeric observation')
        numeric=float(value)
        if not model.math.isfinite(numeric) or (sid=='SP500' and numeric<=0):raise ValueError('Invalid original numeric domain')
        result.append([date,numeric])
    return result,{'original_rows':len(source),'numeric_rows':len(result),'missing_rows':missing,
        'definition':rows[0],'unit':catalog['unit'],'frequency':catalog['frequency'],
        'first_observation':source[0]['date'],'last_observation':source[-1]['date']}


def restore(inputs,stamp,read):
    if set(inputs)!=set(model.SERIES):raise ValueError('All three original source populations required')
    histories={};details={}
    for sid,parts in inputs.items():
        if set(parts)!=set(('definition','observations')):raise ValueError('Complete source definition and observations required')
        definition=original(sid,'definition',parts['definition'],stamp,read)
        observations=original(sid,'observations',parts['observations'],stamp,read)
        histories[sid],audit=daily(sid,definition,observations,stamp)
        details[sid]={**audit,'sources':parts,'vintage_basis':'Current provider vintage acquired at the recorded time; no historical point-in-time claim.'}
    return histories,details
