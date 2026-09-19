"""Original-response verification and native rate histories for carry research."""
import calendar,csv,hashlib,io,json,re
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal
from urllib.parse import urlencode
from zoneinfo import ZoneInfo
import evidence_store,report_observations
import carry_catalog as catalog
from carry_equity import decimal,day,compile_equity

def clock(value):
    parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:raise ValueError('timezone required')
    return parsed.astimezone(timezone.utc)

def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(value):return hashlib.sha256(encoded(value)).hexdigest()

def strict_json(raw):
    def pairs(items):
        result={}
        for k,v in items:
            if k in result:raise ValueError('duplicate JSON key')
            result[k]=v
        return result
    def bad(_):raise ValueError('nonfinite JSON')
    return json.loads(raw,object_pairs_hook=pairs,parse_float=str,parse_constant=bad)

def original(ref,read,expected,at,as_json=True):
    receipt=ref['evidence'];acquired=clock(ref['acquired_at'])
    if ref['url']!=expected or receipt.get('source_url')!=evidence_store.public_source_url(expected):raise ValueError('request identity differs')
    if acquired>clock(at) or clock(receipt['first_received_at'])>acquired:raise ValueError('acquisition clock differs')
    sha=receipt.get('sha256');request_sha=hashlib.sha256(receipt['source_url'].encode()).hexdigest()
    if receipt.get('contract')!='source-evidence.v1' or receipt.get('captured') is not True or receipt.get('provider')!='carry':raise ValueError('receipt contract differs')
    if not isinstance(sha,str) or not re.fullmatch('[a-f0-9]{64}',sha) or receipt['key']!=f'data/evidence/carry/{request_sha}/{sha}.bin.gz':raise ValueError('evidence key differs')
    raw=read(receipt['key'])
    if len(raw)!=receipt['bytes'] or hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('original bytes differ')
    return strict_json(raw) if as_json else raw

def definition_url(sid):return 'https://api.stlouisfed.org/fred/series?'+urlencode({'series_id':sid,'file_type':'json'})

def definition(sid,doc):
    rows=doc.get('seriess',[])
    if len(rows)!=1 or rows[0].get('id')!=sid:raise ValueError('FRED definition identity differs')
    meta=rows[0];expected=catalog.FRED[sid]
    if (meta.get('units'),meta.get('frequency_short'))!=(expected['unit'],expected['frequency']):raise ValueError('FRED unit or frequency differs')
    day(meta['observation_start']);day(meta['observation_end'])
    return meta

def observation_urls(sid,meta,at):
    # ICE's rolling licensed window is honored; do not query imaginary earlier data.
    start=max(date(2000,1,1),day(meta['observation_start']));end=min(clock(at).date(),day(meta['observation_end']));out=[]
    while start<=end:
        stop=min(end,date(start.year+9,12,31)) if catalog.FRED[sid]['frequency']=='D' else end
        out.append('https://api.stlouisfed.org/fred/series/observations?'+urlencode({'series_id':sid,'file_type':'json','units':'lin',
          'sort_order':'desc','limit':4000,'observation_start':start.isoformat(),'observation_end':stop.isoformat()}))
        start=stop+timedelta(days=1)
    return out

def quality(observed,acquired,at,frequency,missing=False):
    age=(clock(at).date()-day(observed)).days;limit=100 if frequency=='M' else 7
    if age<0:raise ValueError('future observation')
    status='incomplete' if missing else 'stale' if age>limit else 'fresh'
    if (clock(at)-clock(acquired)).total_seconds()>26*3600:status='stale_source'
    return {'status':status,'observation_date':observed,'observation_age_days':age,'max_observation_age_days':limit,
      'acquired_at':acquired,'max_acquisition_age_hours':26,'actual_publication_time_verified':False,'holiday_calendar_verified':False}

def fred(sid,refs,read,at):
    doc=original(refs['definition'],read,definition_url(sid),at);meta=definition(sid,doc)
    urls=observation_urls(sid,meta,at)
    if not urls or len(refs['observations'])!=len(urls):raise ValueError('FRED segment count differs')
    rows=[];seen=set();coverage=[]
    for segment,(ref,url) in enumerate(zip(refs['observations'],urls)):
        data=original(ref,read,url,at)
        measured=report_observations.measurement(sid,doc,data,{'definition':refs['definition']['evidence'],'observations':ref['evidence']},at,ref['acquired_at'])
        if not measured['coverage']['complete_query']:raise ValueError('truncated FRED query')
        coverage.append(measured['coverage'])
        for index,row in enumerate(data['observations']):
            d=day(row['date']).isoformat();value=row.get('value')
            value=None if value in (None,'','.') else str(decimal(value))
            if d in seen or d>clock(at).date().isoformat():raise ValueError('duplicate or future FRED date')
            seen.add(d);rows.append({'date':d,'value_decimal':value,'segment':segment,'row_index':index,
              'status':'missing' if value is None else 'observed','realtime_start':row.get('realtime_start'),'realtime_end':row.get('realtime_end')})
    rows.sort(key=lambda r:r['date']);last=rows[-1]
    acquired=min([refs['definition']['acquired_at'],*[r['acquired_at'] for r in refs['observations']]],key=clock)
    return {'id':sid,'name':meta['title'],'unit':meta['units'],'frequency':meta['frequency_short'],'kind':catalog.FRED[sid]['kind'],
      'as_of':last['date'],'value_decimal':last['value_decimal'],'definition':meta,'quality':quality(last['date'],acquired,at,meta['frequency_short'],last['value_decimal'] is None),
      'rows':rows,'originals':refs,'coverage':{'complete_bounded_queries':True,'segments':coverage,'official_available_start':meta['observation_start']},
      'vintage':'Current provider vintage retained on acquisition; not historical publication-time knowledge.'}

def ecb(ref,read,at):
    raw=original(ref,read,catalog.ECB_EURIBOR_URL,at,False);table=list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))))
    if not 1<=len(table)<=1000:raise ValueError('ECB monthly record bound')
    expected={'KEY':catalog.ECB_EURIBOR_ID,'FREQ':'M','REF_AREA':'U2','CURRENCY':'EUR','PROVIDER_FM':'RT','INSTRUMENT_FM':'MM',
      'PROVIDER_FM_ID':'EURIBOR3MD_','DATA_TYPE_FM':'HSTA','UNIT':'PCPA','UNIT_MULT':'0','COLLECTION':'A'}
    rows=[];seen=set()
    for index,r in enumerate(table):
        if any(r.get(k)!=v for k,v in expected.items()):raise ValueError('ECB series definition differs')
        if not re.fullmatch(r'\d{4}-\d{2}',r['TIME_PERIOD']):raise ValueError('ECB monthly period differs')
        d=day(r['TIME_PERIOD']+'-01').isoformat()
        if d in seen or d>clock(at).date().isoformat():raise ValueError('duplicate or future ECB date')
        seen.add(d);value=None if not r['OBS_VALUE'] else str(decimal(r['OBS_VALUE']))
        rows.append({'date':d,'value_decimal':value,'row_index':index,'status':r['OBS_STATUS'],'confidentiality':r['OBS_CONF']})
    rows.sort(key=lambda r:r['date']);last=rows[-1]
    return {'id':catalog.ECB_EURIBOR_ID,'name':table[0]['TITLE_COMPL'],'unit':'Percent','frequency':'M','kind':'interbank_three_month_monthly',
      'as_of':last['date'],'value_decimal':last['value_decimal'],'quality':quality(last['date'],ref['acquired_at'],at,'M',last['value_decimal'] is None or last['status']!='A'),
      'rows':rows,'originals':{'series':ref},'definition':expected,'vintage':'Current ECB CSV vintage; source is Refinitiv via ECB; no historical publication-time archive.',
      'coverage':{'returned':len(rows),'query_scope':'All records returned for the exact ECB series; no independent total-count endpoint.'}}

def fmp_url(symbol,path):return 'https://financialmodelingprep.com/stable/'+path+'?'+urlencode({'symbol':symbol})

def completed_cutoff(at):
    # Conservative: use the preceding New York calendar date, including during today's session.
    # This deliberately lags an after-close refresh rather than assume a session has finalized.
    return (clock(at).astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)).isoformat()

def expected_price_weekday(at):
    cutoff=day(completed_cutoff(at))
    while cutoff.weekday()>=5:cutoff-=timedelta(days=1)
    return cutoff.isoformat()

def equity(symbol,refs,read,at):
    if set(refs)!=set(catalog.FMP_PATHS):raise ValueError('complete reviewed FMP endpoint set required')
    docs={path:original(refs[path],read,fmp_url(symbol,path),at) for path in catalog.FMP_PATHS}
    item=compile_equity(symbol,docs,completed_cutoff(at));expected=catalog.EQUITIES[symbol]
    if item['identity']['isEtf']!=(expected['expected_type']=='etf'):raise ValueError('catalog security type differs')
    acquired=min((r['acquired_at'] for r in refs.values()),key=clock)
    item.update(id='FMP:'+symbol,name=symbol,unit='USD_per_current_share',frequency='D',originals=refs,
      quality=quality(item['as_of'],acquired,at,'D',item['price_adjustment_status']!='split_reconciled'))
    item['quality']['nominal_expected_price_date']=expected_price_weekday(at)
    item['quality']['price_cadence']='Prior New York weekday close; exchange-holiday exceptions are not verified.'
    if item['quality']['status']=='fresh' and item['as_of']<expected_price_weekday(at):item['quality']['status']='release_due_unverified'
    item['price_cutoff']={'through':completed_cutoff(at),'basis':'Preceding New York calendar date; not an exchange-holiday calendar or intraday quote.'}
    return item
