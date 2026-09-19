"""Pure verification of complete Treasury CSLT originals and matching FRED series."""
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal,InvalidOperation
import hashlib,io,json,re,zipfile
from urllib.parse import urlencode
from zoneinfo import ZoneInfo
import evidence_store,report_observations

CSLT_URL='https://ticdata.treasury.gov/resource-center/data-chart-center/tic/Documents/cslt.zip'
SERIES={
 'FORLTTOTALNET99996':('for_lt_total_net_99996','total','Foreign net transactions of all U.S. long-term securities: Grand Total'),
 'FORLTTREASNET99996':('for_lt_treas_net_99996','treasuries','Foreign net transactions of U.S. long-term Treasury securities: Grand Total'),
 'FORLTAGCYNET99996':('for_lt_agcy_net_99996','agency_bonds','Foreign net transactions of U.S. long-term agency bonds: Grand Total'),
 'FORLTCORPNET99996':('for_lt_corp_net_99996','corporate_bonds','Foreign net transactions of U.S. long-term corporate bonds: Grand Total'),
 'FORLTEQTYNET99996':('for_lt_eqty_net_99996','equities','Foreign net transactions of U.S. equity securities: Grand Total'),
 'FORSTTREASNET99996':('for_st_treas_net_99996','short_treasury','Foreign net transactions of U.S. short-term Treasury securities: Grand Total'),
 'USLTTOTALNET99996':('us_lt_total_net_99996','us_abroad','U.S. net transactions of all foreign long-term securities: Grand Total'),
 'FORLTTOTALNET99990':('for_lt_total_net_99990','official','Foreign net transactions of all U.S. long-term securities: Foreign Official'),
 'FORLTTOTALNET99991':('for_lt_total_net_99991','private','Foreign net transactions of all U.S. long-term securities: Foreign Non-Official'),
}
MAX_ARCHIVE=32*1024*1024
MAX_DOCUMENT=192*1024*1024


def clock(value):
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('timezone required')
    return result.astimezone(timezone.utc)


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(value):return hashlib.sha256(encoded(value)).hexdigest()


def strict_json(raw):
    def pairs(items):
        out={}
        for k,v in items:
            if k in out:raise ValueError('duplicate JSON key')
            out[k]=v
        return out
    def bad(_):raise ValueError('nonfinite JSON')
    return json.loads(raw,object_pairs_hook=pairs,parse_float=str,parse_constant=bad)


def amount(value):
    if value is None or value in ('','.','NA'):return None
    if isinstance(value,bool) or len(str(value))>40:raise ValueError('TIC amount shape')
    try:out=Decimal(str(value))
    except InvalidOperation:raise ValueError('TIC amount invalid')
    if not out.is_finite() or abs(out)>Decimal('1e15'):raise ValueError('TIC amount bound')
    # This dataset reports rounded integer USD millions. Refuse an unnoticed
    # precision/definition change instead of widening reconciliation tolerance.
    if out!=out.to_integral_value():raise ValueError('TIC reporting precision changed')
    return str(int(out))


def endpoint(path,params):return 'https://api.stlouisfed.org/fred/'+path+'?'+urlencode({'file_type':'json',**params})


def definition_url(sid,vintage):
    return endpoint('series',{'series_id':sid,'realtime_start':vintage,'realtime_end':vintage})


def observations_url(sid,meta,vintage):
    date.fromisoformat(meta['observation_start'])
    return endpoint('series/observations',{'series_id':sid,'realtime_start':vintage,'realtime_end':vintage,
        'units':'lin','sort_order':'desc','limit':4000,'observation_start':meta['observation_start'],'observation_end':vintage})


def calendar_urls(at):
    year=clock(at).year
    return {'series_release':endpoint('series/release',{'series_id':'FORLTTOTALNET99996'}),
        'release_dates':endpoint('release/dates',{'release_id':3,'realtime_start':f'{year-1}-01-01','realtime_end':f'{year+1}-12-31',
        'sort_order':'desc','limit':1000,'include_release_dates_with_no_data':'true'})}


def original(ref,read,url,at):
    receipt=ref['evidence'];acquired=clock(ref['acquired_at'])
    if ref['url']!=url or receipt.get('source_url')!=evidence_store.public_source_url(url):raise ValueError('original request identity differs')
    if acquired>clock(at) or clock(receipt['first_received_at'])>acquired:raise ValueError('original acquisition clock differs')
    sha=receipt.get('sha256');request_sha=hashlib.sha256(receipt['source_url'].encode()).hexdigest()
    if receipt.get('contract')!='source-evidence.v1' or receipt.get('captured') is not True or receipt.get('provider')!='tic':raise ValueError('TIC receipt contract differs')
    if not isinstance(sha,str) or not re.fullmatch('[a-f0-9]{64}',sha) or receipt['key']!=f'data/evidence/tic/{request_sha}/{sha}.bin.gz':raise ValueError('TIC evidence identity differs')
    raw=read(receipt['key'])
    if len(raw)!=receipt['bytes'] or hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('TIC original bytes differ')
    return raw


def cslt(ref,read,at):
    raw=original(ref,read,CSLT_URL,at)
    if not 0<len(raw)<=MAX_ARCHIVE:raise ValueError('CSLT compressed bound')
    archive=zipfile.ZipFile(io.BytesIO(raw));members=archive.infolist()
    if len(members)!=1 or members[0].filename!='cslt.json' or members[0].flag_bits&1 or not 0<members[0].file_size<=MAX_DOCUMENT:
        raise ValueError('CSLT archive shape or expanded bound')
    with archive.open(members[0]) as stream:body=stream.read(MAX_DOCUMENT+1)
    if len(body)!=members[0].file_size:raise ValueError('CSLT member length differs')
    document=strict_json(body)
    if document.get('releaseID')!='3' or document.get('version')!='2.0' or not isinstance(document.get('series'),list):raise ValueError('CSLT release/schema differs')
    selected={};seen=set();mapping={values[0]:sid for sid,values in SERIES.items()}
    for series_index,item in enumerate(document['series']):
        identity=item.get('source_id')
        if not isinstance(identity,str) or identity in seen:raise ValueError('duplicate or missing native series identity')
        seen.add(identity)
        if identity not in mapping:continue
        sid=mapping[identity];meta=item['metadata']
        if (meta.get('title'),meta.get('frequency'),meta.get('season'),meta.get('units'))!=(SERIES[sid][2],'M','NSA','Millions of Dollars'):
            raise ValueError('native TIC definition differs')
        if ((meta.get('additional') or {}).get('status'))!='A':raise ValueError('native TIC source inactive')
        rows=[];dates=set()
        for index,pair in enumerate(item['observations']):
            if not isinstance(pair,list) or len(pair)!=2:raise ValueError('native TIC row shape')
            day=date.fromisoformat(pair[0])
            if day.day!=1 or day in dates or day>clock(at).date():raise ValueError('native TIC monthly identity differs')
            dates.add(day);value=amount(pair[1])
            rows.append({'date':day.isoformat(),'value_decimal':value,'row_index':index,'status':'observed' if value is not None else 'missing'})
        if not rows:raise ValueError('native TIC history empty')
        rows.sort(key=lambda row:row['date']);latest=rows[-1]
        selected[sid]={'id':sid,'native_source_id':identity,'name':meta['title'],'series_index':series_index,
            'role':SERIES[sid][1],'unit':'usd_million','source_unit':meta['units'],'frequency':'M','seasonal_adjustment':'Not Seasonally Adjusted',
            'definition':meta,'as_of':latest['date'],'value_decimal':latest['value_decimal'],'rows':rows,'original':ref,
            'coverage':{'first_observation':rows[0]['date'],'last_observation':latest['date'],'observations':len(rows),
                'complete_native_series_array':True,'current_vintage_history':True,'historical_publication_vintages_verified':False}}
    if set(selected)!=set(SERIES):raise ValueError('native TIC core series incomplete')
    return selected,{'release_id':document['releaseID'],'archive_version':document['version'],'transmission_label':document.get('transmissionDt'),
        'transmission_timezone_verified':False,'transmission_is_publication_time':False,'archive_series_count':len(document['series']),
        'core_series_verified':len(selected),'other_series_retained_but_not_qualified':len(document['series'])-len(selected),
        'archive_sha256':ref['evidence']['sha256'],'member_sha256':hashlib.sha256(body).hexdigest(),'member_bytes':len(body)}


def fred(sid,refs,read,at,vintage):
    definition=strict_json(original(refs[sid+':definition'],read,definition_url(sid,vintage),at))
    metadata=definition.get('seriess') or []
    if len(metadata)!=1 or metadata[0].get('id')!=sid:raise ValueError('FRED definition identity differs')
    meta=metadata[0];ref=refs[sid+':observations']
    document=strict_json(original(ref,read,observations_url(sid,meta,vintage),at))
    evidence={'definition':refs[sid+':definition']['evidence'],'observations':ref['evidence']}
    measured=report_observations.measurement(sid,definition,document,evidence,at,ref['acquired_at'])
    if (measured['unit'],measured['frequency'],measured['seasonal_adjustment'])!=('Millions of Dollars','M','Not Seasonally Adjusted') or not measured['coverage']['complete_query']:
        raise ValueError('FRED TIC units or coverage differs')
    if meta.get('title','').casefold()!=SERIES[sid][2].casefold():raise ValueError('FRED TIC title differs')
    rows=[]
    for index,item in enumerate(document['observations']):
        if date.fromisoformat(item['date']).day!=1 or item.get('realtime_start')!=vintage or item.get('realtime_end')!=vintage:
            raise ValueError('FRED monthly/vintage identity differs')
        rows.append({'date':item['date'],'value_decimal':amount(item.get('value')),'row_index':index})
    return {'definition':meta,'coverage':measured['coverage'],'rows':rows,
        'originals':{k:refs[sid+':'+k] for k in ('definition','observations')}}


def release_calendar(refs,read,at):
    urls=calendar_urls(at);docs={k:strict_json(original(refs[k],read,url,at)) for k,url in urls.items()}
    releases=docs['series_release'].get('releases') or []
    if len(releases)!=1 or releases[0].get('id')!=3 or 'Continuous Securities Long Term' not in releases[0].get('name',''):
        raise ValueError('FRED release membership differs')
    doc=docs['release_dates'];rows=doc.get('release_dates') or []
    if type(doc.get('count')) is not int or doc['count']!=len(rows) or doc.get('offset')!=0 or doc.get('limit')!=1000:
        raise ValueError('release calendar query incomplete')
    dates=[]
    for item in rows:
        if item.get('release_id')!=3:raise ValueError('release calendar identity differs')
        day=date.fromisoformat(item['date']).isoformat()
        if day in dates:raise ValueError('duplicate release date')
        dates.append(day)
    today=clock(at).astimezone(ZoneInfo('America/New_York')).date()
    prior=sorted(d for d in dates if d<today.isoformat());future=sorted(d for d in dates if d>=today.isoformat())
    latest=prior[-1] if prior else None;next_release=future[0] if future else None
    expected=None
    if latest:
        d=date.fromisoformat(latest);year,month=divmod(d.year*12+d.month-1-2,12);expected=f'{year:04d}-{month+1:02d}-01'
    expires=(datetime.combine(date.fromisoformat(next_release)+timedelta(days=1),datetime.min.time(),tzinfo=ZoneInfo('America/New_York')).astimezone(timezone.utc).isoformat()) if next_release else None
    return {'source':'FRED release 3 source calendar','dates':sorted(dates),'latest_completed_calendar_date':latest,'next_calendar_date':next_release,
        'nominal_expected_observation_month':expected,'calendar_expires_at':expires,'originals':{k:refs[k] for k in urls},
        'actual_publication_time_verified':False,'observation_mapping_verified':False,
        'rule':'Nominal observation month is release month minus two; current-day releases allowed until the following New York midnight. Calendar dates do not guarantee FRED availability.',
        'acquired_at':min((refs[k]['acquired_at'] for k in urls),key=clock)}


def quality(day,acquired,calendar,at,missing=False,disagreement=False):
    age=(clock(at).date()-date.fromisoformat(day)).days
    status='incomplete' if missing else 'fresh'
    if disagreement:status='source_disagreement'
    if age<0:raise ValueError('future TIC observation')
    if age>100:status='stale'
    expected=(calendar or {}).get('nominal_expected_observation_month')
    expiry=(calendar or {}).get('calendar_expires_at')
    if not expected or not expiry:status='release_calendar_unverified'
    elif day<expected or clock(at)>=clock(expiry):status='release_due_unverified'
    if (clock(at)-clock(acquired)).total_seconds()>26*3600 or (calendar and (clock(at)-clock(calendar['acquired_at'])).total_seconds()>26*3600):
        status='stale_source'
    return {'status':status,'observation_date':day,'observation_age_days':age,'acquired_at':acquired,'max_acquisition_age_hours':26,
        'nominal_expected_observation_month':expected,'calendar_expires_at':expiry,'max_observation_age_days':100,
        'publication_time_verified':False,'source_disagreement':disagreement,'missing_value':missing}
