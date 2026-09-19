"""Verify original CSLT identities, complete rows and the separate Treasury holdings table."""
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal,InvalidOperation
from zoneinfo import ZoneInfo
import csv,hashlib,io,json,re,zipfile
from urllib.parse import urlencode
import evidence_store

CSLT_URL='https://ticdata.treasury.gov/resource-center/data-chart-center/tic/Documents/cslt.zip'
TABLE_URL='https://ticdata.treasury.gov/resource-center/data-chart-center/tic/Documents/slt_table5.txt'
MSPD_URL='https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/debt/mspd/mspd_table_1?'+urlencode({
    'fields':'record_date,security_type_desc,security_class_desc,debt_held_public_mil_amt',
    'filter':'security_type_desc:eq:Marketable','sort':'record_date','page[size]':10000,'page[number]':1})
AUCTION_URLS={kind:'https://www.treasurydirect.gov/TA_WS/securities/auctioned?format=json&days=60&type='+name for kind,name in (('auction_note','Note'),('auction_bond','Bond'))}
MAX_ARCHIVE=32*1024*1024
MAX_DOCUMENT=192*1024*1024
FAMILIES={'lt_total':'all U.S. long-term securities','lt_treas':'U.S. long-term Treasury securities',
    'lt_eqty':'U.S. equity securities','lt_agcy':'U.S. long-term agency bonds','lt_corp':'U.S. long-term corporate bonds',
    'st_treas':'U.S. short-term Treasury securities','treas':'U.S. long-term and short-term Treasury securities'}
MEASURES={'pos':'Foreign portfolio holdings of ','net':'Foreign net transactions of ',
    'valchg':'Valuation change on foreign portfolio holdings of '}
AGGREGATES={'16713','16721','19992','34401','39942','39993','49999','59994','69906','69995','72907','76929','79995','99990','99991','99996'}
HISTORICAL_GROUPS={'10308','13056','13059','18007','33596','36005','39001','39004','39101','46612','48909','57215','58904','63908','88862'}
ISSUER_GROUPS={'82643','82644','82645','82651','82652','82653','82678','82679','82680','82686','82687','82688','84018','84019','84020','89991','89992','89993'}
REVIEWED_COUNTRIES=set('10189 10251 10405 10502 10707 10804 11002 11207 11304 11401 11509 11703 11819 12009 12106 12203 12319 12505 12602 12688 12807 13005 13006 13007 13008 13218 14214 14338 15202 15288 15504 15601 15709 15768 15806 16101 16403 16543 29998 30104 30155 30228 30309 30406 30503 30589 30708 30805 31003 31089 31208 31607 31704 31887 32204 32409 32603 32719 35254 35319 35602 35718 35807 36137 36188 37206 37303 37508 37818 40703 41319 41408 42005 42102 42218 42404 42501 42609 43001 43109 43419 43605 44105 44709 44806 45101 45608 46019 46205 46302 46418 46604 46906 50105 50504 51705 52418 52604 53201 53805 54003 54305 55719 57002 60089 61204 61689'.split())


def archive(ref,read,at):
    raw=original(ref,read,CSLT_URL,at)
    if not 0<len(raw)<=MAX_ARCHIVE:raise ValueError('CSLT compressed bound')
    with zipfile.ZipFile(io.BytesIO(raw)) as zipped:
        info=zipped.infolist()
        if len(info)!=1 or info[0].filename!='cslt.json' or info[0].flag_bits&1 or not 0<info[0].file_size<=MAX_DOCUMENT:raise ValueError('CSLT archive shape')
        with zipped.open(info[0]) as stream:body=stream.read(MAX_DOCUMENT+1)
        if len(body)!=info[0].file_size:raise ValueError('CSLT expanded length')
    doc=strict_json(body)
    if doc.get('releaseID')!='3' or doc.get('version')!='2.0' or not isinstance(doc.get('series'),list):raise ValueError('CSLT schema')
    seen=set();geographies={};selected=0
    for index,item in enumerate(doc['series']):
        sid=item.get('source_id')
        if not isinstance(sid,str) or sid in seen:raise ValueError('CSLT series identity')
        seen.add(sid);match=re.fullmatch(r'for_(lt_total|lt_treas|lt_eqty|lt_agcy|lt_corp|st_treas|treas)_(pos|net|valchg)_(\d{5})',sid)
        if not match:continue
        family,measure,code=match.groups();meta=item['metadata'];extra=meta.get('additional') or {};geo=extra.get('geography') or {};name=geo.get('name');state=extra.get('status')
        if not isinstance(name,str) or not name or state not in ('A','D'):raise ValueError('CSLT geography/status')
        expected=MEASURES[measure]+FAMILIES[family]+': '+name+(' (DISCONTINUED)' if state=='D' else '')
        if (meta.get('title'),meta.get('frequency'),meta.get('season'),meta.get('units'))!=(expected,'M','NSA','Millions of Dollars'):raise ValueError('CSLT definition differs: '+sid)
        rows=[];dates=set()
        for row_index,pair in enumerate(item['observations']):
            if not isinstance(pair,list) or len(pair)!=2:raise ValueError('CSLT row shape')
            day=date.fromisoformat(pair[0])
            if day.day!=1 or day in dates or day>clock(at).date():raise ValueError('CSLT monthly row identity')
            dates.add(day);value=amount(pair[1]);rows.append({'date':day.isoformat(),'value_decimal':value,'row_index':row_index,'status':'missing' if value is None else 'observed'})
        if not rows:raise ValueError('CSLT series empty')
        rows.sort(key=lambda v:v['date'])
        scope='reported_aggregate' if code in AGGREGATES else 'historical_group' if code in HISTORICAL_GROUPS else 'issuer_or_instrument_group' if code in ISSUER_GROUPS else 'reported_country_or_territory' if code in REVIEWED_COUNTRIES else 'unreviewed_group'
        area=geographies.setdefault(code,{'code':code,'name':name,'scope':scope,'series':{}})
        if area['name']!=name:raise ValueError('CSLT geography name conflict')
        key=family+':'+measure
        if key in area['series']:raise ValueError('CSLT family duplicate')
        area['series'][key]={'id':sid.replace('_','').upper(),'native_source_id':sid,'series_index':index,'family':family,'measure':measure,
            'source_status':state,'definition':meta,'unit':'usd_million','original':ref,'rows':rows,
            'coverage':{'first_observation':rows[0]['date'],'last_observation':rows[-1]['date'],'observations':len(rows),'complete_native_series_array':True,'historical_publication_vintages_verified':False}}
        selected+=1
    if '99996' not in geographies or 'lt_total:net' not in geographies['99996']['series']:raise ValueError('CSLT grand total absent')
    return geographies,{'archive_series_count':len(seen),'verified_foreign_series':selected,'reported_areas_and_groups':len(geographies),
        'archive_sha256':ref['evidence']['sha256'],'member_sha256':hashlib.sha256(body).hexdigest(),'member_bytes':len(body),
        'transmission_label':doc.get('transmissionDt'),'transmission_is_publication_time':False,'unselected_series_retained':len(seen)-selected}


def holdings_table(ref,read,at):
    raw=original(ref,read,TABLE_URL,at)
    if not 0<len(raw)<=2*1024*1024:raise ValueError('Table5 size')
    rows=list(csv.reader(io.StringIO(raw.decode('utf-8-sig')),delimiter='\t'))
    if not rows or rows[0][0].strip()!='Table 5: Major Foreign Holders of Treasury Securities' or not any(r and r[0].strip()=='Billions of dollars' for r in rows[:6]):raise ValueError('Table5 definition')
    header=[i for i,r in enumerate(rows) if r and r[0].strip()=='Country']
    if len(header)!=1:raise ValueError('Table5 header')
    start=header[0];months=[s.strip() for s in rows[start][1:]]
    if not months or len(set(months))!=len(months) or any(not re.fullmatch(r'\d{4}-\d{2}',m) or date.fromisoformat(m+'-01')>clock(at).date() for m in months):raise ValueError('Table5 dates')
    if months!=sorted(months,reverse=True):raise ValueError('Table5 period order')
    reported={}
    for index,row in enumerate(rows[start+1:],start+1):
        if not row or not row[0].strip():break
        name=row[0].strip()
        if len(row)!=len(months)+1 or name in reported:raise ValueError('Table5 row identity')
        values={}
        for month,text in zip(months,row[1:]):
            text=text.strip()
            if text in ('','n.a.','NA','n.a','--'):value=None
            else:
                if not re.fullmatch(r'-?\d+\.\d',text):raise ValueError('Table5 reporting precision')
                value=str(Decimal(text)*1000)
            values[month+'-01']=value
        reported[name]={'name':name,'values_usd_million_decimal':values,'source_row_index':index}
    if 'Grand Total' not in reported or len(reported)<10:raise ValueError('Table5 coverage')
    return {'name':'Treasury Table 5 major foreign holdings, all maturities','unit':'usd_bn','source_resolution_usd_million':100,
        'months':[m+'-01' for m in months],'rows':reported,'original':ref,'custodial_attribution_is_not_ultimate_ownership':True,
        'scope':'Reported foreign holdings of marketable and nonmarketable Treasury bills, bonds and notes; monthly SLT and BL2 sources.'}


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
    period=date.fromisoformat(day);year,month=divmod(period.year*12+period.month,12)
    period_end=date(year,month+1,1)-timedelta(days=1)
    age=(clock(at).date()-period_end).days
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
    return {'status':status,'observation_date':day,'observation_period_end':period_end.isoformat(),'date_represents':'monthly_period_label',
        'observation_age_days':age,'acquired_at':acquired,'max_acquisition_age_hours':26,
        'nominal_expected_observation_month':expected,'calendar_expires_at':expiry,'max_observation_age_days':100,
        'publication_time_verified':False,'source_disagreement':disagreement,'missing_value':missing}
