"""Strict, pure compilation of retained FRED and CFTC responses."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib, json, re
from urllib.parse import urlencode
from zoneinfo import ZoneInfo
import evidence_store
import report_observations

SERIES = {
 'JPNASSETS': ('boj_assets','100 Million Yen','M','BOJ total assets; component attribution is required before calling a change QE or QT.'),
 'IR3TIB01JPM156N': ('jp_rate_3m','Percent','M','Monthly three-month interbank indicator; not the BOJ policy rate or an available borrowing quote.'),
 'IRLTLT01JPM156N': ('jgb_10y','Percent','M','Monthly OECD ten-year government bond yield indicator; not a live bond quote.'),
 'DEXJPUS': ('usdjpy','Japanese Yen to One U.S. Dollar','D','H.10 noon buying rate, JPY per USD; daily observations published weekly, not an executable FX quote.'),
 'DGS2': ('us_2y','Percent','D','Two-year constant-maturity Treasury yield; not an overnight investment return.'),
 'DGS10': ('us_10y','Percent','D','Ten-year constant-maturity Treasury yield; duration and price risk are not carry.'),
 'DFF': ('fed_funds','Percent','D','Effective federal funds rate; not a rate available to this portfolio.'),
}
CFTC_BASE='https://publicreporting.cftc.gov/resource/gpe5-46if.json'
CFTC_URLS={'metadata':'https://publicreporting.cftc.gov/api/views/gpe5-46if.json',
 'count':CFTC_BASE+'?'+urlencode({'cftc_contract_market_code':'097741','$select':'count(*)'}),
 'rows':CFTC_BASE+'?'+urlencode({'cftc_contract_market_code':'097741','$order':'report_date_as_yyyy_mm_dd DESC','$limit':5000})}
CLASSES={
 'dealer':('dealer_positions_long_all','dealer_positions_short_all','dealer_positions_spread_all'),
 'asset_manager':('asset_mgr_positions_long','asset_mgr_positions_short','asset_mgr_positions_spread'),
 'leveraged_funds':('lev_money_positions_long','lev_money_positions_short','lev_money_positions_spread'),
 'other_reportable':('other_rept_positions_long','other_rept_positions_short','other_rept_positions_spread'),
 'nonreportable':('nonrept_positions_long_all','nonrept_positions_short_all',None),
}

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

def amount(value):
    if value is None or value in ('','.'):return None
    if isinstance(value,bool) or len(str(value))>60:raise ValueError('numeric shape')
    try:result=Decimal(str(value))
    except InvalidOperation:raise ValueError('numeric value')
    if not result.is_finite() or abs(result)>Decimal('1e18'):raise ValueError('numeric bound')
    return str(result)

def segments(sid,at):
    today=clock(at).date();start=date(2000,1,1);result=[]
    while start<=today:
        end=min(today,date(start.year+9,12,31)) if SERIES[sid][2]=='D' else today
        result.append((start.isoformat(),end.isoformat()));start=end+timedelta(days=1)
    return result

def fred_urls(sid,at):
    root='https://api.stlouisfed.org/fred/series'
    definition=root+'?'+urlencode({'series_id':sid,'file_type':'json'})
    observations=[root+'/observations?'+urlencode({'series_id':sid,'file_type':'json','units':'lin','sort_order':'desc',
        'limit':4000,'observation_start':start,'observation_end':end}) for start,end in segments(sid,at)]
    return definition,observations

def original(ref,read,expected,at):
    receipt=ref['evidence'];acquired=clock(ref['acquired_at'])
    if ref['url']!=expected or receipt.get('source_url')!=evidence_store.public_source_url(expected):raise ValueError('request identity differs')
    if acquired>clock(at) or clock(receipt['first_received_at'])>acquired:raise ValueError('acquisition clock differs')
    sha=receipt.get('sha256');request_sha=hashlib.sha256(receipt['source_url'].encode()).hexdigest()
    if receipt.get('contract')!='source-evidence.v1' or receipt.get('captured') is not True or receipt.get('provider')!='yen':raise ValueError('receipt contract differs')
    if not isinstance(sha,str) or not re.fullmatch('[a-f0-9]{64}',sha) or receipt['key']!=f'data/evidence/yen/{request_sha}/{sha}.bin.gz':raise ValueError('evidence key differs')
    raw=read(receipt['key'])
    if len(raw)!=receipt['bytes'] or hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('original bytes differ')
    return strict_json(raw)

def quality(sid,day,acquired,at,missing=False):
    age=(clock(at).date()-date.fromisoformat(day)).days
    limit=100 if SERIES[sid][2]=='M' else 7
    out={'observation_date':day,'observation_age_days':age,'acquired_at':acquired,'max_acquisition_age_hours':26,
         'actual_publication_time_verified':False,'holiday_calendar_verified':False,'max_observation_age_days':limit}
    status='incomplete' if missing else 'stale' if age>limit else 'fresh'
    if sid=='DEXJPUS':
        local=clock(at).astimezone(ZoneInfo('America/New_York'));monday=local.date()-timedelta(days=local.weekday())
        if local.weekday()==0 and (local.hour,local.minute)<(16,15):monday-=timedelta(days=7)
        expected=monday-timedelta(days=3)
        status='incomplete' if missing else 'fresh' if day>=expected.isoformat() else 'release_due_unverified'
        out.update(max_observation_age_days=None,nominal_expected_observation_date=expected.isoformat(),
          publication_cadence='Weekly Monday 16:15 America/New_York, previous-week daily observations; holiday exceptions not verified.',
          calendar_source='https://www.federalreserve.gov/releases/h10/about.htm')
    if age<0:raise ValueError('future observed date')
    if (clock(at)-clock(acquired)).total_seconds()>26*3600:status='stale_source'
    out['status']=status
    return out

def fred(sid,refs,read,at):
    definition_url,urls=fred_urls(sid,at)
    if len(refs['observations'])!=len(urls):raise ValueError('FRED segment count differs')
    definition=original(refs['definition'],read,definition_url,at);rows=[];coverage=[];seen=set();measure=None
    for segment,(ref,url) in enumerate(zip(refs['observations'],urls)):
        document=original(ref,read,url,at)
        evidence={'definition':refs['definition']['evidence'],'observations':ref['evidence']}
        measure=report_observations.measurement(sid,definition,document,evidence,at,ref['acquired_at'])
        if (measure['unit'],measure['frequency'])!=SERIES[sid][1:3] or not measure['coverage']['complete_query']:raise ValueError('FRED definition or incomplete segment')
        coverage.append(measure['coverage'])
        for index,item in enumerate(document['observations']):
            d=item['date'];value=amount(item.get('value'))
            if value is not None and ((sid=='DEXJPUS' and Decimal(value)<=0) or (sid=='JPNASSETS' and Decimal(value)<0)):
                raise ValueError('native FX or asset level domain differs')
            if d in seen:raise ValueError('duplicate segment date')
            seen.add(d)
            if d>clock(at).date().isoformat():raise ValueError('future FRED record')
            rows.append({'date':d,'value_decimal':value,'segment':segment,'row_index':index,
                'status':'observed' if value is not None else 'missing',
                'realtime_start':item.get('realtime_start'),'realtime_end':item.get('realtime_end')})
    rows.sort(key=lambda x:x['date']);last=rows[-1]
    acquired=min([refs['definition']['acquired_at'],*[r['acquired_at'] for r in refs['observations']]],key=clock)
    return {'id':sid,'name':measure['name'],'unit':measure['unit'],'frequency':measure['frequency'],'as_of':last['date'],
      'value_decimal':last['value_decimal'],'value':float(last['value_decimal']) if last['value_decimal'] is not None else None,
      'quality':quality(sid,last['date'],acquired,at,last['value_decimal'] is None),'definition':measure['definition'],
      'originals':refs,'rows':rows,'coverage':{'requested_start':'2000-01-01','complete_bounded_query':True,'segments':coverage},
      'limitation':SERIES[sid][3],'vintage':'Current provider vintage retained on acquisition; not historical publication-time knowledge.'}

def count(value):
    if not isinstance(value,(str,int)) or isinstance(value,bool) or not re.fullmatch(r'\d{1,12}',str(value)):raise ValueError('integer contract count required')
    return int(value)

def cftc(refs,read,at):
    docs={k:original(refs[k],read,url,at) for k,url in CFTC_URLS.items()}
    meta=docs['metadata'];records=docs['rows'];reported=docs['count']
    if meta.get('id')!='gpe5-46if' or meta.get('name')!='TFF - Futures Only':raise ValueError('CFTC dataset identity differs')
    columns={c.get('fieldName'):c.get('dataTypeName') for c in meta.get('columns',[])}
    for key in {v for keys in CLASSES.values() for v in keys if v}|{'open_interest_all','tot_rept_positions_long_all','tot_rept_positions_short'}:
        if columns.get(key)!='number':raise ValueError('CFTC numeric schema differs')
    if not isinstance(reported,list) or len(reported)!=1 or not isinstance(records,list) or len(records)!=count(reported[0]['count']) or not 1<=len(records)<5000:raise ValueError('CFTC full-query count differs')
    rows=[];seen=set()
    for index,r in enumerate(records):
        if r.get('cftc_contract_market_code')!='097741' or r.get('futonly_or_combined')!='FutOnly' or r.get('market_and_exchange_names')!='JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE':raise ValueError('CFTC contract population differs')
        if r.get('contract_units') not in ('(CONTRACTS OF JPY 12,500,000)',"'(CONTRACTS OF JPY 12,500,000)'"):raise ValueError('CFTC multiplier differs')
        day=r['report_date_as_yyyy_mm_dd']
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}T00:00:00.000',day):raise ValueError('CFTC report date shape')
        day=day[:10]
        if day in seen or date.fromisoformat(day)>clock(at).date():raise ValueError('duplicate or future CFTC date')
        seen.add(day);oi=count(r['open_interest_all'])
        if not oi:raise ValueError('zero CFTC open interest')
        row={'date':day,'row_index':index,'open_interest':oi,'categories':{},'contract_size_jpy':12500000}
        for name,(long,short,spread) in CLASSES.items():
            a,b=count(r[long]),count(r[short]);c=count(r[spread]) if spread else None
            if max(a,b)>oi:raise ValueError('CFTC position exceeds open interest')
            row['categories'][name]={'long':a,'short':b,'spreading':c,'net_contracts':a-b,'net_pct_oi_decimal':str(Decimal(100)*(a-b)/oi)}
        for side,total in (('long','tot_rept_positions_long_all'),('short','tot_rept_positions_short')):
            summed=sum(v[side]+v['spreading'] for k,v in row['categories'].items() if k!='nonreportable')
            if summed!=count(r[total]) or summed+row['categories']['nonreportable'][side]!=oi:raise ValueError('CFTC category/open-interest reconciliation failed')
        rows.append(row)
    rows.sort(key=lambda r:r['date']);latest=rows[-1];age=(clock(at).date()-date.fromisoformat(latest['date'])).days
    acquired=min((v['acquired_at'] for v in refs.values()),key=clock)
    status='stale' if age>14 else 'stale_source' if (clock(at)-clock(acquired)).total_seconds()>26*3600 else 'fresh'
    baseline=rows[-261:-1];stats={}
    for name in CLASSES:
        values=[Decimal(r['categories'][name]['net_pct_oi_decimal']) for r in baseline]
        current=Decimal(latest['categories'][name]['net_pct_oi_decimal']);sd=None;mean=None
        contiguous=all(4<=(date.fromisoformat(b['date'])-date.fromisoformat(a['date'])).days<=10 for a,b in zip([*baseline,latest],[*baseline,latest][1:]))
        if len(values)==260 and contiguous:
            mean=sum(values)/len(values);sd=(sum((x-mean)**2 for x in values)/(len(values)-1)).sqrt()
        stats[name]={'prior_reports':len(values),'current_excluded':True,'contiguous_weekly_reports':contiguous,
            'baseline_start':baseline[0]['date'] if baseline else None,'baseline_end':baseline[-1]['date'] if baseline else None,
            'z':float((current-mean)/sd) if sd else None,
            'percentile':float(Decimal(100)*(sum(v<current for v in values)+Decimal('.5')*sum(v==current for v in values))/len(values)) if mean is not None else None,
            'interpretation':'Descriptive positioning relative to 260 prior reports, not unwind probability.'}
    return {'id':'CFTC:097741:TFF:FUTURES_ONLY','unit':'contracts','frequency':'W','as_of':latest['date'],'current':latest,
      'quality':{'status':status,'observation_age_days':age,'max_observation_age_days':14,'acquired_at':acquired,
        'actual_publication_time_verified':False,'holiday_calendar_verified':False,'max_acquisition_age_hours':26},
      'rows':rows,'originals':refs,'category_reconciliation':'Every report: each reportable side including spreading plus nonreportable side equals open interest.',
      'statistics':stats,'scope':'CFTC TFF JPY futures only; trader classifications do not establish trading motives. Excludes OTC swaps, bank loans and most global carry positions.',
      'quote_orientation':'Futures are quoted USD per JPY; a long yen future has the opposite FX direction to a long USD/JPY spot position.',
      'whole_carry_trade_size':None,'call':None,'calls_eligible':False,'sizing_eligible':False}
