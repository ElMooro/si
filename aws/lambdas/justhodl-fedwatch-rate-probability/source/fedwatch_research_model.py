"""Source-defined federal-funds futures observations; no inferred policy certainty.

Retained Yahoo chart rows are dated provider bars, not official settlements or
known completed exchange sessions. Calendar end dates are not effective dates.
"""
from calendar import month_name
from collections import Counter
from datetime import date,datetime,time,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
from html.parser import HTMLParser
from zoneinfo import ZoneInfo
import hashlib,json,math,re
import report_observations
from research_brief_model import clock,row_status,AGE_LIMITS

CONTRACT='fedwatch-native-research.v1';PREFIX='data/fedwatch-research/';CURRENT='data/fedwatch.json'
PRIVATE='audit-private/20260909-originals/fedwatch-research/'
SERIES=('DFEDTARU','DFEDTARL','DFF','FEDFUNDS')
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
CALENDAR_URL='https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
MONTH_CODES='FGHJKMNQUVXZ';MAX_SOURCE_BYTES=2*1024*1024;MAX_TOTAL_BYTES=28*1024*1024
SPECS={'DFEDTARU':('Federal funds target range upper limit','D'),
       'DFEDTARL':('Federal funds target range lower limit','D'),
       'DFF':('Daily effective federal funds rate','D'),
       'FEDFUNDS':('Monthly average effective federal funds rate','M')}

def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def shown(value):
    if value is None:return None
    out=float(value)
    if not math.isfinite(out):raise ValueError('Nonfinite source arithmetic')
    return out
def decimal(value):
    if isinstance(value,bool):raise ValueError('Boolean is not a price or rate')
    out=report_observations.decimal(value)
    if value is not None and out is None:raise ValueError('Invalid numeric source value')
    return out
def stamp(value):
    if type(value) is not int or not 0<value<4102444800:raise ValueError('Canonical provider seconds required')
    return datetime.fromtimestamp(value,timezone.utc)
def quote_url(symbol):
    if not re.fullmatch(r'ZQ[FGHJKMNQUVXZ][0-9]{2}\.CBT',symbol):raise ValueError('Exact monthly ZQ symbol required')
    return 'https://query1.finance.yahoo.com/v8/finance/chart/'+symbol+'?range=10d&interval=1d'
def plan(started_at):
    at=clock(started_at);base=at.year*12+at.month-1;out=[]
    for offset in range(12):
        n=base+offset;y=n//12;m=n%12+1;symbol=f'ZQ{MONTH_CODES[m-1]}{y%100:02d}.CBT'
        out.append({'symbol':symbol,'contract_month':f'{y}-{m:02d}','request_url':quote_url(symbol)})
    return out

class Calendar(HTMLParser):
    """Extract only explicit meeting panel headings and month/date class fields."""
    def __init__(self):
        super().__init__(convert_charrefs=True);self.depth=0;self.capture=None;self.buffer=[];self.year=None
        self.month=None;self.rows=[];self.years=[]
    def handle_starttag(self,tag,attrs):
        if tag in ('br','hr','img','meta','link','input','source','wbr'):return
        self.depth+=1;classes=dict(attrs).get('class','').split()
        kind='heading' if tag=='h4' else 'month' if 'fomc-meeting__month' in classes else 'day' if 'fomc-meeting__date' in classes else None
        if kind:
            if self.capture:raise ValueError('Overlapping calendar source fields')
            self.capture=(kind,self.depth);self.buffer=[]
    def handle_data(self,text):
        if self.capture:self.buffer.append(text)
    def handle_endtag(self,tag):
        if tag in ('br','hr','img','meta','link','input','source','wbr'):return
        if self.capture and self.depth==self.capture[1]:
            kind=self.capture[0];text=' '.join(''.join(self.buffer).split());self.capture=None
            if kind=='heading':
                found=re.fullmatch(r'(20[0-9]{2}) FOMC Meetings',text)
                self.year=int(found[1]) if found else None
                if found:self.years.append(self.year)
            elif kind=='month':self.month=text
            elif kind=='day':
                if self.year is None or self.month is None:raise ValueError('Meeting date lacks source year/month')
                self.rows.append({'year':self.year,'month_text':self.month,'date_text':text,'original_calendar_row':len(self.rows)})
                self.month=None
        self.depth=max(0,self.depth-1)
    def handle_startendtag(self,tag,attrs):pass

def calendar(raw,page,started_at):
    if page.get('request_url')!=CALENDAR_URL:raise ValueError('Official calendar identity differs')
    if page.get('http_status')!=200:return {'available':False,'reason':'calendar_http_unavailable','meetings':[],'years':[]}
    parser=Calendar();parser.feed(raw.decode('utf-8'));parser.close()
    if parser.capture or len(set(parser.years))!=len(parser.years) or not parser.rows:raise ValueError('Calendar structure incomplete')
    months={name.lower():i for i,name in enumerate(month_name) if name};months.update({'apr':4,'jun':6,'jul':7,'sep':9,'sept':9,'oct':10,'nov':11,'dec':12,'jan':1,'feb':2,'mar':3,'aug':8})
    out=[];excluded=[];seen=set()
    for row in parser.rows:
        text=row['date_text'].replace('–','-').replace('—','-');match=re.fullmatch(r'([0-9]{1,2})(?:-([0-9]{1,2}))?(\*)?',text)
        if not match:
            excluded.append({**row,'reason':'not_a_plain_scheduled_meeting_date'});continue
        parts=row['month_text'].lower().split('/')
        if not 1<=len(parts)<=2 or any(p not in months for p in parts):raise ValueError('Unknown calendar month')
        first,last=months[parts[0]],months[parts[-1]];year=row['year']
        start=date(year-int(first>last),first,int(match[1]));end=date(year,last,int(match[2] or match[1]))
        if not 0<=(end-start).days<=3:raise ValueError('Unqualified meeting date span')
        if str(end) in seen:raise ValueError('Duplicate scheduled meeting date')
        seen.add(str(end));out.append({**row,'start_date':str(start),'end_date':str(end),'projections_marked':bool(match[3]),
            'policy_effective_date':None,'status':'published_schedule_subject_to_change','source_url':CALENDAR_URL})
    current=clock(started_at).date();by=Counter(r['year'] for r in out)
    if current.year not in parser.years or by[current.year]!=8:raise ValueError('Current-year regular calendar coverage differs')
    return {'available':True,'years':sorted(parser.years),'scheduled_counts_by_year':{str(y):by[y] for y in sorted(parser.years)},
        'meetings':sorted(out,key=lambda r:r['end_date']),'excluded_source_rows':excluded,
        'source_received_at':page['received_at'],'original':page['original'],
        'meaning':'Explicit published meeting dates. Future dates may change; no inferred effective date, unscheduled-event completeness or unconditional probability.'}

def chart(raw,page,item):
    """Preserve array alignment and separate bar labels from metadata clocks."""
    if page.get('request_url')!=item['request_url']:raise ValueError('Monthly source request differs')
    row={**item,'available':False,'bars':[],'latest_bar':None,'provider_market_mark':None,'original':page.get('original'),
        'http_status':page.get('http_status'),'source_received_at':page.get('received_at'),
        'contract_unit':'IMM_index_points','monthly_rate_unit':'percent_per_annum','official_settlement_verified':False,
        'exchange_session_completion_verified':False,'execution_quote_qualified':False,'meeting_probabilities':None,**PERMISSIONS}
    if page.get('http_status')!=200:return {**row,'reason':page.get('error') or 'provider_http_unavailable'}
    d=json.loads(raw,parse_float=Decimal,parse_constant=lambda x:(_ for _ in ()).throw(ValueError('Nonfinite provider JSON')))
    c=d.get('chart',{});results=c.get('result') or []
    if c.get('error') or not results:return {**row,'reason':'provider_result_unavailable'}
    if len(results)!=1:raise ValueError('One monthly contract result required')
    result=results[0];meta=result.get('meta',{})
    if (meta.get('symbol')!=item['symbol'] or meta.get('currency')!='USD' or meta.get('exchangeName')!='CBT'
            or meta.get('instrumentType')!='FUTURE' or meta.get('dataGranularity')!='1d'):
        raise ValueError('Provider contract definition differs')
    zone=ZoneInfo(meta['exchangeTimezoneName']);received=clock(page['received_at'])
    times=result.get('timestamp') or [];quotes=result.get('indicators',{}).get('quote') or []
    if len(quotes)!=1 or not isinstance(times,list) or len(times)>64:raise ValueError('Bounded aligned daily bars required')
    q=quotes[0];keys=('open','high','low','close','volume')
    if any(not isinstance(q.get(k),list) or len(q[k])!=len(times) for k in keys):raise ValueError('Quote timestamp arrays differ')
    bars=[];seen=set();first=stamp(meta['firstTradeDate']) if meta.get('firstTradeDate') is not None else None
    for i,ts in enumerate(times):
        at=stamp(ts)
        if ts in seen or at>received or first and at<first:raise ValueError('Invalid or duplicate provider bar timestamp')
        seen.add(ts);values={k:decimal(q[k][i]) for k in keys};price=values['close'];volume=values['volume']
        if volume is not None and (volume<0 or volume!=volume.to_integral_value()):raise ValueError('Nonnegative integral reported volume required')
        if values['high'] is not None and values['low'] is not None:
            if values['low']>values['high'] or any(values[k] is not None and not values['low']<=values[k]<=values['high'] for k in ('open','close')):raise ValueError('OHLC source bounds differ')
        bars.append({'original_row_index':i,'provider_bar_timestamp':at.isoformat(),'provider_local_date':str(at.astimezone(zone).date()),
            'close_field':shown(price),'exact_close_field':str(price) if price is not None else None,
            'reported_volume':int(volume) if volume is not None else None,
            'rate_equivalent_percent':shown(100-price) if price is not None else None,
            'meaning':'Provider daily close field, possibly in progress. Timestamp is the source bar label, not a certified closing trade or exchange session date.'})
    bars.sort(key=lambda r:r['provider_bar_timestamp']);numeric=[r for r in bars if r['close_field'] is not None]
    latest=numeric[-1] if numeric else None;mark=None
    if meta.get('regularMarketPrice') is not None and meta.get('regularMarketTime') is not None:
        at=stamp(meta['regularMarketTime']);price=decimal(meta['regularMarketPrice'])
        if at>received or first and at<first:raise ValueError('Future or pre-listing provider market clock')
        mark={'price':shown(price),'exact_price':str(price),'provider_market_time':at.isoformat(),
            'rate_equivalent_percent':shown(100-price),'meaning':'Separate provider metadata mark. Not silently substituted into the daily-bar array; no official-settlement or executable-quote claim.'}
    age=(received-clock(latest['provider_bar_timestamp'])).total_seconds() if latest else None
    fresh=age is not None and 0<=age<=7*86400
    return {**row,'available':fresh,'reason':'dated_provider_bars_only' if fresh else 'bar_missing_or_over_seven_days',
        'bars':bars,'latest_bar':latest,'provider_market_mark':mark,'provider_bar_age_seconds_at_capture':age,
        'metadata':{k:meta.get(k) for k in ('symbol','currency','exchangeName','fullExchangeName','instrumentType','exchangeTimezoneName','shortName','longName','dataGranularity')},
        'quote_valid_until':(min(received+timedelta(hours=26),clock(latest['provider_bar_timestamp'])+timedelta(days=7))).isoformat() if latest else None,
        'adjusted_close_used':False,'formula':'Rate equivalent = 100 minus provider index price; this is not a meeting outcome or probability.'}

def policy_rows(packet,originals,generated_at):
    at=clock(generated_at);source=clock(packet['generated_at']);rows={}
    if source>at:raise ValueError('Future canonical rate source')
    for sid in SERIES:
        m=packet.get('measurements',{}).get(sid) or {};label,freq=SPECS[sid]
        if m and not originals.get(sid):raise ValueError('Original policy-rate reconstruction required')
        if m and (m.get('series_id')!=sid or m.get('unit')!='Percent' or m.get('frequency')!=freq or m.get('definition',{}).get('seasonal_adjustment_short')!='NSA'):raise ValueError('Policy-rate source definition differs')
        state=row_status(m,at,(at-source).total_seconds());v=decimal(m.get('current_decimal')) if state=='fresh' else None
        due=[source+timedelta(hours=26)]
        if m.get('acquired_at'):due.append(clock(m['acquired_at'])+timedelta(hours=26))
        if m.get('date'):due.append(datetime.combine(date.fromisoformat(m['date'])+timedelta(days=AGE_LIMITS[freq]+1),time.min,timezone.utc))
        rows[sid]={'series_id':sid,'label':label,'value':shown(v),'exact_value':str(v) if v is not None else None,
            'unit':'percent','frequency':freq,'observation_date':m.get('date'),'acquired_at':m.get('acquired_at'),
            'current_row_index':m.get('current_row_index'),'evidence':m.get('evidence',{}),'source_valid_until':min(due).isoformat(),
            'quality':{'status':'within_age_ceiling' if v is not None else state},**PERMISSIONS}
    lo,hi=rows['DFEDTARL'],rows['DFEDTARU'];same=lo['value'] is not None and hi['value'] is not None and lo['observation_date']==hi['observation_date']
    if same and Decimal(lo['exact_value'])>Decimal(hi['exact_value']):raise ValueError('Crossed target bounds')
    target={'lower':lo['value'] if same else None,'upper':hi['value'] if same else None,
        'midpoint':shown((Decimal(lo['exact_value'])+Decimal(hi['exact_value']))/2) if same else None,
        'as_of':lo['observation_date'] if same else None,'meaning':'Same-date target range; not the effective rate used in futures cash settlement.'}
    return rows,target

def _build(packet,originals,collection,source_bodies,generated_at):
    started=clock(collection['started_at']);generated=clock(generated_at)
    if not started<=clock(collection['completed_at'])<=generated<=started+timedelta(seconds=150):raise ValueError('Bounded native capture clock required')
    if clock(packet['generated_at'])>started:raise ValueError('Canonical snapshot is from after capture began')
    expected=plan(collection['started_at'])
    if collection['plan']!=expected or set(collection['quotes'])!={x['symbol'] for x in expected}:raise ValueError('Exact twelve-month request plan required')
    pages=[collection['calendar'],*collection['quotes'].values()]
    for page in pages:
        if page.get('request_sent') is True:
            if not started<=clock(page['started_at'])<=clock(page['received_at'])<=clock(collection['completed_at']):raise ValueError('Source capture clocks differ')
        elif page.get('request_sent') is not False or page.get('original') is not None:raise ValueError('Explicit sent/skipped request state required')
    if type(collection['provider_requests']) is not int or collection['provider_requests']!=sum(p['request_sent'] for p in pages):raise ValueError('Provider request count differs')
    size=sum(p.get('original',{}).get('bytes',0) for p in pages if p.get('original'))
    if type(collection['source_bytes']) is not int or collection['source_bytes']!=size or not 0<=size<=MAX_TOTAL_BYTES:raise ValueError('Bounded original byte count differs')
    rows,target=policy_rows(packet,originals,generated_at);calpage=collection['calendar']
    cal=calendar(source_bodies.get('calendar',b''),calpage,collection['started_at'])
    quotes=[chart(source_bodies.get(x['symbol'],b''),collection['quotes'][x['symbol']],x) for x in expected]
    meetings=[m for m in cal['meetings'] if m['end_date']>=str(started.date())]
    legacy_meetings=[{**m,'date':m['end_date'],'status':'scheduled_probability_unqualified','implied_move_bps':None,
        'implied_post_meeting_rate_pct':None,'probabilities_pct':None,'rate_buckets_pct':None} for m in meetings]
    fresh=sum(q['available'] for q in quotes)
    return {'contract':CONTRACT,'version':'2.0.0','engine':'justhodl-fedwatch-rate-probability','generated_at':generated_at,
        'source_generated_at':packet['generated_at'],'policy_measurements':rows,'current_fed_funds_range':target,
        'calendar':cal,'contracts':quotes,'next_meeting':legacy_meetings[0] if legacy_meetings else None,'meetings_ahead':legacy_meetings,
        'n_meetings_with_data':0,'next_6mo_summary':{'scenario':None,'cumulative_implied_move_bps':None,'n_reliable_meetings':0,'avg_implied_move_per_meeting_bps':None},
        'collection':{k:collection[k] for k in ('started_at','completed_at','provider_requests','source_bytes')},
        'freshness':{'capture_check_due_at':(started+timedelta(hours=26)).isoformat(),
            'canonical_check_due_at':(clock(packet['generated_at'])+timedelta(hours=26)).isoformat(),
            'basis':'Capture ceiling is not an exchange trading calendar or settlement-completion check.'},
        'quality':{'status':'partial' if fresh else 'unavailable','requested_contracts':len(expected),'dated_contracts_within_age_ceiling':fresh,
            'official_settlements_verified':0,'synchronized_curve_verified':False,'calendar_available':cal['available'],'independent_investment_votes':0},
        'score':None,'regime':None,'call':None,'portfolio_action':'WAIT',**PERMISSIONS,
        'methodology':{'rate_equivalent':'100 minus a contract index price is an arithmetic rate equivalent. A daily close field is not necessarily an official settlement or completed-session price.',
            'clocks':'Each bar retains its original index and timestamp. Provider market metadata is separate. No prior close, current mark or contract month is silently substituted.',
            'monthly_settlement':'CME ZQ final settlement references the arithmetic calendar-day average effective federal funds rate, including its specified nonpublication-day convention. The current policy midpoint is not that realized/expected average.',
            'probabilities':'No probability is identified by a single expected rate without model assumptions. Non-meeting anchors, effective dates, accrued daily rates, quote synchronization, state support and branching rules require a reviewed model. Out-of-support values are never clipped into certainty.',
            'calendar':'Published meeting end dates do not establish a future policy effective date or rule out unscheduled action.',
            'replay':'Exact captured source bodies and canonical FRED originals reconstruct current research. Current-vintage history is not a point-in-time forecast backtest.'},
        'references':{'contract_rules':'https://www.cmegroup.com/rulebook/CBOT/III/22.pdf','effr_definition':'https://www.newyorkfed.org/markets/reference-rates/effr','calendar':CALENDAR_URL,
            'cme_methodology':'https://www.cmegroup.com/articles/2023/understanding-the-cme-group-fedwatch-tool-methodology.html'},
        'portfolio_consequences':{'status':'EXPLICIT_SCENARIOS_ONLY','instrument':'CME 30-Day Federal Funds (ZQ) futures',
            'dollars_per_index_point':4167,'dollars_per_rate_basis_point_per_contract':41.67,
            'formula':'Signed contracts * 4167 * (entered exit index price - entered entry index price)',
            'meaning':'Hypothetical mark-to-market price P&L under entered prices; fees, margin funding and execution differences excluded. No position or quote is supplied.'}}

def build(packet,originals,collection,source_bodies,generated_at):
    with localcontext() as arithmetic:
        arithmetic.prec=40;arithmetic.rounding=ROUND_HALF_EVEN
        return _build(packet,originals,collection,source_bodies,generated_at)
