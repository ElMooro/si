"""Exact scheduled-event observations and descriptive history, never a policy-shock forecast."""
from calendar import month_name
from collections import Counter
from datetime import date,datetime,time,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
from html.parser import HTMLParser
import hashlib,json,math,re
import report_observations
from research_brief_model import clock,row_status,AGE_LIMITS
CONTRACT='fomc-native-research.v1';PREFIX='data/fomc-research/';CURRENT='data/fomc-reaction.json'
PRIVATE='audit-private/20260909-originals/fomc-research/'
SERIES=('DGS2','DGS10','SP500','NASDAQCOM');HORIZONS=(1,5,21,63)
SPECS={'DGS2':('US Treasury 2-year constant-maturity yield','Percent','yield'),
       'DGS10':('US Treasury 10-year constant-maturity yield','Percent','yield'),
       'SP500':('S&P 500 price index','Index','equity_price_index'),
       'NASDAQCOM':('Nasdaq Composite price index','Index Feb 5, 1971=100','equity_price_index')}
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
CALENDAR_URL='https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def shown(value):
    if value is None:return None
    out=float(value)
    if not math.isfinite(out):raise ValueError('Nonfinite source arithmetic')
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

def history(original,through):
    if original is None:return []
    rows=[];seen=set()
    for index,row in enumerate(original['observations']['observations']):
        day=date.fromisoformat(row['date']);value=report_observations.decimal(row.get('value'))
        if str(day)!=row['date'] or day in seen:raise ValueError('Unique canonical observation dates required')
        if row.get('value') not in (None,'.','') and value is None:raise ValueError('Invalid original observation')
        seen.add(day)
        if day<=through:rows.append({'date':str(day),'value':value,'original_row_index':index})
    return sorted(rows,key=lambda r:r['date'])

def point(row):return {'date':row['date'],'value':shown(row['value']),'exact_value':str(row['value']) if row['value'] is not None else None,'original_row_index':row['original_row_index']}

def source_histories(packet,originals,generated_at):
    at=clock(generated_at);source=clock(packet['generated_at'])
    if source>at:raise ValueError('Future canonical publication')
    histories={};measurements={}
    for sid,(label,unit,kind) in SPECS.items():
        m=packet.get('measurements',{}).get(sid) or {};original=originals.get(sid)
        if m and original is None:raise ValueError('Original reconstruction required')
        if m and (m.get('series_id')!=sid or m.get('unit')!=unit or m.get('frequency')!='D' or m.get('definition',{}).get('seasonal_adjustment_short')!='NSA'):raise ValueError('Source instrument definition differs')
        rows=history(original,at.date());histories[sid]=rows
        if kind=='equity_price_index' and any(r['value'] is not None and r['value']<=0 for r in rows):raise ValueError('Positive equity index levels required')
        state=row_status(m,at,(at-source).total_seconds());due=[source+timedelta(hours=26)]
        if m.get('acquired_at'):due.append(clock(m['acquired_at'])+timedelta(hours=26))
        if m.get('date'):due.append(datetime.combine(date.fromisoformat(m['date'])+timedelta(days=AGE_LIMITS['D']+1),time.min,timezone.utc))
        measurements[sid]={'series_id':sid,'label':label,'unit':unit,'kind':kind,'frequency':'D','seasonal_adjustment':'NSA',
            'value':shown(report_observations.decimal(m.get('current_decimal'))),'observation_date':m.get('date'),'acquired_at':m.get('acquired_at'),
            'quality':{'status':'within_age_ceiling' if state=='fresh' else state},'source_valid_until':min(due).isoformat(),
            'evidence':m.get('evidence',{}),'current_row_index':m.get('current_row_index'),
            'history':{'returned_rows':len(rows),'numeric_rows':sum(r['value'] is not None for r in rows),
                'first_date':rows[0]['date'] if rows else None,'last_date':rows[-1]['date'] if rows else None,
                'scope':'Bounded current-vintage original rows; not all-time coverage or historical first availability.'},**PERMISSIONS}
    return histories,measurements

def event_change(rows,event_date,kind):
    i=next((i for i,r in enumerate(rows) if r['date']==event_date),None)
    out={'available':False,'value':None,'unit':'basis_points' if kind=='yield' else 'price_return_percent',
        'event_date':event_date,'baseline':None,'event_observation':point(rows[i]) if i is not None else None,
        'reason':'exact_event_observation_missing',**PERMISSIONS}
    if i is None or rows[i]['value'] is None:return out
    if i==0:return {**out,'reason':'preceding_observation_outside_retained_history'}
    a,b=rows[i-1],rows[i];days=(date.fromisoformat(b['date'])-date.fromisoformat(a['date'])).days
    out.update(baseline=point(a),calendar_days=days)
    if a['value'] is None or days>4:return {**out,'reason':'adjacent_observation_missing_or_gap_over_four_days'}
    value=(b['value']-a['value'])*100 if kind=='yield' else (b['value']/a['value']-1)*100
    return {**out,'available':True,'value':shown(value),'exact_value':str(value),'reason':'dated_daily_association',
        'formula':'100 * (event - baseline)' if kind=='yield' else '100 * (event / baseline - 1)'}

def outcome(rows,event_date,horizon,kind,meeting_dates):
    i=next((i for i,r in enumerate(rows) if r['date']==event_date),None)
    out={'available':False,'value':None,'unit':'basis_points' if kind=='yield' else 'price_return_percent',
        'horizon_reported_numeric_observations':horizon,'start':point(rows[i]) if i is not None else None,'end':None,
        'reason':'exact_event_observation_missing','intervening_scheduled_meetings':[],**PERMISSIONS}
    if i is None or rows[i]['value'] is None:return out
    following=[(j,r) for j,r in enumerate(rows) if j>i and r['value'] is not None]
    if len(following)<horizon:return {**out,'reason':'horizon_not_observed','following_numeric_observations_available':len(following)}
    j,b=following[horizon-1];a=rows[i]
    value=(b['value']-a['value'])*100 if kind=='yield' else (b['value']/a['value']-1)*100
    return {**out,'available':True,'value':shown(value),'exact_value':str(value),'end':point(b),'reason':'observed_historical_change',
        'calendar_days':(date.fromisoformat(b['date'])-date.fromisoformat(a['date'])).days,
        'missing_reported_rows_inside_span':sum(r['value'] is None for r in rows[i+1:j]),
        'intervening_scheduled_meetings':[d for d in meeting_dates if event_date<d<=b['date']],
        'formula':'100 * (end - start)' if kind=='yield' else '100 * (end / start - 1)',
        'horizon_meaning':'Following numeric source observations with actual endpoint dates; not certified exchange trading sessions or fixed calendar duration.'}

def describe(events,sid,horizon,condition):
    eligible=[e for e in events if condition=='all' or e['two_year_daily_direction']==condition]
    samples=[{'event_date':e['event_date'],**e['assets'][sid]['forward_changes'][str(horizon)]} for e in eligible if e['assets'][sid]['forward_changes'][str(horizon)]['available']]
    values=sorted(Decimal(s['exact_value']) for s in samples);n=len(values)
    def quantile(fraction):
        k=Decimal(n-1)*fraction;lo=int(k);hi=min(lo+1,n-1)
        return shown(values[lo]+(values[hi]-values[lo])*(k-lo))
    overlaps=[]
    for i,a in enumerate(samples):
        for b in samples[i+1:]:
            if a['start']['date']<b['end']['date'] and b['start']['date']<a['end']['date']:overlaps.append([a['event_date'],b['event_date']])
    return {'series_id':sid,'condition':condition,'horizon_reported_numeric_observations':horizon,'eligible_events':len(eligible),'numeric_outcomes':n,
        'excluded_events':[e['event_date'] for e in eligible if not e['assets'][sid]['forward_changes'][str(horizon)]['available']],
        'event_dates':[s['event_date'] for s in samples],'mean':shown(sum(values)/n) if n else None,'median':quantile(Decimal('.5')) if n else None,
        'p25':quantile(Decimal('.25')) if n else None,'p75':quantile(Decimal('.75')) if n else None,
        'historical_positive_fraction_percent':shown(Decimal(sum(v>0 for v in values))*100/n) if n else None,
        'zero_outcomes':sum(v==0 for v in values),'unit':'basis_points' if SPECS[sid][2]=='yield' else 'price_return_percent',
        'sample_overlap_pairs':overlaps,'outcomes_with_intervening_meetings':sum(bool(s['intervening_scheduled_meetings']) for s in samples),
        'confidence_interval':None,'forecast_probability':None,'independent_sample_size':None,
        'meaning':'Descriptive current-vintage sample, including every available eligible event. Quartiles are sample order statistics, not forecast intervals. Post-event conditioning and overlap prohibit an independent forecast-accuracy claim.',**PERMISSIONS}

def _build(packet,originals,calendar_packet,calendar_raw,generated_at):
    at=clock(generated_at);fw=calendar_packet
    if fw.get('contract')!='fedwatch-native-research.v1' or clock(fw['generated_at'])>at:raise ValueError('Dated native calendar producer required')
    if any(fw.get(k) is not False for k in PERMISSIONS):raise ValueError('Upstream calendar cannot convey investment authority')
    c=fw['calendar'];ref=c.get('original') or {}
    if not c.get('available') or ref.get('sha256')!=sha(calendar_raw) or ref.get('bytes')!=len(calendar_raw):raise ValueError('Exact official calendar original required')
    page={'request_url':CALENDAR_URL,'http_status':200,'received_at':c['source_received_at'],'original':ref}
    cal=calendar(calendar_raw,page,fw['collection']['started_at'])
    if cal!=c:raise ValueError('Original calendar reconstruction differs')
    if not clock(fw['collection']['started_at'])<=clock(c['source_received_at'])<=clock(fw['generated_at']):raise ValueError('Calendar source clocks differ')
    histories,measurements=source_histories(packet,originals,generated_at)
    meeting_dates=[m['end_date'] for m in cal['meetings']];past=[m for m in cal['meetings'] if m['end_date']<=str(at.date())];events=[]
    for meeting in past:
        day=meeting['end_date'];assets={}
        for sid,(_,_,kind) in SPECS.items():
            rows=histories[sid];assets[sid]={'event_date_change':event_change(rows,day,kind),
                'forward_changes':{str(h):outcome(rows,day,h,kind,meeting_dates) for h in HORIZONS}}
        d=assets['DGS2']['event_date_change'];v=Decimal(d['exact_value']) if d['available'] else None
        direction='up' if v is not None and v>0 else 'down' if v is not None and v<0 else 'unchanged' if v is not None else None
        events.append({'event_date':day,'published_meeting':meeting,'two_year_daily_direction':direction,'assets':assets,
            'identified_policy_shock':None,'forecast_qualified':False})
    summaries=[describe(events,sid,h,condition) for sid in SERIES for h in HORIZONS for condition in ('all','up','down','unchanged')]
    upcoming=[m for m in cal['meetings'] if m['end_date']>=str(at.date())]
    canonical_due=clock(packet['generated_at'])+timedelta(hours=26);calendar_due=clock(c['source_received_at'])+timedelta(hours=26)
    return {'contract':CONTRACT,'version':'2.0.0','engine':'justhodl-fomc-reaction','generated_at':generated_at,
        'source_generated_at':packet['generated_at'],'calendar_generated_at':fw['generated_at'],'calendar':cal,'measurements':measurements,
        'events':events,'summaries':summaries,'horizons_reported_numeric_observations':list(HORIZONS),
        'is_published_meeting_end_date':str(at.date()) in meeting_dates,'next_published_meeting':upcoming[0] if upcoming else None,
        'freshness':{'canonical_check_due_at':canonical_due.isoformat(),'calendar_check_due_at':calendar_due.isoformat(),
            'pipeline_check_due_at':min(canonical_due,calendar_due).isoformat()},
        'quality':{'status':'partial','declared_series':len(SERIES),'available_histories':sum(bool(h) for h in histories.values()),
            'historical_scheduled_events':len(events),'events_with_two_year_daily_direction':sum(e['two_year_daily_direction'] is not None for e in events),
            'independent_investment_votes':0,'as_known_at_vintages_verified':False,'intraday_shock_identified':False},
        'surprise':{'label':None,'basis':'unqualified','d2y_change_bp':None,'statement_tone':None,'priced_in':None},
        'reaction_map':{},'calibration':{'status':'historical_descriptive_only','forecast_calibrated':False},
        'regime_context':None,'self_grading':{'n':0,'directional_accuracy_pct':None,'coverage_pct':None,'status':'prospective_grading_unqualified'},
        'legacy_assets':{'status':'retained_predecessor_only','assets':['SPY','QQQ','IWM','TLT','HYG','GLD','UUP','BTCUSD'],
            'meaning':'Full predecessor and calibration remain protected. Price indexes are not ETF returns; Treasury yield changes are not bond total returns. No unsupported asset forecast is carried forward.'},
        'call':None,'score':None,'regime':None,'portfolio_action':'WAIT',**PERMISSIONS,
        'methodology':{'event_registry':'Official currently published scheduled meeting dates only; retained page years define coverage. Unscheduled interventions and notation votes are not a complete registry.',
            'daily_association':'Daily yield changes mix policy, other news, premia and timing. They do not identify an intraday policy surprise. LLM tone never substitutes for observations.',
            'sample':'Exact event-date join. Event-date changes require the preceding returned row, both numeric, at most four calendar days apart. No Sunday-to-Friday substitution.',
            'horizons':'Follow 1/5/21/63 subsequent numeric source observations; every result retains actual endpoints, original indices, skipped missing rows and intervening scheduled meetings. No shorter-horizon fallback.',
            'conditioning':'Up/down/unchanged uses the observed event-date DGS2 change and is retrospective. Samples overlap; frequency positive is descriptive, not a calibrated probability.',
            'identities':'SP500 is the S&P 500 price index, NASDAQCOM the Nasdaq Composite, neither SPY nor QQQ. Index returns exclude dividends. Yields remain basis-point changes.',
            'vintages':'Reconstructed current-vintage history; historical first-availability times and executable prices are not verified.'},
        'references':{'calendar':CALENDAR_URL,'high_frequency_identification':'https://www.federalreserve.gov/pubs/feds/2004/200466/200466pap.pdf'},
        'portfolio_consequences':{'status':'EXPLICIT_SCENARIOS_ONLY','formula':'Equity signed USD exposure * entered price return percent / 100; bond first-order P&L = -signed USD DV01 * entered yield change in bp',
            'meaning':'User assumptions only. No forecast, position or hedge is supplied. Bond duration approximation excludes convexity, nonparallel curves, spread, carry, FX, fees and funding.'}}

def build(packet,originals,calendar_packet,calendar_raw,generated_at):
    with localcontext() as arithmetic:
        arithmetic.prec=40;arithmetic.rounding=ROUND_HALF_EVEN
        return _build(packet,originals,calendar_packet,calendar_raw,generated_at)
