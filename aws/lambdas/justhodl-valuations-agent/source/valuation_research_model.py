"""Dated valuation inputs and descriptive comparisons, never fabricated fair value."""
from copy import deepcopy
from datetime import date,datetime,time,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import hashlib,json,math
import report_observations
from research_brief_model import clock,row_status,AGE_LIMITS

CONTRACT='valuation-native-research.v1';PREFIX='data/valuation-research/';CURRENT='valuations-data.json'
PRIVATE='audit-private/20260909-originals/valuation-research/'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
# Source units and sampling are part of identity, not display conversions.
SPECS={
 'SP500':('S&P 500 price index','Index','D','NSA'),
 'GDP':('US nominal GDP, annualized quarterly flow','Billions of Dollars','Q','SAAR'),
 'DGS10':('US ten-year Treasury constant-maturity yield','Percent','D','NSA'),
 'VIXCLS':('Cboe VIX index','Index','D','NSA'),
 'BAA':("Moody's seasoned Baa corporate yield",'Percent','M','NSA'),
 'AAA':("Moody's seasoned Aaa corporate yield",'Percent','M','NSA'),
 'BAMLH0A0HYM2':('ICE BofA US high-yield index OAS','Percent','D','NSA'),
 'BAMLC0A0CM':('ICE BofA US corporate index OAS','Percent','D','NSA'),
 'T10YIE':('US ten-year breakeven inflation rate','Percent','D','NSA'),
 'M2SL':('US M2 money stock','Billions of Dollars','M','SA'),
 'CPIAUCSL':('US all-items CPI, seasonally adjusted','Index 1982-1984=100','M','SA'),
 'DTWEXBGS':('Nominal broad US dollar index','Index Jan 2006=100','D','NSA'),
 'WALCL':('Federal Reserve total assets, Wednesday level','Millions of U.S. Dollars','W','NSA'),
 'DCOILWTICO':('WTI Cushing spot oil price','Dollars per Barrel','D','NSA'),
 'DCOILBRENTEU':('Brent Europe spot oil price','Dollars per Barrel','D','NSA'),
 'DHHNGSP':('Henry Hub spot natural gas price','Dollars per Million BTU','D','NSA')}
UNQUALIFIED={
 'CAPE':{'reason':'No reviewed Shiller original-source contract. A missing FRED series is not CAPE evidence.',
         'source_url':'https://www.econ.yale.edu/~shiller/data.htm'},
 'WILL5000INDFC':{'reason':'Wilshire index data removed from FRED in 2024. Index points cannot replace market-cap dollars.',
         'source_url':'https://news.research.stlouisfed.org/2024/04/fred-will-remove-wilshire-index-data-on-june-3-2024/'},
 'GOLDAMGBD228NLBM':{'reason':'IBA gold benchmarks removed from FRED in 2022. No fixed price or ETF-share multiplier substitution.',
         'source_url':'https://news.research.stlouisfed.org/2022/01/ice-benchmark-administration-ltd-iba-data-to-be-removed-from-fred/'}}
SERIES=tuple((*SPECS,*UNQUALIFIED))
PROVIDER_GAPS={
 'GLD': 'ETF share close is not spot gold per ounce; backing, fees, timestamps and original quote unqualified.',
 'SLV': 'ETF share close is not spot silver per ounce; no fixed share-to-ounce multiplier.',
 'PPLT': 'ETF share close is not spot platinum per ounce; no fixed share-to-ounce multiplier.',
 'GDX': 'Gold-miner equity ETF snapshot is not metal spot value; original quote is not retained by the predecessor.',
 'XAUUSD': 'Legacy quote identifier, market identity, units and original response need qualification.',
 'CMC_GLOBAL': 'Legacy crypto market-cap aggregate lacks retained original response and universe reconciliation; market cap is not intrinsic fair value.'}

def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def shown(value):
    if value is None:return None
    result=float(round(value,8))
    if not math.isfinite(result):raise ValueError('Nonfinite derived value')
    return result

def history(original,through):
    if original is None:return []
    out=[];seen=set()
    for i,row in enumerate(original['observations']['observations']):
        day=date.fromisoformat(row['date'])
        if str(day)!=row['date'] or day in seen:raise ValueError('Duplicate or noncanonical source date')
        seen.add(day)
        value=report_observations.decimal(row.get('value'))
        if row.get('value') not in (None,'.','') and value is None:raise ValueError('Invalid source number')
        if day>date.fromisoformat(through):continue
        out.append({'date':str(day),'value':value,'original_row_index':i})
    return sorted(out,key=lambda r:r['date'])

def statistics(rows,current,n):
    sample=[r for r in rows if r['value'] is not None][-n:]
    first=sample[0]['date'] if sample else None;last=sample[-1]['date'] if sample else None
    out={'requested_observations':n,'numeric_observations':len(sample),'first_date':first,'last_date':last,
        'missing_rows_inside_span':sum(r['value'] is None and first<=r['date']<=last for r in rows) if sample else 0,
        'mean':None,'sample_sd':None,'difference_from_mean':None,'relative_difference_percent':None,
        'midrank_percentile':None,'z_score':None,'status':'insufficient_history',
        'scope':'Last N numeric observations inclusive of the current dated observation; actual dates, not N trading days. Current vintage, not fair value, a probability or a backtest.',**PERMISSIONS}
    if len(sample)!=n or n<2:return out
    values=[r['value'] for r in sample];mean=sum(values)/n;sd=(sum((v-mean)**2 for v in values)/Decimal(n-1)).sqrt()
    out.update(mean=shown(mean),sample_sd=shown(sd),status='historical_only' if current is None else 'constant_window' if sd==0 else 'available',
        original_row_indices=[r['original_row_index'] for r in sample])
    if current is not None:
        diff=current-mean
        out.update(difference_from_mean=shown(diff),relative_difference_percent=shown(100*diff/mean) if mean>0 else None,
            midrank_percentile=shown(100*(Decimal(sum(v<current for v in values))+Decimal('0.5')*sum(v==current for v in values))/n),
            z_score=shown(diff/sd) if sd else None)
    return out

def observed(packet,originals,generated_at):
    at=clock(generated_at);source=clock(packet['generated_at'])
    if source>at:raise ValueError('Future canonical publication')
    rows={};histories={}
    for sid in SERIES:
        m=packet.get('measurements',{}).get(sid) or {};original=originals.get(sid)
        if m and not original:raise ValueError('Pinned original reconstruction required')
        if sid in UNQUALIFIED:
            rows[sid]={'series_id':sid,'label':sid,'value':None,'exact_value':None,'unit':None,'frequency':None,
                'observation_date':None,'quality':{'status':'source_unqualified'},'original_available':original is not None,
                **deepcopy(UNQUALIFIED[sid]),**PERMISSIONS};histories[sid]=[];continue
        label,unit,freq,adj=SPECS[sid]
        if m and (m.get('series_id')!=sid or m.get('unit')!=unit or m.get('frequency')!=freq
                  or m.get('definition',{}).get('seasonal_adjustment_short')!=adj):raise ValueError('Source definition differs: '+sid)
        status=row_status(m,at,(at-source).total_seconds());day=m.get('date')
        h=history(original,day) if day else [];histories[sid]=h
        value=report_observations.decimal(m.get('current_decimal')) if status=='fresh' else None
        due=[source+timedelta(hours=26)]
        if m.get('acquired_at'):due.append(clock(m['acquired_at'])+timedelta(hours=26))
        if day:due.append(datetime.combine(date.fromisoformat(day)+timedelta(days=AGE_LIMITS[freq]+1),time.min,timezone.utc))
        # No market-price floor: genuine negative energy prices and zero rates survive.
        if value is not None and sid in ('SP500','GDP','VIXCLS','M2SL','CPIAUCSL','DTWEXBGS','WALCL') and value<=0:
            raise ValueError('Nonpositive level for positive-defined series')
        n={'D':200,'W':52,'M':60,'Q':40}[freq]
        rows[sid]={'series_id':sid,'label':label,'source_url':'https://fred.stlouisfed.org/series/'+sid,
            'value':float(value) if value is not None else None,'exact_value':str(value) if value is not None else None,
            'unit':unit,'source_unit':m.get('unit'),'frequency':freq,'seasonal_adjustment':adj,'observation_date':day,
            'current_row_index':m.get('current_row_index'),'source_generated_at':packet['generated_at'],
            'acquired_at':m.get('acquired_at'),'source_valid_until':min(due).isoformat(),
            'definition':deepcopy(m.get('definition')),'evidence':deepcopy(m.get('evidence',{})),
            'quality':{'status':'within_age_ceiling' if value is not None else status,'release_calendar_verified':False},
            'history_coverage':{'returned_rows':len(h),'numeric_rows':sum(r['value'] is not None for r in h),
                'first_date':h[0]['date'] if h else None,'last_date':h[-1]['date'] if h else None,
                'current_vintage_only':True,'point_in_time_history':False},
            'statistics':statistics(h,value,n),'changes':deepcopy(m.get('changes',{})) if value is not None else {},**PERMISSIONS}
    return rows,histories

def difference(rows,left,right,label,unit,scale=1):
    a=rows[left];b=rows[right]
    ok=all(m['quality']['status']=='within_age_ceiling' for m in (a,b)) and a['observation_date']==b['observation_date']
    return {'label':label,'available':ok,'value':shown((Decimal(a['exact_value'])-Decimal(b['exact_value']))*scale) if ok else None,
        'unit':unit,'roots':[left,right],'observation_date':a['observation_date'] if ok else None,
        'input_dates':{left:a['observation_date'],right:b['observation_date']},
        'formula':f'({left} - {right}) * {scale}',
        'status':'same_date_descriptive_difference' if ok else 'dated_inputs_unavailable_or_unmatched',
        'interpretation':'Same reported date; no synchronized intraday timestamp, fair value or trading implication.',**PERMISSIONS}

def cpi_yoy(rows,histories):
    m=rows['CPIAUCSL'];h=histories['CPIAUCSL'];day=date.fromisoformat(m['observation_date']) if m['observation_date'] else None
    target=str(day.replace(year=day.year-1)) if day else None
    baseline=next((r for r in h if r['date']==target),None);b=baseline['value'] if baseline else None
    ok=m['value'] is not None and b is not None and b>0
    return {'label':'All-items CPI change over twelve matched months, seasonally adjusted','available':ok,
        'value':shown(100*(Decimal(m['exact_value'])/b-1)) if ok else None,'unit':'percent_change',
        'roots':['CPIAUCSL'],'observation_date':m['observation_date'],'baseline_date':target,
        'baseline_value':shown(b),'baseline_original_row_index':baseline['original_row_index'] if baseline else None,
        'current_original_row_index':m['current_row_index'],'formula':'100 * (current CPI / CPI exactly twelve months earlier - 1)',
        'interpretation':'Seasonally adjusted CPI series; not the headline non-seasonally-adjusted CPI-U release or an inflation forecast.',**PERMISSIONS}

def _build(packet,originals,legacy,generated_at):
    rows,histories=observed(packet,originals,generated_at)
    derivatives={'hy_minus_ig_oas':difference(rows,'BAMLH0A0HYM2','BAMLC0A0CM','HY minus IG index OAS','basis_points',100),
        'baa_minus_aaa_yield':difference(rows,'BAA','AAA','Baa minus Aaa seasoned corporate yield','basis_points',100),
        'brent_minus_wti':difference(rows,'DCOILBRENTEU','DCOILWTICO','Brent Europe minus WTI Cushing spot','USD_per_barrel'),
        'cpi_yoy':cpi_yoy(rows,histories)}
    n=sum(m['quality']['status']=='within_age_ceiling' for m in rows.values())
    return {'contract':CONTRACT,'engine':'justhodl-valuations-agent','schema_version':'2.0','generated_at':generated_at,
        'as_of':None,'source_generated_at':packet['generated_at'],'measurements':rows,'descriptive_comparisons':derivatives,
        'freshness':{'pipeline_check_due_at':(clock(packet['generated_at'])+timedelta(hours=26)).isoformat(),
            'basis':'Canonical publication/acquisition ceiling 26 hours; frequency-specific observation age ceilings. Publication is not source release time.'},
        'quality':{'status':'partial' if n else 'unavailable','within_age_ceiling':n,'declared_series':len(SERIES),
            'reviewed_definitions':len(SPECS),'release_calendar_verified':False,'independent_investment_votes':0},
        'source_gaps':deepcopy(PROVIDER_GAPS),
        'valuation_gaps':{'market_cap_gdp':'A matched, source-defined equity market-cap dollar numerator and nominal GDP denominator are not established. No index multiple proxy.',
            'cape':'Reviewed original price, real earnings, dividend and payout conventions are not established. No earnings-yield forecast.',
            'metal_spot':'No native current physical metal benchmark or reconciled ETF backing; legacy synthetic spot prices withheld.',
            'gold_m2':'Global gold stock value and US money stock differ in coverage; an arbitrary benchmark does not establish fair value.',
            'crypto':'Legacy market cap retained but no economic fair-value model or matched historical universe.',
            'fair_value':'No composite fair value or undervalued/overvalued regime. History location is not an investment forecast.'},
        'legacy_context':{k:{'retained':v is not None,'qualified':False,
            'reason':'Whole predecessor packet retained privately. Original quotes, provider identity and benchmark assumptions are not reconstructed.'} for k,v in legacy.items()},
        'composite':{'score':None,'regime':None,'color':None,'sample_size':0},'score':None,'regime':None,'call':None,
        'cape':None,'buffett_indicator':None,'market_cap_gdp':None,'portfolio_action':'WAIT',**PERMISSIONS,
        'sp500':{**{k:None for k in ('sp500_price','cape','cape_avg','gdp','wilshire','buffett_indicator','dgs10','vix','baa','aaa','hy_spread','ig_spread','t10yie','earnings_yield','ey_spread','credit_spread')},'metrics':[]},
        'gold_metals':{**{k:None for k in ('gold_price','gold','gold_avg','silver_price','platinum','gdx_price','m2','cpi','dxy','fed_balance_sheet')},'metrics':[]},
        'crypto':{**{k:None for k in ('market_cap','btc_dominance','eth_dominance','total_volume_24h')},'metrics':[]},
        'oil_commodities':{**{k:None for k in ('wti','brent','natural_gas','wti_avg')},'metrics':[]},
        'all_metrics':[],'summary':{'total_metrics':0,'overvalued_count':None,'undervalued_count':None,'fair_value_count':None,'most_overvalued':None,'most_undervalued':None},
        'dependency_graph':{'canonical_run':deepcopy(packet['replay']),'roots':['FRED:'+s for s in SERIES],
            'features':{k:v['roots'] for k,v in derivatives.items()},'independence_status':'Shared canonical macro, credit and energy roots; no additional independent investment votes.'},
        'portfolio_consequences':{'status':'EXPLICIT_SCENARIOS_ONLY',
            'formula':'Entered EPS * entered P/E gives an assumed price; signed USD exposure * (assumed price / entered current price - 1).',
            'limits':'One security and one currency, fixed units, entered assumptions only. No forecast, earnings estimate, allocation, dividends, FX, fees, financing or hedges.'},
        'methodology':{'replay':'Canonical complete bounded response originals and source definitions reconstructed under pinned compiler; histories are current-vintage revisions.',
            'periods':'GDP is an annualized quarterly flow, M2 a monthly money-stock measure, WALCL a weekly asset level; they are never added or treated as same-date inputs.',
            'energy':'Prices can be zero or negative. Brent-WTI compares distinct locations and grades; the difference is not an arbitrage or fair-value signal.',
            'statistics':'Exact sample dates and original row indices accompany descriptive means, sample standard deviations, midrank percentiles and z scores. Missing observations are not filled.',
            'decisions':'WAIT is abstention, not certification that an existing portfolio is safe. No return forecast, position size or independent vote.'}}

def build(packet,originals,legacy,generated_at):
    with localcontext() as arithmetic:
        arithmetic.prec=40;arithmetic.rounding=ROUND_HALF_EVEN
        return _build(packet,originals,legacy,generated_at)
