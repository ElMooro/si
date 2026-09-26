"""Pure dated Sentinel calculations; original histories required, no investment authority."""
import math,json,hashlib
from datetime import datetime,timezone
CONTRACT='us10y-sentinel-research.v1'
PREFIX='data/us10y-sentinel-research/'
CURRENT='data/us10y-sentinel.json'
TIERS=[(5.00,'CRITICAL'),(4.75,'RED'),(4.50,'HIGH'),(4.25,'ELEVATED'),(4.00,'WATCH'),(-99,'BENIGN')]
TIER_ORDER=['BENIGN','WATCH','ELEVATED','HIGH','RED','CRITICAL']
SERIES={'DGS10':{'start':'1962-01-01','unit':'Percent','frequency':'Daily'},
        'DFII10':{'start':'2003-01-01','unit':'Percent','frequency':'Daily'},
        'SP500':{'start':'1962-01-01','unit':'Index','frequency':'Daily, Close'}}
def clock(value):
    if not isinstance(value,str):raise ValueError('Dated acquisition required')
    parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:raise ValueError('Timezone required')
    return parsed.astimezone(timezone.utc)
def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(value):return hashlib.sha256(encoded(value)).hexdigest()

def tier_of(level):
    for thr, name in TIERS:
        if level >= thr:
            return name
    return "BENIGN"


def clean_daily(rows, positive=False):
    clean={}
    for d,v in rows:
        try:
            datetime.strptime(d,'%Y-%m-%d')
            if math.isfinite(v) and (not positive or v>0):clean[d]=v
        except (ValueError,TypeError):pass
    return sorted(clean.items())


def episode_study(dgs10, spx):
    """Dated daily price-only studies; absent coverage cannot jump decades ahead."""
    from bisect import bisect_left
    from statistics import median
    yields=clean_daily(dgs10)
    prices=clean_daily(spx,positive=True)
    dates=[datetime.strptime(d,'%Y-%m-%d').date() for d,_ in prices]

    def fwd(d0,n):
        trigger=datetime.strptime(d0,'%Y-%m-%d').date()
        i=bisect_left(dates,trigger)
        if i>=len(dates) or (dates[i]-trigger).days>4:
            return None,'OUTSIDE_PRICE_COVERAGE',None
        if i+n>=len(dates):return None,'HORIZON_NOT_COMPLETE',None
        window=dates[i:i+n+1]
        if any((b-a).days>7 for a,b in zip(window,window[1:])):
            return None,'NON_DAILY_OR_GAPPED_PRICE_HISTORY',None
        if (window[-1]-window[0]).days>n*2+7:
            return None,'HORIZON_DATE_MISMATCH',None
        value=(prices[i+n][1]/prices[i][1]-1)*100
        if not math.isfinite(value) or abs(value)>100:
            return None,'QUARANTINED_IMPLAUSIBLE_SPX_RETURN',None
        return round(value,2),'VALID',{'series_id':'SP500','start_date':prices[i][0],'start_value':prices[i][1],
            'end_date':prices[i+n][0],'end_value':prices[i+n][1],'subsequent_observations':n,
            'elapsed_calendar_days':(dates[i+n]-dates[i]).days,'formula':'(end_value / start_value - 1) * 100',
            'unit':'percent','return_basis':'price-only; dividends excluded'}

    out={}
    ys=[v for _,v in yields]
    for thr in (4.50,4.75,5.00):
        eps=[]
        for i in range(250,len(yields)):
            if ys[i]<thr or max(ys[i-250:i])>=thr:continue
            window=[datetime.strptime(d,'%Y-%m-%d').date() for d,_ in yields[i-250:i+1]]
            if any((b-a).days>7 for a,b in zip(window,window[1:])):continue
            d0=yields[i][0]
            row={'date':d0,'y':round(ys[i],2),'return_basis':'SP500 price-only; dividends excluded','quality':{},'return_inputs':{}}
            for label,n in (('1w',5),('1m',21),('3m',63)):
                value,status,inputs=fwd(d0,n)
                row['spx_'+label]=value;row['quality'][label]=status;row['return_inputs'][label]=inputs
            eps.append(row)
        r3=[e['spx_3m'] for e in eps if e['spx_3m'] is not None]
        r1=[e['spx_1m'] for e in eps if e['spx_1m'] is not None]
        out['cross_%.2f'%thr]={
            'n':len(eps),'episodes':eps,'episodes_complete':True,'n_valid_1m':len(r1),'n_valid_3m':len(r3),'minimum_summary_n':3,
            'median_spx_1m':round(median(r1),2) if len(r1)>=3 else None,
            'median_spx_3m':round(median(r3),2) if len(r3)>=3 else None,
            'neg_3m_hit_rate_pct':round(100*sum(x<0 for x in r3)/len(r3),1) if len(r3)>=3 else None,
            'status':'DESCRIPTIVE_SMALL_SAMPLE' if len(r3)>=3 else 'INSUFFICIENT_VALID_EPISODES',
            'source':'FRED SP500 daily close','return_basis':'price-only; excludes dividends',
            'price_start':prices[0][0] if prices else None,'price_end':prices[-1][0] if prices else None,
            'methodology_version':'daily-price-episodes.v2',
            'note':'250 prior daily yield observations below threshold; 5/21/63 subsequent daily equity closes. '
                   'Date gaps checked; no returns outside price coverage. Current-vintage descriptive study, not a forecast.'}
    return out


def observed_change(rows, intervals):
    """Dated endpoints for an observation-count change, never calendar days."""
    out={'series_id':'DGS10','intervals':intervals,'basis':'valid yield observations',
         'value':None,'unit':'basis_points','start_date':None,'end_date':None,
         'start_value':None,'end_value':None,'elapsed_calendar_days':None,
         'formula':'(end_value - start_value) * 100','status':'insufficient_observations'}
    if len(rows)<intervals+1:return out
    start,end=rows[-intervals-1],rows[-1]
    out.update(value=round((end[1]-start[1])*100,1),start_date=start[0],end_date=end[0],
        start_value=start[1],end_value=end[1],elapsed_calendar_days=(datetime.strptime(end[0],'%Y-%m-%d')-
        datetime.strptime(start[0],'%Y-%m-%d')).days,status='descriptive')
    return out


def observation_quality(d, generated_at):
    try:age=(clock(generated_at).date()-datetime.strptime(d,'%Y-%m-%d').date()).days
    except (ValueError,TypeError):age=None
    return {'status':'unavailable' if age is None else 'invalid' if age<0 else 'stale' if age>7 else 'fresh',
            'observation_date':d,'max_age_days':7,'age_days':age}


def corr(a, b):
    n = min(len(a), len(b))
    if n < 20:
        return None
    a, b = a[-n:], b[-n:]
    ma, mb = sum(a) / n, sum(b) / n
    cov = sum((x - ma) * (y - mb) for x, y in zip(a, b))
    va = math.sqrt(sum((x - ma) ** 2 for x in a))
    vb = math.sqrt(sum((y - mb) ** 2 for y in b))
    return round(cov / (va * vb), 3) if va and vb else None



def build(histories, stamp, previous_tier, bus, evidence, duration_s=None):
    clock(stamp)
    if set(histories)!=set(SERIES) or set(evidence)!=set(SERIES):raise ValueError('All original series required')
    dgs10=histories['DGS10'];dfii=histories['DFII10'];spx=histories['SP500']
    if not dgs10:raise ValueError('No usable Treasury yield observations')
    # FRED constant-maturity yield is a daily observation, not an intraday quote.
    fred_lvl = round(dgs10[-1][1],3)
    live_src = "FRED_DGS10_daily"
    level = fred_lvl
    source_quality = observation_quality(dgs10[-1][0], stamp)
    ys = [v for _, v in dgs10]
    since90 = [v for d, v in dgs10 if d >= "1990-01-01"]
    pct_rank = round(100 * sum(1 for v in since90 if v <= level)
                     / max(1, len(since90)), 1)
    change20,change60=observed_change(dgs10,20),observed_change(dgs10,60)
    for change in (change20,change60):change['current_usable']=source_quality['status']=='fresh' and change['value'] is not None
    d20,d60=change20['value'],change60['value']

    tier = tier_of(level)
    bumped = False
    if d60 is not None and d60 >= 50 and tier != "CRITICAL":
        i = TIER_ORDER.index(tier)
        if i >= 1:  # only bump when already WATCH+
            tier = TIER_ORDER[min(i + 1, len(TIER_ORDER) - 1)]
            bumped = True

    dfii=clean_daily(dfii)
    real10 = round(dfii[-1][1],2) if dfii and observation_quality(dfii[-1][0], stamp)["status"]=="fresh" else None
    dist_bps = round((5.00 - level) * 100, 1)

    # Up to 60 matched daily price-return/yield-change observations.
    ymap = dict(dgs10)
    rets, dys, pairs = [], [], []
    spx_recent = spx[-130:]
    for i in range(1, len(spx_recent)):
        d1, p1 = spx_recent[i]
        d0, p0 = spx_recent[i - 1]
        if d1 in ymap and d0 in ymap and p0 and (datetime.strptime(d1,"%Y-%m-%d")-datetime.strptime(d0,"%Y-%m-%d")).days<=7:
            rets.append(p1 / p0 - 1)
            dys.append(ymap[d1] - ymap[d0])
            pairs.append({'start_date':d0,'end_date':d1,'start_price':p0,'end_price':p1,
                'start_yield_percent':ymap[d0],'end_yield_percent':ymap[d1],
                'price_return_fraction':rets[-1],'yield_change_percentage_points':dys[-1]})
    c60 = corr(rets[-60:], dys[-60:])
    equity_quality=observation_quality(spx[-1][0] if spx else None, stamp)
    pair_quality=observation_quality(pairs[-1]['end_date'] if pairs else None,stamp)
    if equity_quality['status']!='fresh' or source_quality['status']!='fresh' or pair_quality['status']!='fresh':
        c60=None
    negative_association = (c60 <= -0.30) if c60 is not None else None

    change_text='unavailable' if d60 is None else '%+.1f basis points' % d60
    reason = ('10Y %.2f%% observed %s; 60-observation yield change: %s%s; reported real 10Y: %s. '
              'Yield bands are uncalibrated review context, not a crisis probability or allocation instruction.'
              % (level,dgs10[-1][0],change_text,' (heuristic velocity bump)' if bumped else '',
                 ('%.2f%% observed %s' % (real10,dfii[-1][0])) if real10 is not None else 'unavailable'))
    if real10 is not None and real10 >= 2.25:
        reason += " (elevated real-yield review threshold, uncalibrated)"
    if negative_association:
        reason += " · negative equity-return/yield-change association; correlation does not establish causation"

    eps = episode_study(dgs10, spx) if spx else {}

    out = {
        "schema": "2.2", "engine": "justhodl-us10y-sentinel",
        "as_of": stamp,
        "generated_at": stamp,
        "quality": source_quality, "methodology_version": "daily-price-episodes.v2",
        "calibration_status": "HEURISTIC_REVIEW_ONLY", "execution_eligible": False, "calls_eligible":False,
        "sizing_eligible":False, "forecast_qualified":False, "call": None,
        "level": level, "level_source": live_src,
        "fred_close": fred_lvl, "fred_date": dgs10[-1][0],
        "distance_to_5pct_bps": dist_bps,
        "pct_rank_since_1990": pct_rank,
        "velocity": {"d20_bps": d20, "d60_bps": d60,
                     "velocity_bump": bumped, "comparisons": {"20_observations":change20,"60_observations":change60}},
        "real_10y": real10,
        "tier": tier, "tier_reason": reason,
        "prev_tier": previous_tier,
        "corr60_spx_vs_dy": c60,
        "yields_driving_stocks": None,
        "negative_equity_yield_association": negative_association,
        "correlation_observations": min(60,len(rets)),
        "correlation_trace":{'observations':pairs[-60:],'matched_end_quality':pair_quality,
            'method':'Pearson correlation of paired simple price returns and yield changes; at least 20 pairs.',
            'interpretation':'Descriptive association; no causal or forecast qualification.'},
        "equity_quality": equity_quality,
        "real_10y_date": dfii[-1][0] if dfii else None,
        "episode_study": eps,
        "ladder": [{"thr": t, "name": n} for t, n in TIERS if t > 0],
        "history_260d": [{"d": d, "v": round(v, 3)}
                         for d, v in dgs10[-260:]],
        "duration_s":duration_s,"duration_scope":"Source acquisition and retention before deterministic calculation; not total Lambda runtime.",
    }

    if source_quality['status'] != 'fresh':
        out.update(level=None, tier='UNKNOWN', distance_to_5pct_bps=None,
                   pct_rank_since_1990=None, tier_reason='Daily Treasury yield unavailable or stale.',
                   corr60_spx_vs_dy=None, negative_equity_yield_association=None,
                   velocity={'d20_bps':None,'d60_bps':None,'velocity_bump':False,
                             'comparisons':{'20_observations':change20,'60_observations':change60}})
    out.update(alert_sent=False,alert_eligible=False,
        alert_reason='No independently qualified Sentinel threshold-alert or allocation policy is registered.')

    out.update(contract=CONTRACT, decision={'verb':'WAIT','meaning':'abstain'},
        point_in_time_backtest_qualified=False, histories=histories, original_sources=evidence,
        bus_cross=bus, evidence_ancestry={'DGS10':'Federal Reserve H.15','DFII10':'Federal Reserve H.15',
            'SP500':'S&P Dow Jones Indices via FRED'}, independent_investment_votes=0,
        portfolio_consequences={'status':'UNAVAILABLE','target_weights':None,
            'reason':'Descriptive yield and equity-price study has no qualified forecast or portfolio mandate.'})
    return out
