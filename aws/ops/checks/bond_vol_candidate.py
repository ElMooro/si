"""Pure original-source research candidate. No IO, notifications or portfolio actions."""
from copy import deepcopy
from datetime import date,datetime,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
from fractions import Fraction
import bond_vol_timezone as pinned_timezone
import hashlib,json,math,re
import report_observations as observations
from research_brief_model import clock
from bond_vol_catalog import (SERIES,SPECS,WINDOW,BASELINE,ANNUAL_STEPS,MAX_GAP_DAYS,
    MAX_OBSERVATION_DAYS,MAX_SOURCE_SECONDS,MOVE_URL,MOVE_NAMES,METHOD,METHOD_SOURCES)

CONTRACT='bond-vol-candidate.v1'
PRIVATE='audit-private/20260909-originals/bond-vol-research/'
AUTHORITY={k:False for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified',
    'point_in_time_backtest_qualified','publication_eligible')}

def original_ref(ref):
    return (isinstance(ref,dict) and bool(re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256'))))
        and ref.get('key')==PRIVATE+ref['sha256']+'.bin' and type(ref.get('bytes')) is int and 0<ref['bytes']<=64*1024*1024)

def scalar(value):
    display=float(value) if value is not None else None
    if display is not None and not math.isfinite(display):raise ValueError('Nonfinite statistic')
    return {'value':display,'calculated_decimal':str(value) if value is not None else None}

def number(value):
    if value in (None,'.',''):return None
    if not isinstance(value,str):raise ValueError('Original FRED decimal string required')
    result=Decimal(value)
    if not result.is_finite() or len(result.as_tuple().digits)>28 or not -28<=result.as_tuple().exponent<=12:
        raise ValueError('Original number outside reviewed precision')
    return result

def variance(values):
    if len(values)<2:return None
    if all(v==values[0] for v in values):return Decimal(0)
    # Exact centering avoids order-dependent rounding changing percentile ties.
    exact=[Fraction(v) for v in values];mean=sum(exact)/len(exact)
    result=sum((v-mean)**2 for v in exact)/(len(exact)-1)
    return Decimal(result.numerator)/Decimal(result.denominator)

def rolling(rows,cutoff):
    ordered=sorted((dict(row,original_row=i) for i,row in enumerate(rows) if row['date']<=cutoff),key=lambda r:r['date'])
    numeric=[dict(row,position=i,decimal=number(row.get('value'))) for i,row in enumerate(ordered) if number(row.get('value')) is not None]
    history=[]
    # O(N * window), with every returned row separately retained in the candidate.
    for end in range(WINDOW,len(numeric)):
        window=numeric[end-WINDOW:end+1];first,last=window[0],window[-1]
        gaps=[(date.fromisoformat(b['date'])-date.fromisoformat(a['date'])).days for a,b in zip(window,window[1:])]
        changes=[(b['decimal']-a['decimal'])*100 for a,b in zip(window,window[1:])]
        var=variance(changes)
        history.append({'start_date':first['date'],'end_date':last['date'],
            'start_original_row':first['original_row'],'end_original_row':last['original_row'],
            'numeric_observations':WINDOW+1,'change_count':WINDOW,
            'elapsed_calendar_days':(date.fromisoformat(last['date'])-date.fromisoformat(first['date'])).days,
            'missing_rows_inside_window':last['position']-first['position']-WINDOW,
            'max_interval_days':max(gaps),'comparable_interval_ceiling_met':max(gaps)<=MAX_GAP_DAYS,
            'sample_variance_bp2':scalar(var),'step_dispersion_bp':scalar(var.sqrt()),
            'annualized_assuming_252_steps_bp':scalar((var*ANNUAL_STEPS).sqrt())})
    return history

def distribution(history):
    prior=history[-BASELINE-1:-1] if history else [];latest=history[-1] if history else None
    out={'required_prior_windows':BASELINE,'prior_window_count':len(prior),'excludes_current':True,
        'first_prior_end_date':prior[0]['end_date'] if prior else None,'last_prior_end_date':prior[-1]['end_date'] if prior else None,
        'invalid_interval_windows':sum(not p['comparable_interval_ceiling_met'] for p in prior),
        'status':'insufficient_prior_windows','mean_bp':scalar(None),'sample_sd_bp':scalar(None),
        'z_score':scalar(None),'midrank_percentile':scalar(None),'independent_sample_size':None}
    if len(prior)!=BASELINE or latest is None:return out
    if out['invalid_interval_windows'] or not latest['comparable_interval_ceiling_met']:
        out['status']='noncomparable_intervals';return out
    vals=[Decimal(p['annualized_assuming_252_steps_bp']['calculated_decimal']) for p in prior]
    current=Decimal(latest['annualized_assuming_252_steps_bp']['calculated_decimal'])
    mean=vals[0] if all(v==vals[0] for v in vals) else sum(vals)/len(vals);sd=variance(vals).sqrt()
    out.update(status='flat_baseline' if sd==0 else 'available',mean_bp=scalar(mean),sample_sd_bp=scalar(sd),
        z_score=scalar((current-mean)/sd if sd else None),
        midrank_percentile=scalar(Decimal(100)*(sum(v<current for v in vals)+Decimal('0.5')*sum(v==current for v in vals))/len(vals)))
    return out

def move_quote(raw,receipt,generated_at):
    if raw is None:
        if receipt is not None:raise ValueError('Missing quote cannot have a success receipt')
        return {'status':'unavailable','current':None,'original':None,'receipt':None,'history':[],
            'is_proxy':False,'official_feed_parity_verified':False,**AUTHORITY}
    if not isinstance(raw,bytes) or not 0<len(raw)<=8*1024*1024:raise ValueError('Whole bounded quote response required')
    if (not isinstance(receipt,dict) or receipt.get('source_url')!=MOVE_URL or receipt.get('http_status')!=200
        or receipt.get('bytes')!=len(raw) or receipt.get('sha256')!=hashlib.sha256(raw).hexdigest()):raise ValueError('Original quote receipt differs')
    now=clock(generated_at);acquired=clock(receipt['acquired_at'])
    if acquired>now:raise ValueError('Future quote acquisition')
    try:doc=json.loads(raw,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite quote JSON')))
    except (ValueError,UnicodeDecodeError):doc=None
    out={'status':'schema_mismatch','current':None,'original':deepcopy(doc),'receipt':deepcopy(receipt),'history':[],
        'is_proxy':False,'unit':'index_points','official_feed_parity_verified':False,**AUTHORITY}
    if not isinstance(doc,dict) or not isinstance(doc.get('chart'),dict):return out
    chart=doc['chart'];results=chart.get('result')
    if chart.get('error') is not None or not isinstance(results,list) or len(results)!=1:return out
    res=results[0]
    if not isinstance(res,dict) or not isinstance(res.get('meta'),dict) or not isinstance(res.get('indicators'),dict):return out
    meta=res['meta'];stamps=res.get('timestamp');quotes=res['indicators'].get('quote')
    if not isinstance(stamps,list) or not isinstance(quotes,list) or len(quotes)!=1:return out
    if not isinstance(quotes[0],dict):return out
    values=quotes[0];closes=values.get('close')
    if not isinstance(closes,list) or any(not isinstance(v,list) or len(v)!=len(stamps) for v in values.values()):return out
    if any(type(t) is not int or not 0<=t<=253402214400 for t in stamps) or len(set(stamps))!=len(stamps):return out
    if any(c is not None and (type(c) not in (int,float) or not math.isfinite(c) or c<0) for c in closes):return out
    # A matching ticker is insufficient when the provider names another instrument.
    identity=(meta.get('symbol')=='^MOVE' and meta.get('instrumentType')=='INDEX'
        and meta.get('exchangeTimezoneName')=='America/New_York' and meta.get('dataGranularity')=='1d'
        and all(meta.get(k) in MOVE_NAMES for k in ('longName','shortName')))
    zone=pinned_timezone.new_york()
    history=[{'original_row':i,'timestamp':t,'session_date':datetime.fromtimestamp(t,timezone.utc).astimezone(zone).date().isoformat(),
        'reported_close':closes[i]} for i,t in enumerate(stamps)]
    if len({r['session_date'] for r in history})!=len(history):return out
    out.update(history=history,identity_reviewed=identity,provider_name=meta.get('longName'),
        timezone={'name':'America/New_York','iana_version':pinned_timezone.VERSION,'tzif_sha256':pinned_timezone.SHA256},
        provider_timezone=meta.get('exchangeTimezoneName'),status='identity_mismatch' if not identity else 'unavailable')
    if not identity or not history:return out
    latest=max(history,key=lambda r:r['timestamp']);out['last_observed']=deepcopy(latest)
    source_day=acquired.astimezone(zone).date();age=(now.astimezone(zone).date()-date.fromisoformat(latest['session_date'])).days
    out['status']=('provisional_or_future_session' if latest['session_date']>=str(source_day) else
        'missing_latest_close' if latest['reported_close'] is None else
        'stale_source' if (now-acquired).total_seconds()>MAX_SOURCE_SECONDS else
        'stale_observation' if not 0<=age<=MAX_OBSERVATION_DAYS else 'quoted_previous_session')
    if out['status']=='quoted_previous_session':out['current']=deepcopy(latest)
    return out

def build(source,originals,generated_at,context,predecessor,quote_raw=None,quote_receipt=None):
    if (source.get('contract')!=observations.CONTRACT or source.get('calls_eligible') is not False
        or source.get('sizing_eligible') is not False or set(originals)!=set(SERIES)):
        raise ValueError('Complete canonical input inventory required')
    ref=source.get('replay') or {}
    if (not re.fullmatch(r'data/report-research/runs/[a-f0-9]{64}\.json',ref.get('manifest_key',''))
        or ref.get('output_sha256')!=observations.digest({k:v for k,v in source.items() if k!='replay'})):
        raise ValueError('Exact canonical source identity required')
    if (not original_ref(predecessor) or context.get('source_key')!='data/funding-plumbing.json' or context.get('independent_votes')!=0
        or context.get('status') not in ('retained_unqualified_context','missing')
        or (context.get('status')=='retained_unqualified_context' and not original_ref(context.get('original')))):
        raise ValueError('Preserved predecessor and complete unqualified funding context required')
    if context.get('status')=='missing' and context.get('original') is not None:raise ValueError('Missing context cannot have an original')
    now=clock(generated_at);source_time=clock(source['generated_at'])
    if now<source_time:raise ValueError('Future source publication')
    result={}
    with localcontext() as arithmetic:
        arithmetic.prec=72;arithmetic.rounding=ROUND_HALF_EVEN
        for sid in SERIES:
            original=originals[sid];m=source.get('measurements',{}).get(sid)
            if (original is None)!=(m is None):raise ValueError('Source availability differs: '+sid)
            rows=[];definition={};acquired=None;last=None;state='unavailable';history=[];acq_age=obs_age=None;reviewed=False
            if original is not None:
                with localcontext() as canonical_precision:
                    canonical_precision.prec=28;canonical_precision.rounding=ROUND_HALF_EVEN
                    rebuilt=observations.measurement(sid,original['definition'],original['observations'],original['evidence'],source['generated_at'],original['acquired_at'])
                if rebuilt!=m:raise ValueError('Canonical reconstruction differs: '+sid)
                rows=deepcopy(original['observations']['observations']);definition=deepcopy(original['definition']['seriess'][0]);seen=set()
                for row in rows:
                    day=date.fromisoformat(row['date'])
                    if str(day)!=row['date'] or day in seen:raise ValueError('Duplicate or noncanonical source date')
                    seen.add(day);number(row.get('value'))
                reviewed=tuple(definition.get(k) for k in ('units','frequency_short','frequency','seasonal_adjustment'))==SPECS[sid]['reviewed_definition']
                acquired=original['acquired_at'];acquired_clock=clock(acquired);acq_age=(now-acquired_clock).total_seconds()
                if acquired_clock>source_time:raise ValueError('Acquisition after source publication')
                eligible=[row for row in rows if row['date']<=str(acquired_clock.date())]
                last=max(eligible,key=lambda r:r['date']) if eligible else None
                obs_age=(now.date()-date.fromisoformat(last['date'])).days if last else None
                state=('definition_mismatch' if not reviewed else 'unavailable' if last is None or number(last.get('value')) is None else
                    'stale_observation' if not 0<=obs_age<=MAX_OBSERVATION_DAYS else
                    'stale_source' if not 0<=acq_age<=MAX_SOURCE_SECONDS or (now-source_time).total_seconds()>MAX_SOURCE_SECONDS else
                    'source_unusable' if m['quality']['status']!='fresh' else 'within_age_ceiling')
                history=rolling(rows,str(acquired_clock.date())) if reviewed else []
            prior=distribution(history);latest=history[-1] if history else None
            usable=bool(state=='within_age_ceiling' and latest and latest['end_date']==last['date'] and latest['comparable_interval_ceiling_met'])
            result[sid]={'series_id':sid,**deepcopy(SPECS[sid]),'source_definition':definition or None,'definition_reviewed':reviewed,
                'acquired_at':acquired,'provider_updated_at':definition.get('last_updated'),'latest_observation':deepcopy(last),
                'original_rows':rows,'rolling_history':history,'last_calculated':deepcopy(latest),'historical_distribution':prior,
                'current':deepcopy(latest) if usable else None,'current_distribution':deepcopy(prior) if usable else None,
                'quality':{'status':state,'observation_age_days':obs_age,'acquisition_age_seconds':acq_age,
                    'current_dispersion_available':usable,'max_observation_age_days':MAX_OBSERVATION_DAYS,
                    'max_acquisition_age_seconds':MAX_SOURCE_SECONDS,'release_calendar_verified':False},
                'coverage':deepcopy((m or {}).get('coverage')),'evidence':deepcopy((original or {}).get('evidence',{})),**AUTHORITY}
        move=move_quote(quote_raw,quote_receipt,generated_at)
    current=sum(r['current'] is not None for r in result.values())
    return {'contract':CONTRACT,'candidate_only':True,'generated_at':generated_at,'source_generated_at':source['generated_at'],
        'source_replay':deepcopy(ref),'series':result,'move':move,'funding_context':deepcopy(context),'predecessor':deepcopy(predecessor),
        'quality':{'status':'complete_descriptive_rates' if current==len(SERIES) else 'degraded','current_series':current,
            'total_series':len(SERIES),'original_rows':sum(len(r['original_rows']) for r in result.values()),
            'move_status':move['status']},'methodology':deepcopy(METHOD),'method_sources':list(METHOD_SOURCES),
        'dependency_graph':{'series_to_engine':list(SERIES),'series_roots':{s:list(SPECS[s]['dependency_roots']) for s in SERIES},
            'independent_votes':0,'statistical_independence_qualified':False},
        'composite_z_score':None,'regime':None,'call':None,'signals':[],'decision':{'verb':'WAIT','meaning':'abstain'},
        'portfolio_consequences':{'target_weights':None,'reason':'Descriptive source statistics lack validated predictive edge and portfolio risk inputs.'},**AUTHORITY}
