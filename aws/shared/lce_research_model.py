"""Reproducible liquidity/credit measurements, without heuristic portfolio authority."""
from copy import deepcopy
from datetime import date,datetime,timezone
import math
import re
from statistics import mean,pstdev

from lce_research_catalog import SERIES
from report_observations import measurement,months_before
from research_brief_model import clock,digest,encoded,number,row_status,SOURCE_CONTRACT
from macro_donor_inputs import ciss_context,repo_context

CONTRACT='liquidity-credit-research.v1'


def narrative_context(packet,now=None):
    """Bounded, dated context for text consumers; no unit guessing or advice."""
    now=now or datetime.now(timezone.utc)
    out={'status':'unavailable','call':None,'calls_eligible':False,'sizing_eligible':False,
         'measurements':[],'reason':'Original-source measurements only; no forecast, crisis probability or portfolio authority.'}
    try:
        ref=packet['replay']
        if packet.get('contract')!=CONTRACT or not re.fullmatch(r'data/lce-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']):return out
        if digest({k:v for k,v in packet.items() if k!='replay'})!=ref['output_sha256']:return out
        age=(now-clock(packet['source_generated_at'])).total_seconds()
        packet_age=(now-clock(packet['generated_at'])).total_seconds()
        if age<0 or packet_age<0 or packet_age>26*3600:return out
        for sid in SERIES:
            row=(packet.get('series') or {}).get(sid) or {}
            state=row_status({'contract':SOURCE_CONTRACT,'date':row.get('latest_date'),
                'frequency':row.get('frequency'),'acquired_at':row.get('acquired_at'),
                'current_decimal':row.get('latest_value_decimal'),'quality':row.get('quality'),
                'evidence':row.get('evidence')},now,age)
            out['measurements'].append({'series_id':sid,'definition':row.get('_label'),
                'value':row.get('latest_value_decimal') if state=='fresh' else None,
                'unit':row.get('_units'),'observation_date':row.get('latest_date'),
                'acquired_at':row.get('acquired_at'),'status':state})
        out.update(status='research_only',source_replay=ref,generated_at=packet['generated_at'])
    except (KeyError,TypeError,ValueError,AttributeError):
        out.update(status='unavailable',measurements=[])
    return out


def statistics(original,row,years):
    """Current-vintage population z-score, only when history spans the cutoff.

    Two distinct numeric values make this statistic mathematically defined;
    neither that condition nor the dated span establishes statistical inference.
    Missing observations are retained in the coverage count, not filled.
    """
    latest=date.fromisoformat(row['date']);target=months_before(latest,12*years)
    observations=original['observations']['observations']
    dated=[(date.fromisoformat(o['date']),number(o.get('value'))) for o in observations if o['date']<=row['date']]
    before=[d for d,v in dated if d<=target]
    tolerance={'D':4,'W':6,'BW':13,'M':31,'Q':92,'SA':184,'A':366}[row['frequency']]
    complete=bool(before) and (target-max(before)).days<=tolerance
    inside=[(d,v) for d,v in dated if target<=d<=latest]
    values=[float(v) for d,v in inside if v is not None]
    out={'window_years':years,'target_date':target.isoformat(),'end_date':row['date'],
         'start_date':min((d.isoformat() for d,v in inside),default=None),
         'numeric_observations':len(values),'missing_observations':sum(v is None for d,v in inside),
         'window_covered':complete,'z':None,'population_mean':None,'population_sd':None,
         'definition':'(latest - population mean) / population standard deviation; latest included; original current retrieved vintage; no predictive probability'}
    if not complete:out['status']='insufficient_history';return out
    if len(values)<2:out['status']='insufficient_numeric_observations';return out
    avg=mean(values);sd=pstdev(values)
    out.update(population_mean=avg,population_sd=sd)
    if sd==0:out['status']='zero_variance';return out
    value=(float(row['current_decimal'])-avg)/sd
    if not math.isfinite(value):out['status']='nonfinite_statistic';return out
    out.update(status='descriptive',z=value)
    return out


def build(source,ciss,repo,originals,generated_at):
    now=clock(generated_at)
    if not isinstance(source,dict) or source.get('contract')!=SOURCE_CONTRACT:
        raise ValueError('canonical macro packet required')
    ref=source.get('replay') or {}
    if not re.fullmatch(r'data/report-research/runs/[a-f0-9]{64}\.json',str(ref.get('manifest_key',''))):
        raise ValueError('macro source replay required')
    if digest({k:v for k,v in source.items() if k!='replay'})!=ref.get('output_sha256'):
        raise ValueError('macro content binding differs')
    age=(now-clock(source['generated_at'])).total_seconds()
    if age<0:raise ValueError('future macro source')
    rows={};categories={};reference={};fresh=0
    for sid,catalog in SERIES.items():
        row=deepcopy(source.get('measurements',{}).get(sid,{}))
        if row:
            original=originals.get(sid)
            if not isinstance(original,dict):raise ValueError('original input missing: '+sid)
            rebuilt=measurement(sid,original['definition'],original['observations'],original['evidence'],
                                source['generated_at'],original['acquired_at'])
            if rebuilt!=row:raise ValueError('measurement differs from original source: '+sid)
        state=row_status(row,now,age) if row else 'unavailable'
        usable=state=='fresh';fresh+=usable
        item={'series_id':sid,'available':usable,'quality':{'status':state,'evaluated_at':generated_at},
              '_category':catalog['category'],'_label':row.get('name') or sid,
              'requested_label':catalog['display_name'],'_units':row.get('unit'),
              'frequency':row.get('frequency'),'seasonal_adjustment':row.get('seasonal_adjustment'),
              'latest_date':row.get('date'),'latest_value':row.get('current') if usable else None,
              'latest_value_decimal':row.get('current_decimal') if usable else None,
              'latest_value_raw':row.get('current') if usable else None,
              'last_observed_value':row.get('current_decimal'),
              'unit_policy':'Official native FRED units; no category-wide millions/billions conversion',
              'acquired_at':row.get('acquired_at'),'published_at':None,
              'source_definition':row.get('definition'),'provider_updated_at':row.get('provider_updated_at'),
              'evidence':row.get('evidence'),'source_row':row.get('current_row_index'),
              'source_replay':ref,'calendar_comparisons':deepcopy(row.get('changes',{})),
              'history':deepcopy(row.get('history',[])), 'history_scope':row.get('embedded_history_scope'),
              'n_observations':row.get('coverage',{}).get('eligible_observations',0),
              'signal':None,'signal_reason':'No independently validated threshold classification',
              'calls_eligible':False,'sizing_eligible':False,'call':None,'z_1y':None,'z_5y':None,
              'statistics':{},'error':None if row else source.get('errors',{}).get(sid,'source_not_collected')}
        for old,new in [('wow','week'),('mom','month'),('qoq','quarter'),('yoy','year')]:
            change=item['calendar_comparisons'].get(new) or {}
            item[old+'_pct']=change.get('pct_change') if usable else None
        if usable:
            for years in (1,5):
                stat=statistics(originals[sid],row,years)
                item['statistics'][str(years)+'y']=stat;item['z_'+str(years)+'y']=stat['z']
        if not usable:
            # Retain old changes as source history, but none can become a current headline.
            item['historical_calendar_comparisons']=item['calendar_comparisons'];item['calendar_comparisons']={}
        rows[sid]=item;categories.setdefault(catalog['category'],[]).append(sid)
        if sid in ('DGS1','DGS2','DGS5','DGS10','DGS30'):reference[sid]=item['latest_value']
    ciss_donor=ciss_context(ciss,now);repo_donor=repo_context(repo,now)
    reason='These measurements do not establish a validated return forecast, portfolio action or target weight.'
    return {'contract':CONTRACT,'schema_version':'2.0','generated_at':generated_at,
        'source_generated_at':source['generated_at'],'source_replay':ref,
        'series':rows,'by_category':categories,'reference':reference,
        'regime':'UNAVAILABLE','composite':{'score':None,'base_score':None,'n_firing':None,'n_assessed':fresh,
            'by_category':{},'calibration_status':'UNVALIDATED','reason':reason},
        'transitions':[],'call':None,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'decision':{'verb':'WAIT','meaning':'abstain','reason':reason},
        'quality':{'status':'fresh' if fresh==len(SERIES) else 'degraded' if fresh else 'unavailable',
            'expected_series':len(SERIES),'fresh_series':fresh,'unavailable_or_stale':len(SERIES)-fresh,
            'basis':'Original responses, official metadata and bounded observation/acquisition ages; exact release calendar not verified'},
        'data_status':'PARTIAL' if fresh!=len(SERIES) else 'MEASUREMENTS_AVAILABLE',
        'ciss_systemic':ciss_donor['source_context'],
        'donor_inputs':{'ciss_stress':ciss_donor,'repo_market':repo_donor,'base_score':None,'review_score':None,
            'score_rule':'No donor score contributes to a forecast or portfolio weight',
            'calibration_status':'UNVALIDATED','execution_eligible':False,
            'repo_scope':'Retained descriptive donor; original repo source and calibration qualification remain pending'},
        'interpretation':{'as_of':generated_at,'regime':None,'composite_score':None,'overall_posture':'WAIT',
            'confidence':None,'pillars':{},'cross_asset':{},'target_allocation':[],'avoid':[],'hedges':[],
            'key_risks':['Current retrieved vintages are not historical publication-time observations.',
                         'HQM spot yields and Treasury constant-maturity yields do not establish an executable matched credit spread.',
                         'Threshold calibration and portfolio-specific risk consequences require separate validation.'],
            'decisive_call':'WAIT — abstain. Dated source observations do not authorize a portfolio action.',
            'portfolio_consequences':{'status':'UNAVAILABLE','reason':reason}},
        'source_qualification_scope':'FRED observations and definitions plus canonical ECB CISS; repo donor remains unverified context',
        'validation_status':'DESCRIPTIVE_MEASUREMENTS_ONLY',
        'scope':'Original-source measurements and descriptive current-vintage statistics. No historical point-in-time, calibrated forecast or allocation claim.'}
