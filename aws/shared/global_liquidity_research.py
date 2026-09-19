"""Reproducible, native-unit three-bank balance-sheet research, without forecasts."""
from copy import deepcopy
from datetime import timedelta
import re
from report_observations import measurement, liquidity
from research_brief_model import clock, digest, encoded, row_status, SOURCE_CONTRACT
from global_liquidity_calendar import POLICY, effective_date, series_rows, subtotal, endpoint_change

CONTRACT='global-liquidity-research.v1'
SERIES=('WALCL','WTREGEN','RRPONTSYD','ECBASSETSW','JPNASSETS','DEXUSEU','DEXJPUS','M2SL')
REASON='These balance-sheet and money-supply measurements establish no calibrated return forecast or portfolio allocation. WAIT means abstain, not liquidate existing holdings.'


def build(source, originals, generated_at, legacy_context=None):
    now=clock(generated_at);ref=source.get('replay') or {}
    if source.get('contract')!=SOURCE_CONTRACT or not re.fullmatch(r'data/report-research/runs/[a-f0-9]{64}\.json',ref.get('manifest_key','')):
        raise ValueError('canonical macro source and replay required')
    if digest({k:v for k,v in source.items() if k!='replay'})!=ref.get('output_sha256'):
        raise ValueError('canonical macro content binding differs')
    age=(now-clock(source['generated_at'])).total_seconds()
    if age<0:raise ValueError('future macro source')
    rows={};histories={};fresh=0
    for sid in SERIES:
        row=deepcopy(source.get('measurements',{}).get(sid) or {})
        if row:
            original=originals.get(sid)
            if not original:raise ValueError('original input missing: '+sid)
            rebuilt=measurement(sid,original['definition'],original['observations'],original['evidence'],source['generated_at'],original['acquired_at'])
            if rebuilt!=row:raise ValueError('measurement differs from original: '+sid)
            if sid in POLICY:histories[sid]=series_rows(sid,original,row)
        status=row_status(row,now,age) if row else 'unavailable'
        if sid=='JPNASSETS' and row.get('date'):
            observed=effective_date(sid,row['date']);row['effective_observation_date']=observed.isoformat()
            if observed>now.date():status='incomplete_measurement_period'
        usable=status=='fresh';fresh+=usable
        row.update(series_id=sid,available=usable,quality={'status':status,'evaluated_at':generated_at},
                   last_observed_value=row.get('current_decimal'),calls_eligible=False,sizing_eligible=False)
        if not usable:row.update(current=None,current_decimal=None,historical_changes=row.get('changes',{}),changes={})
        if not row.get('name'):row['error']=source.get('errors',{}).get(sid,'source_not_collected')
        rows[sid]=row
    # Current values require independently fresh source rows. Historical charts
    # remain explicitly dated current-vintage research even when inputs expire.
    current=subtotal({sid:doc for sid,doc in histories.items() if rows[sid]['available']},now.date().isoformat())
    current['source_ineligible']=[sid for sid in POLICY if not rows[sid]['available']]
    current['unit']='USD_millions'
    current['period_basis']='One valuation date, asynchronously observed stocks and dated FX. BOJ is month-end; each selected observation and carry age is shown.'
    end=now.date()-timedelta(days=(now.weekday()-4)%7)
    try:start=end.replace(year=end.year-5)
    except ValueError:start=end.replace(year=end.year-5,day=28)
    start+=timedelta(days=(4-start.weekday())%7)
    history={};cursor=start
    while cursor<=end:
        day=cursor.isoformat();history[day]=subtotal(histories,day);cursor+=timedelta(weeks=1)
    windows={str(weeks)+'w':endpoint_change(history,end.isoformat(),weeks) for weeks in (13,52)}
    proxy=liquidity(rows)
    proxy['basis']='WALCL Wednesday stock minus WTREGEN weekly average minus RRP daily operation amount, normalized once. Mixed-date proxy, not investable cash.'
    m2=rows['M2SL']
    monetary={'series_id':'M2SL','current_native_decimal':m2.get('current_decimal'),'unit':m2.get('unit'),
        'observation_period':m2.get('date'),'year_comparison':m2.get('changes',{}).get('year'),
        'status':'descriptive' if m2['available'] else 'unavailable','growth_acceleration':None,
        'scope':'US M2 is a money-supply aggregate, not a central-bank balance sheet; positive year-over-year growth alone is not acceleration.'}
    quality={'status':'fresh' if fresh==len(SERIES) and current['status']=='descriptive' else 'degraded' if fresh else 'unavailable',
             'fresh_series':fresh,'expected_series':len(SERIES),'subtotal_status':current['status'],
             'basis':'Original observations and acquisition ceilings; historical release timing is not established.'}
    return {'contract':CONTRACT,'engine':'justhodl-global-liquidity','version':'2.0.0','generated_at':generated_at,
        'source_generated_at':source['generated_at'],'source_replay':ref,'series':rows,'quality':quality,
        'three_bank_subtotal':current,'us_net_liquidity_proxy':proxy,'us_m2':monetary,
        'calendar_research':{'status':'CURRENT_VINTAGE_RESEARCH','sampling':'Weekly Friday observation-date valuation',
            'start':start.isoformat(),'end':end.isoformat(),'expected_weekly_slots':len(history),
            'complete_weekly_slots':sum(v['status']=='descriptive' for v in history.values()),'history':history,'latest_changes':windows,
            'point_in_time':False,'historical_feature_replay_ready':False,'calls_eligible':False,'sizing_eligible':False,
            'scope':'Reconstruction from current-retrieved provider histories, including later revisions. Observation dates are not release dates or historical system-possession times.'},
        'dependency_groups':{'fed_h41':['WALCL','WTREGEN'],'fed_rrp_operations':['RRPONTSYD'],
            'ecb_weekly_statement':['ECBASSETSW'],'boj_accounts':['JPNASSETS'],'fed_h10_fx':['DEXUSEU','DEXJPUS'],'fed_h6_money':['M2SL']},
        'dependency_note':'The Fed stock also appears in the US proxy; these are overlapping measurements, not separate independent votes.',
        'decision':{'verb':'WAIT','meaning':'abstain','reason':REASON},'call':None,'regime':'UNQUALIFIED',
        'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'publication_eligible':False,
        'portfolio_consequences':{'status':'UNAVAILABLE','target_weights':None,'expected_return':None,'forced_liquidation':False,'reason':REASON},
        'global_liquidity_index':{'total_usd_bn':None,'total_usd_trillions':None,'change_13w_pct':None,'change_52w_pct':None,'components_usd_bn':{},'status':'SUPERSEDED_BY_SCOPED_RESEARCH'},
        'fed_net_liquidity':{'value_usd_bn':None,'value_usd_trillions':None,'change_13w_pct':None,'change_52w_pct':None},
        'global_impulse_13w_pct':None,'global_impulse_52w_pct':None,'m2_yoy_pct':None,
        'broad_money':{'m2_yoy_pct':None,'us_m2_yoy_pct':None,'as_of':None,'read':REASON,'regime':'UNQUALIFIED'},'legacy_context':legacy_context,
        'regime_read':REASON,'method':'original_bound_three_bank_research','history_points':[],'net_liq_points':[],
        'wl_research':{'status':'UNQUALIFIED_LEGACY','reason':'Prior related context is retained in the immutable legacy packet; it does not vote.'},
        'methodology':'Official native units; JPNASSETS is measured at month-end, with 100 million JPY per native unit. '
            'USD conversions use DEXUSEU (USD per EUR) and reciprocal DEXJPUS (JPY per USD). '
            'Each date uses only observations on or before it, within published carry limits; explicit missing observations remain missing. '
            '13/52-week changes use exactly 91/364-day endpoints. Balance and FX contributions are a disclosed arithmetic decomposition, not causal flows.',
        'validation_status':'DESCRIPTIVE_RESEARCH_ONLY','paid_ai_calls':0,'notifications_sent':0}
