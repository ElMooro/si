"""Offline behavioral/ablation tests for equity donor effects and producer units.
Loads the actual pure producer functions without constructing cloud clients.
"""
import ast
import copy
import io
import json
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'aws/shared'))
from equity_donor_inputs import (load_inputs,safe_evidence,stock_context,conviction_members,
    annotate_book,true_flow_rows,sector_flow_context,constrain_sizes,firm_board_contract)
from capital_contract import CRITICAL_SLAS,authority_expiry
NOW=datetime(2026,9,9,12,tzinfo=timezone.utc)
TS=NOW.isoformat()

def producer_functions(engine,names,scope=None):
    path=ROOT/'aws/lambdas'/('justhodl-'+engine)/'source/lambda_function.py'
    tree=ast.parse(path.read_text())
    nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name in names]
    assert {n.name for n in nodes}==set(names)
    scope=dict(scope or {})
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),scope)
    return scope

def base_docs():
    health=[{'name':name,'critical':True,'status':'FRESH','as_of':TS,'max_age_h':hours} for name,hours in CRITICAL_SLAS.items()]
    auth={'engine':'justhodl-khalid-risk','schema_version':'1.0.0','status':'OK','generated_at':TS,
          'exposure_cap_pct':30,'policy':{'exposure_cap_pct':30,'allows_new_entries':True,'mode':'SELECTIVE'},
          'source_health':health,'critical_failures':[],'hard_vetoes':[],'expires_at':authority_expiry(TS,health)}
    book={'schema_version':'1.0','status':'READY','allows_new_entries':True,'book_id':'test-book','account_id':'test-account','currency':'USD',
          'as_of':TS,'reconciled_at':TS,'equity_nav':100000,'cash':100000,'liabilities':0,'reserved_order_exposure':0,
          'gross_exposure':0,'net_exposure':0,'positions':[],'unpriced_positions':[],'open_orders':[],
          'nav_history':[{'book_id':'test-book','account_id':'test-account','as_of':(NOW-timedelta(days=1)).isoformat(),'equity_nav':100000},
                         {'book_id':'test-book','account_id':'test-account','as_of':TS,'equity_nav':100000}]}
    return {'data/khalid-risk.json':auth,'portfolio/snapshot.json':{'generated_at':TS,'capital_book':book},
            'data/risk-gate.json':{'generated_at':TS,'sizing_multiplier':1},
            'data/liquidity-profile.json':{'generated_at':TS,'all_tickers':{'AAPL':{'adv_usd':1e8,'n_bars':20,'observed_at':TS}}},
            'data/liquidity-capacity.json':{'generated_at':TS,'firm':{'n_unknown_volume':0},'least_liquid_names':[]},
            'data/factor-risk.json':{'generated_at':TS,'firm':{'var_99_1d_pct':1},'coverage':{'direct':1}},
            'data/engine-trust.json':{'generated_at':TS,'current_regime':'BALANCED','engines':[{'signal_type':'eng:test-engine','effective_trust':.8,
                'regime_n':50,'regime_wilson_lb':.6,'net_alpha_t_stat':3,'net_alpha_excess_pct':1,'alpha_status':'ALPHA_PROVEN'}]}}

def size(docs):
    recs=[{'ticker':'AAPL','engine':'test-engine','final_w_pct':4}]
    result=constrain_sizes(recs,docs,now=NOW)
    return recs[0],result

def test_sizing_positive_control_and_required_donor_ablations():
    docs=base_docs();rec,summary=size(docs)
    assert rec['final_w_pct']==2.56 and summary['authority_usable'] and summary['constraints_ready'],(rec,summary)
    assert rec['execution_eligible'] is False
    for key in ['data/khalid-risk.json','portfolio/snapshot.json','data/risk-gate.json','data/liquidity-profile.json','data/liquidity-capacity.json','data/factor-risk.json','data/engine-trust.json']:
        bad=copy.deepcopy(docs);bad.pop(key)
        rec,_=size(bad);assert rec['final_w_pct']==0,key
    for field,value in [('regime_n',2),('regime_wilson_lb',.4),('net_alpha_t_stat',1),('net_alpha_excess_pct',-1),('alpha_status','ALPHA_NEGATIVE')]:
        bad=copy.deepcopy(docs);bad['data/engine-trust.json']['engines'][0][field]=value
        assert size(bad)[0]['final_w_pct']==0,field

def test_sizing_zero_cap_zero_gate_orders_and_old_volume_are_binding():
    docs=base_docs();docs['data/risk-gate.json']['sizing_multiplier']=0
    assert size(docs)[0]['final_w_pct']==0
    docs=base_docs();a=docs['data/khalid-risk.json'];a['exposure_cap_pct']=0;a['policy']['exposure_cap_pct']=0
    assert size(docs)[0]['final_w_pct']==0
    docs=base_docs();b=docs['portfolio/snapshot.json']['capital_book'];b['open_orders']=[{'symbol':'AAPL','remaining_exposure':4900}];b['reserved_order_exposure']=4900
    assert size(docs)[0]['final_w_pct']<=.1  # An order-only name still consumes the per-name cap.
    docs=base_docs();docs['data/liquidity-profile.json']['all_tickers']['AAPL']['observed_at']=(NOW-timedelta(days=6)).isoformat()
    assert size(docs)[0]['final_w_pct']==0
    docs=base_docs();docs['data/liquidity-capacity.json']['firm']['n_unknown_volume']=1
    assert size(docs)[0]['final_w_pct']==0
    docs=base_docs();docs['data/khalid-risk.json']['expires_at']=(NOW-timedelta(minutes=1)).isoformat()
    assert size(docs)[0]['final_w_pct']==0

def test_donor_reader_rejects_stale_future_missing_and_retains_zero_and_invalid_evidence():
    class S3:
        def __init__(self,docs):self.docs=docs
        def get_object(self,**kw):return {'Body':io.BytesIO(json.dumps(self.docs[kw['Key']]).encode())}
    docs={'fresh':{'generated_at':TS,'value':0,'row':{'bad':float('nan')}},'stale':{'generated_at':(NOW-timedelta(days=3)).isoformat(),'value':5},
          'future':{'generated_at':(NOW+timedelta(days=1)).isoformat(),'value':5}}
    got,receipts=load_inputs(S3(docs),'b',[(k,24,('value',)) for k in ['fresh','stale','future','missing']],now=NOW)
    assert got['fresh']['value']==0 and receipts['fresh']['usable']
    assert got['fresh']['row']['bad']=={'invalid_number':'nan'}
    for k in ['stale','future','missing']:assert not got[k] and not receipts[k]['usable']
    json.dumps({'docs':got,'receipts':receipts},allow_nan=False)

def test_conviction_exact_trust_match_and_empirical_redundancy_reduce_votes():
    rows=[{'engine':'one','skill':1.4,'signal':80},{'engine':'two','skill':1.2,'signal':60}]
    trust={'generated_at':TS,'engines':[{'signal_type':'eng:one','effective_trust':.8},{'signal_type':'eng:two','effective_trust':.4}]}
    orth={'as_of':TS,'snapshots_total':50,'clusters_high_redundancy':[['one','two']],'effective_information_rank':1}
    result=conviction_members(copy.deepcopy(rows),trust,orth)
    assert [r['skill'] for r in result]==[.8,.2]
    no_orth=conviction_members(copy.deepcopy(rows),trust,{})
    assert [r['skill'] for r in no_orth]==[.8,.4]
    assert conviction_members([{'engine':'someone','skill':2,'signal':20}],trust,{})[0]['skill']==.5
    book={'generated_at':TS,'equity_book':[{'symbol':'AAPL','net_pct':4,'gross_pct':6,'desks':['a','b']}]}
    names=annotate_book([{'ticker':'AAPL'},{'ticker':'NEW'}],book)
    assert names[0]['firm_exposure']['already_owned'] and not names[1]['firm_exposure']['already_owned']
    assert names[0]['firm_exposure']['existing']['gross_pct']==6

def test_stock_context_joins_dated_credit_revision_quality_without_overwriting_growth():
    row={'ticker':'AAPL','forward_growth_pct':8}
    docs={'data/credit-before-equity.json':{'generated_at':TS,'names':[{'ticker':'AAPL','synthetic_cds_bp':0,'hist_n':8}]},
          'data/estimate-revisions.json':{'generated_at':TS,'upward_revisions':[{'ticker':'AAPL','eps_revision_pct':2,'fiscal_period':'FY2027','baseline_date':'2026-09-01'}]},
          'data/earnings-quality.json':{'as_of':TS,'all_ranked':[{'ticker':'AAPL','cash_conversion':1.2,'quality_score':80}]}}
    stock_context([row],docs);ctx=row['cross_engine_context']
    assert ctx['credit']['record']['synthetic_cds_bp']==0 and row['forward_growth_pct']==8
    assert ctx['estimate_revisions']['record']['baseline_date']=='2026-09-01'
    assert ctx['earnings_quality']['record']['quality_score']==80
    stock_context([row],{});assert row['cross_engine_context']['credit']['record'] is None

def flow_doc():
    windows={f'{n}d':{'available':True,'prior_observation_date':'2026-09-01','comparison_end_run_date':'2026-09-09'} for n in [1,5,20]}
    return {'generated_at':TS,'by_etf':{'XLK':{'ticker':'XLK','nav_source':'FMP_ETF_INFO','flow_windows':windows,'net_flow_1d_usd':0,'net_flow_5d_usd':-100,'net_flow_20d_usd':300,'tna':1000}}}

def test_true_flow_changes_actual_daily_five_day_fields_and_sector_vote_once():
    doc=flow_doc();row={'ticker':'XLK','daily_flow_usd':999,'flow_5d_usd':999,'flow_21d_usd':777}
    assert true_flow_rows([row],doc)==2
    assert row['daily_flow_usd']==0 and row['flow_5d_usd']==-100 and row['flow_21d_usd']==777 and row['true_flow_20d_usd']==300
    sector={'symbol':'XLK','rotation_score':65,'rotation_score_preflow':60}
    sector_flow_context([sector],doc);assert sector['rotation_score']==50 and sector['true_flow_contribution']==-10
    bad=flow_doc();bad['by_etf']['XLK']['flow_windows']['5d']['available']=False
    row={'ticker':'XLK','flow_5d_usd':999};true_flow_rows([row],bad);assert row['flow_5d_usd']==999
    sector={'symbol':'XLK','rotation_score':65,'rotation_score_preflow':60};sector_flow_context([sector],bad);assert sector['rotation_score']==65
    bad=flow_doc();bad['by_etf']['XLK']['nav_source']='PRICE_FALLBACK_DEGRADED'
    row={'ticker':'XLK','daily_flow_usd':999};assert true_flow_rows([row],bad)==0 and row['daily_flow_usd']==999

def test_true_flow_producer_dividend_zero_and_five_observation_baseline():
    scope=producer_functions('etf-true-flows',['calculate_nav_flows'],{'ANOMALY_BP':50})
    compute=scope['calculate_nav_flows'];row={'ticker':'XLK','shares_outstanding':100,'nav':9,'price':9,'tna':900}
    history=[{'date':f'2026-09-0{i+1}','shares':{'XLK':80 if i==0 else 100},'nav':{'XLK':10}} for i in range(5)]
    got,anomalies=compute({'XLK':row},history,{'XLK':100},{'XLK':10},{'XLK':1000},{'XLK':1},'2026-09-09','2026-09-05')
    r=got[0];assert r['net_flow_1d_usd']==0 and r['net_flow_1d_tna_method_usd']==0 and not anomalies,r
    assert r['net_flow_5d_usd']==180 and r['shares_chg_5d_pct']==25 and r['net_flow_20d_usd'] is None
    got,_=compute({'XLK':row},history[-1:],{'XLK':90},{'XLK':10},{},{},'2026-09-09','2026-09-05')
    assert got[0]['net_flow_1d_usd']==90 and got[0]['net_flow_5d_usd'] is None and not got[0]['flow_windows']['5d']['available']

def test_complete_holdings_producer_and_actual_flow_attribution_include_row_101():
    holdings=[{'asset':f'S{i}','weightPercentage':1,'marketValue':100,'updatedAt':'2026-09-08'} for i in range(101)]
    scope=producer_functions('etf-constituents',['fetch_constituents','compute_per_stock_etf_exposure'],
       {'fmp_holdings':lambda *a,**k:holdings,'FMP_KEY':'offline','FETCH_TIMEOUT':1,'pctf':lambda x:float(x) if x is not None else None,'datetime':datetime,'timezone':timezone})
    result=scope['fetch_constituents']('XLK');assert len(result['top_constituents'])==101,result
    etfs=[{'ticker':'XLK','daily_flow_usd':100,'flow_5d_usd':500,'flow_21d_usd':2100}]
    true_flow_rows(etfs,flow_doc())
    mapped=scope['compute_per_stock_etf_exposure'](etfs,{'XLK':result})
    assert len(mapped)==101 and mapped['S100']['total_aggregate_flow_daily_usd']==0 and mapped['S100']['total_aggregate_flow_5d_usd']==-1

def test_liquidity_high_low_changes_volatility_without_inventing_spread_score():
    bars=[{'c':100,'v':10000,'h':101,'l':99,'t':NOW.timestamp()*1000} for _ in range(20)]
    scope=producer_functions('liquidity-profile',['analyze_one'],{'fetch_last_20d':lambda _:bars,'datetime':datetime,'timezone':timezone})
    first=scope['analyze_one']('AAPL');bars[-1].update(h=120,l=80);wide=scope['analyze_one']('AAPL')
    assert wide['atr_percent']>first['atr_percent'] and wide['liquidity_score']==first['liquidity_score']
    assert wide['spread_bps'] is None and wide['spread_proxy_bps'] is None and wide['execution_eligible'] is False
    bars[:]=[{'c':100,'v':10000,'h':100,'l':100,'t':NOW.timestamp()*1000} for _ in range(20)]
    assert scope['analyze_one']('AAPL')['atr_pct']==0

def test_funding_one_four_eight_hour_normalization_unknown_and_real_zero():
    scope=producer_functions('crypto-funding',['funding_interval','fetch_coin_data','_mean','_stdev'],
           {'math':math,'FUNDING_HIGHLY_BULL':.0003,'FUNDING_BULL':.0001,'FUNDING_BEAR':-.0001,'FUNDING_HIGHLY_BEAR':-.0003})
    for hours in [1,4,8]:
        current={'fundingRate':'0.0001','fundingTime':str(int(NOW.timestamp()*1000)),'nextFundingTime':str(int((NOW+timedelta(hours=hours)).timestamp()*1000)),'ts':str(int(NOW.timestamp()*1000))}
        def fetch(path):return {'code':'0','data':[current]} if 'funding-rate?' in path else {'code':'0','data':[]}
        scope['fetch_json']=fetch;r=scope['fetch_coin_data']('BTC')
        assert r['funding_interval_hours']==hours and r['interval_source']=='venue_schedule'
        assert r['annualized_pct']==round(.0001*24/hours*365*100,2)
        assert r['next_funding_time']==current['nextFundingTime'] and r['current_funding_time']==current['fundingTime']
    current.clear();current['fundingRate']='0';r=scope['fetch_coin_data']('BTC');assert r['annualized_pct'] is None and r['regime']=='UNKNOWN_INTERVAL'
    current.update(fundingTime='10000000',nextFundingTime='24400000');r=scope['fetch_coin_data']('BTC');assert r['annualized_pct']==0
    current['fundingRate']=None;assert scope['fetch_coin_data']('BTC') is None
    assert scope['funding_interval']({},[{'ts':'garbage'},{'ts':float('nan')}])==(None,'unknown')
    assert scope['funding_interval']({},[{'ts':10000000},{'ts':24400000}])==(4.0,'observed_settlement_interval')

def test_firm_board_requires_aligned_current_book_and_full_volume_coverage():
    factor={'generated_at':TS,'firm_book_asof':TS,'firm':{'var_99_1d_pct':1},'coverage':{'direct':2},'risk_contributors':[{'ticker':'AAPL','marginal_var':.2}]}
    capacity={'generated_at':TS,'firm':{'n_unknown_volume':0},'trapped_names':[{'symbol':'AAPL'}]};book={'generated_at':TS,'firm':{'gross_pct':10}}
    out=firm_board_contract({'firm_posture':'GREEN'},factor,capacity,book,now=NOW)
    assert out['data_contract_status']=='READY' and out['factor_model_detail']['risk_contributors']==factor['risk_contributors'] and out['allows_new_entries'] is False
    for missing in ['factor','capacity','book']:
        args=[copy.deepcopy(factor),copy.deepcopy(capacity),copy.deepcopy(book)];args[['factor','capacity','book'].index(missing)]={}
        assert firm_board_contract({'firm_posture':'GREEN'},*args,now=NOW)['firm_posture']=='DATA_HOLD'
    factor['firm_book_asof']=(NOW-timedelta(minutes=1)).isoformat();out=firm_board_contract({'firm_posture':'GREEN'},factor,capacity,book,now=NOW)
    assert out['firm_posture']=='DATA_HOLD' and out['confidence']=='UNVERIFIED'
