"""Same-book candidate risk, model covariance guards, producer completeness and privacy."""
import ast
import copy
import json
import math
import runpy
import sys
import time
from datetime import datetime,timedelta,timezone
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'aws/shared'))
from proposed_book_risk import prepare_model,constrain_candidate,export_model
from capital_contract import capital_book_view
_fixture=runpy.run_path(str(Path(__file__).with_name('test_equity_donor_contracts.py')))
base_docs=_fixture['base_docs'];NOW=_fixture['NOW'];TS=_fixture['TS'];producer_functions=_fixture['producer_functions']


def state_from(docs):
    return prepare_model(docs['data/factor-risk.json'],capital_book_view(docs['portfolio/snapshot.json'],now=NOW),now=NOW)


def add_position(docs,symbol,amount):
    book=docs['portfolio/snapshot.json']['capital_book']
    book['positions'].append({'symbol':symbol,'market_value':amount,'valuation_status':'PRICED','mark_age_h':1})
    book['gross_exposure']+=abs(amount);book['net_exposure']+=amount;book['cash']-=amount
    model=docs['data/factor-risk.json']['proposed_book_model']
    model['loadings'][symbol]=copy.deepcopy(model['loadings']['AAPL'])


def test_marginal_var_changes_size_and_excludes_unknown_proxy_or_stale_candidate():
    docs=base_docs();state=state_from(docs);amount,report=constrain_candidate(state,'AAPL','LONG',4)
    assert amount==4 and report['after']['var_99_1d_pct']>report['before']['var_99_1d_pct']
    docs['data/factor-risk.json']['proposed_book_model']['covariance']['matrix_daily']=[[1.0]]
    amount,report=constrain_candidate(state_from(docs),'AAPL','LONG',4)
    assert 2.13<=amount<=2.15 and report['after']['var_99_1d_pct']<=5,report
    for key,value in [('loading_source','proxy'),('observed_through','2020-01-01'),('n_observations',5)]:
        bad=base_docs();bad['data/factor-risk.json']['proposed_book_model']['loadings']['AAPL'][key]=value
        assert constrain_candidate(state_from(bad),'AAPL','LONG',4)[0]==0,key
    assert constrain_candidate(state_from(base_docs()),'UNMODELED','LONG',4)[0]==0


def test_scenario_budget_uses_actual_same_book_and_accepts_valid_risk_reducing_hedge():
    docs=base_docs();add_position(docs,'OTHER',28000)
    amount,report=constrain_candidate(state_from(docs),'AAPL','LONG',5)
    assert amount==2 and abs(report['before']['worst_scenario_loss_pct']-14)<1e-8
    assert report['after']['worst_scenario_loss_pct']<=15+1e-8
    docs=base_docs();add_position(docs,'OTHER',40000)
    amount,report=constrain_candidate(state_from(docs),'AAPL','SHORT',12)
    assert amount==12 and report['marginal_var_99_1d_pct']<0 and report['after']['worst_scenario_loss_pct']<=15
    assert constrain_candidate(state_from(docs),'AAPL','SHORT',9)[0]==0  # Too small to bring breached risk back under limits.


def test_invalid_covariance_and_incomplete_same_book_orders_fail_closed():
    for matrix in [[[float('nan')]], [[-1]], [[1,2]], [[1,2],[2,1]]]:
        docs=base_docs();m=docs['data/factor-risk.json']['proposed_book_model'];m['covariance']['matrix_daily']=matrix
        assert state_from(docs)['status']=='BLOCKED'
    docs=base_docs();add_position(docs,'PRIVATE_UNMODELED',10000)
    del docs['data/factor-risk.json']['proposed_book_model']['loadings']['PRIVATE_UNMODELED']
    state=state_from(docs);assert state['status']=='BLOCKED'
    _,report=constrain_candidate(state,'AAPL','LONG',4)
    assert 'PRIVATE_UNMODELED' not in json.dumps(report) and 'test-account' not in json.dumps(report)
    docs=base_docs();b=docs['portfolio/snapshot.json']['capital_book'];b['open_orders']=[{'symbol':'AAPL','remaining_exposure':1000}];b['reserved_order_exposure']=1000
    assert state_from(docs)['status']=='BLOCKED'
    b['open_orders'][0]['side']='BUY';state=state_from(docs);assert state['status']=='READY' and state['weights']['AAPL']==.01
    docs['portfolio/snapshot.json']['capital_book']['reconciled_at']='2020-01-01T00:00:00Z'
    assert state_from(docs)['status']=='BLOCKED'


def test_repeated_name_sizes_share_liquidity_and_portfolio_risk_budget():
    from equity_donor_inputs import constrain_sizes
    docs=base_docs();docs['data/liquidity-profile.json']['all_tickers']['AAPL']['adv_usd']=100000
    recs=[{'ticker':'AAPL','direction':'LONG','engine':'test-engine','final_w_pct':4} for _ in range(2)]
    constrain_sizes(recs,docs,now=NOW)
    assert sum(row['final_w_pct'] for row in recs)<=1 and recs[1]['final_w_pct']==0,recs
    docs=base_docs();add_position(docs,'OTHER',10000)
    # Direct risk loadings exist, but unknown exit volume in the actual book blocks new size.
    assert _fixture['size'](docs)[0]['final_w_pct']==0


def test_actual_factor_handler_publishes_full_model_and_all_contributors():
    factors=['MKT','SIZE','VALUE','MOM','QUALITY','LOWVOL'];etfs=['SPY','IWM','IWD','IWF','MTUM','QUAL','USMV']
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    names=[f'S{i}' for i in range(25)]
    dates=[(NOW-timedelta(days=i)).date().isoformat() for i in range(80,0,-1)]
    cache={'loadings':{symbol:{'betas':{f:1.0 if f=='MKT' else 0.0 for f in factors},'resid_var':.0001,
           'n_obs':70,'asof':TS,'observed_from':dates[0],'observed_through':dates[-1]} for symbol in names}}
    scope=producer_functions('factor-risk',['lambda_handler','returns_from_closes','cov_matrix','matvec','quantile'],
        {'datetime':Clock,'timezone':timezone,'time':time,'math':math,'json':json,'MIN_OBS':60,'SCHEMA':'1.0','CACHE_KEY':'cache','OUT_KEY':'out',
         'ALL_ETFS':etfs,'FACTOR_NAMES':factors,'TRADING_DAYS':252,'Z95':1.645,'Z99':2.326,'ES95_MULT':2.063,'CACHE_STALE_DAYS':10,'FETCH_BUDGET_S':720,
         'SCENARIOS':[{'name':'Stress','shock':{f:-.1 for f in factors}}],'FACTOR_HEDGE_ETF':dict(zip(factors,etfs)),'export_model':export_model})
    writes={}
    scope.update(load_equity_book=lambda:([{'symbol':symbol,'weight':.01,'sector':'Tech','name':symbol} for symbol in names],TS),
                 poly_daily_closes=lambda *a,**k:{date:100+i+math.sin(i) for i,date in enumerate(dates)},
                 get_json=lambda key:cache,put_json=lambda key,doc:writes.__setitem__(key,copy.deepcopy(doc)))
    result=scope['lambda_handler']({},None);out=writes['out'];model=out['proposed_book_model']
    assert result['statusCode']==200 and len(out['risk_contributors'])==25 and len(out['top_risk_contributors'])==15
    assert len(model['loadings'])==25 and len(model['covariance']['matrix_daily'])==6
    assert all('weight' not in row and row['loading_source']=='direct' and row['observed_through']==dates[-1] for row in model['loadings'].values())
    assert model['covariance']['observed_through']==dates[-1]
    json.dumps(out,allow_nan=False)


def test_actual_public_sizing_handler_uses_no_unscoped_portfolio_scan_or_private_rows():
    from equity_donor_inputs import constrain_sizes
    from public_brain_projection import sanitize_public
    docs=base_docs();add_position(docs,'PRIVATE_POS',10000)
    calls=[];writes={}
    class Table:
        def scan(self,**kwargs):return {'Items':[]}
    class DDB:
        def Table(self,name):calls.append(name);return Table()
    class S3:
        def put_object(self,**kw):writes[kw['Key']]=json.loads(kw['Body'])
    class Attr:
        def __init__(self,*a):pass
        def eq(self,*a):return self
        def gte(self,*a):return self
        def is_in(self,*a):return self
        def __and__(self,other):return self
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    scope=producer_functions('sizing-engine',['lambda_handler','rets'],{'datetime':Clock,'timezone':timezone,'time':time,'json':json,
        'DDB':DDB(),'S3':S3(),'BUCKET':'b','OUT_KEY':'data/sizing.json','SIZING_SPECS':[],'load_inputs':lambda *a:(docs,{}),
        's3json':lambda key:{},'Attr':Attr,'capital_book_view':lambda doc:capital_book_view(doc,now=NOW),
        'poly_closes':lambda ticker:[100,101],'constrain_sizes':lambda rows,donors:constrain_sizes(rows,donors,now=NOW),
        'sanitize_public':sanitize_public,'VERSION':'test','VOL_TARGET':.3,'CAP_W':5,'FLOOR_W':.25,'STARTER_W':.5})
    scope['lambda_handler']({},None);out=writes['data/sizing.json'];encoded=json.dumps(out)
    assert calls==['justhodl-signals'] and out['holdings'] is None
    assert out['holdings_publication']=='REDACTED_ACCOUNT_PRIVATE'
    assert 'PRIVATE_POS' not in encoded and 'test-account' not in encoded and 'test-book' not in encoded
