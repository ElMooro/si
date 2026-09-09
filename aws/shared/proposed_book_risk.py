"""Covariance and proposed-book risk with complete reconciled-book coverage.
Private position vectors stay internal; reports expose aggregate risk and model provenance.
"""
import math
from datetime import datetime, timezone
from donor_contract import inspect_donor, numeric

MODEL_SCHEMA='factor-risk-model.v1'
VAR99_LIMIT_PCT=5.0
SCENARIO_LOSS_LIMIT_PCT=15.0
Z99=2.326


def export_model(factors,covariance,dates,loadings,name_load,scenarios,generated_at):
    """Publish already estimated parameters, never owner inventory."""
    rows={}
    for symbol,row in loadings.items():
        rows[symbol]={'betas':row.get('betas'),'residual_variance_daily':row.get('resid_var'),
                      'n_observations':row.get('n_obs'),'calibrated_at':row.get('asof'),
                      'observed_from':row.get('observed_from'),'observed_through':row.get('observed_through'),
                      'loading_source':'direct'}
    for symbol,row in name_load.items():
        if symbol not in rows:
            rows[symbol]={'betas':row.get('betas'),'residual_variance_daily':row.get('resid_var'),
                          'loading_source':'proxy','proxy_scope':row.get('sector'),'n_observations':None,
                          'calibrated_at':None,'observed_through':None}
    return {'schema_version':MODEL_SCHEMA,'generated_at':generated_at,
            'covariance':{'factor_order':list(factors),'matrix_daily':covariance,
                          'units':'return_fraction_squared_per_session','n_observations':len(dates),
                          'observed_from':dates[0] if dates else None,'observed_through':dates[-1] if dates else None},
            'loadings':rows,'loading_units':{'betas':'dimensionless','residual_variance_daily':'return_fraction_squared_per_session'},
            'scenarios':[{'name':row.get('scenario',row.get('name')),'shock':row.get('shock'),
                          'units':'return_fraction','empirical':row.get('empirical') is True} for row in scenarios],
            'scope':'estimated parameters; no broker positions; proxy/missing/stale loadings cannot authorize sizing',
            'assumptions':['linear factor exposures','normal one-session VaR','independent security residuals','scenario analogues are not probabilities']}


def _psd(matrix):
    """Cholesky test allowing semidefinite pivots."""
    n=len(matrix);lower=[[0.0]*n for _ in range(n)];tol=1e-12
    for i in range(n):
        for j in range(i+1):
            if abs(matrix[i][j]-matrix[j][i])>tol:return False
            value=matrix[i][j]-sum(lower[i][k]*lower[j][k] for k in range(j))
            if i==j:
                if value < -tol:return False
                lower[i][j]=math.sqrt(max(0,value))
            elif lower[j][j]>tol:lower[i][j]=value/lower[j][j]
            elif abs(value)>tol:return False
    return True


def prepare_model(factor_doc,book_view,now=None):
    """Rebuild risk for this exact reconciled snapshot and its directed open orders."""
    now=now or datetime.now(timezone.utc)
    model=factor_doc.get('proposed_book_model') if isinstance(factor_doc,dict) else None
    model=model if isinstance(model,dict) else {}
    receipt=inspect_donor(model,'data/factor-risk.json#proposed_book_model',48,
        observed_paths=('covariance.observed_through',),required_paths=('covariance','loadings','scenarios'),
        now=now,max_observation_age_hours=96)
    state={'status':'BLOCKED','errors':[],'model_receipt':{k:v for k,v in receipt.items() if k!='fields'},
           'book_as_of':(book_view.get('contract') or {}).get('as_of'),'weights':{},'loadings':{},
           'limits':{'var_99_1d_pct':VAR99_LIMIT_PCT,'worst_scenario_loss_pct':SCENARIO_LOSS_LIMIT_PCT},
           'limit_scope':'conservative research risk limits; capital permission is separately governed'}
    errors=state['errors']
    if not receipt['usable'] or model.get('schema_version')!=MODEL_SCHEMA:errors.append('model contract unavailable, stale or unsupported')
    if model.get('loading_units')!={'betas':'dimensionless','residual_variance_daily':'return_fraction_squared_per_session'}:errors.append('loading units invalid')
    if book_view.get('status')!='READY':errors.append('complete reconciled capital book unavailable')
    cov=model.get('covariance');cov=cov if isinstance(cov,dict) else {}
    factors=cov.get('factor_order');factors=factors if isinstance(factors,list) else []
    matrix=cov.get('matrix_daily');n=len(factors)
    valid_matrix=0<n<=12 and all(isinstance(f,str) for f in factors) and len(set(factors))==n and isinstance(matrix,list) and len(matrix)==n
    valid_matrix=valid_matrix and all(isinstance(row,list) and len(row)==n and all(numeric(x) is not None for x in row) for row in matrix)
    if valid_matrix:matrix=[[float(x) for x in row] for row in matrix];valid_matrix=_psd(matrix)
    if not valid_matrix or cov.get('units')!='return_fraction_squared_per_session' or (numeric(cov.get('n_observations')) or 0)<60:
        errors.append('covariance dimensions, units, samples or positive semidefiniteness invalid')
    if errors:return state
    state.update(factors=factors,covariance=matrix,weights=dict(book_view.get('signed_weights') or {}))
    scenarios=[]
    for row in model.get('scenarios',[]) if isinstance(model.get('scenarios'),list) else []:
        shock=row.get('shock') if isinstance(row,dict) else None
        if not isinstance(shock,dict) or row.get('units')!='return_fraction' or any(numeric(shock.get(f)) is None for f in factors):
            errors.append('scenario shock units or dimensions invalid');continue
        scenarios.append({'name':row.get('name'),'shock':[float(shock[f]) for f in factors],'empirical':row.get('empirical') is True})
    if not scenarios:errors.append('scenario coverage unavailable')
    raw_loadings=model.get('loadings') or {}
    for symbol,row in raw_loadings.items() if isinstance(raw_loadings,dict) else []:
        if not isinstance(row,dict) or row.get('loading_source')!='direct':continue
        dated=inspect_donor({'generated_at':row.get('calibrated_at'),'observed_at':row.get('observed_through')},
                            'factor-loading',240,observed_paths=('observed_at',),now=now,max_observation_age_hours=240)
        beta=row.get('betas');residual=numeric(row.get('residual_variance_daily'))
        if not dated['usable'] or not isinstance(beta,dict) or any(numeric(beta.get(f)) is None for f in factors) or residual is None or residual<0 or (numeric(row.get('n_observations')) or 0)<60:continue
        state['loadings'][symbol.upper()]={'betas':[float(beta[f]) for f in factors],'residual':residual,
                                         'provenance':{k:row.get(k) for k in ('loading_source','n_observations','calibrated_at','observed_through')}}
    for order in (book_view.get('contract') or {}).get('open_orders',[]):
        symbol=str(order.get('symbol','')).upper();amount=numeric(order.get('remaining_exposure'));nav=book_view.get('equity_nav')
        side=str(order.get('side') or order.get('direction') or '').upper()
        direction=1 if side in ('BUY','LONG','BUY_TO_COVER') else -1 if side in ('SELL','SHORT','SELL_SHORT') else None
        if amount and (direction is None or not nav):errors.append('open order direction is not reconciled');continue
        if amount and direction:state['weights'][symbol]=state['weights'].get(symbol,0)+direction*amount/nav
    if any(abs(w)>0 and symbol not in state['loadings'] for symbol,w in state['weights'].items()):
        errors.append('complete direct loading coverage of the reconciled book is unavailable')
    state['scenarios']=scenarios
    if not errors:state['status']='READY'
    return state


def _risk(state,weights):
    factors=[0.0]*len(state['factors']);idio=0.0
    for symbol,weight in weights.items():
        if not weight:continue
        row=state['loadings'][symbol]
        factors=[value+weight*beta for value,beta in zip(factors,row['betas'])]
        idio+=weight*weight*row['residual']
    systematic=sum(factors[i]*state['covariance'][i][j]*factors[j] for i in range(len(factors)) for j in range(len(factors)))
    variance=max(0,systematic+idio)
    scenarios=[{'name':row['name'],'pnl_pct':100*sum(a*b for a,b in zip(factors,row['shock'])),'empirical':row['empirical']} for row in state['scenarios']]
    return {'var_99_1d_pct':Z99*math.sqrt(variance)*100,'worst_scenario_loss_pct':max([0]+[-row['pnl_pct'] for row in scenarios]),
            'scenarios':scenarios,'variance':variance,'exposures':factors}


def constrain_candidate(state,symbol,direction,upper_pct):
    """Solve quadratic VaR and all linear scenario constraints, then update the book."""
    report={'status':state['status'],'errors':list(state['errors']),'book_as_of':state['book_as_of'],
            'model_receipt':state['model_receipt'],'limits':state['limits'],'limit_scope':state['limit_scope'],
            'model_scope':'same reconciled snapshot plus directed reserved orders and accepted candidates','execution_eligible':False}
    row=state['loadings'].get(symbol)
    sign=1 if direction=='LONG' else -1 if direction=='SHORT' else None
    if state['status']!='READY' or row is None or sign is None:
        report['status']='BLOCKED';report['errors'].append('candidate direct dated loadings or trade direction unavailable')
        return 0.0,report
    before=_risk(state,state['weights']);beta=row['betas'];cov=state['covariance'];n=len(beta)
    a=sum(beta[i]*cov[i][j]*beta[j] for i in range(n) for j in range(n))+row['residual']
    b=2*sign*(sum(beta[i]*cov[i][j]*before['exposures'][j] for i in range(n) for j in range(n))+state['weights'].get(symbol,0)*row['residual'])
    c=before['variance']-(VAR99_LIMIT_PCT/100/Z99)**2
    lower,upper=0.0,max(0,float(upper_pct)/100)
    if a>1e-16:
        discriminant=b*b-4*a*c
        if discriminant<0:upper=-1
        else:
            lower=max(lower,(-b-math.sqrt(discriminant))/(2*a));upper=min(upper,(-b+math.sqrt(discriminant))/(2*a))
    elif abs(b)>1e-16:
        if b>0:upper=min(upper,-c/b)
        else:lower=max(lower,-c/b)
    elif c>0:upper=-1
    for scenario in state['scenarios']:
        base=sum(x*y for x,y in zip(before['exposures'],scenario['shock']))
        slope=sign*sum(x*y for x,y in zip(beta,scenario['shock']))
        bound=-SCENARIO_LOSS_LIMIT_PCT/100-base
        if slope>1e-16:lower=max(lower,bound/slope)
        elif slope < -1e-16:upper=min(upper,bound/slope)
        elif bound>0:upper=-1
    amount=max(0,math.floor(upper*10000+1e-9)/100) if upper>=lower else 0.0
    weights=dict(state['weights']);weights[symbol]=weights.get(symbol,0)+sign*amount/100
    after=_risk(state,weights)
    if after['var_99_1d_pct']>VAR99_LIMIT_PCT+1e-8 or after['worst_scenario_loss_pct']>SCENARIO_LOSS_LIMIT_PCT+1e-8:
        amount=0.0;after=before
    state['weights']=weights if amount else state['weights']
    clean=lambda risk:{k:v for k,v in risk.items() if k not in ('variance','exposures')}
    report.update(status='READY' if amount else 'RISK_LIMIT_HOLD',before=clean(before),after=clean(after),
                  marginal_var_99_1d_pct=after['var_99_1d_pct']-before['var_99_1d_pct'],
                  candidate_loading=row['provenance'],risk_constrained_w_pct=amount)
    return amount,report
