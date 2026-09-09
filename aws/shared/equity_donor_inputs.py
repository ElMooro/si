"""D15-D34: dated donor contracts, explicit context and bounded research constraints.
No execution quotes, borrow availability, marginal-risk model or broker book is invented.
"""
import json,math
from datetime import datetime,timezone
from donor_contract import inspect_donor
from capital_contract import authority_view, capital_book_view

def finite(value):
    if isinstance(value,bool):return None
    try:n=float(value);return n if math.isfinite(n) else None
    except (TypeError,ValueError,OverflowError):return None

def safe_evidence(value):
    """Preserve invalid numeric evidence without emitting non-standard NaN/Infinity JSON."""
    if isinstance(value,float) and not math.isfinite(value):return {'invalid_number':str(value)}
    if isinstance(value,dict):return {str(k):safe_evidence(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [safe_evidence(v) for v in value]
    return value

def flow_window_valid(row,window):
    windows=row.get('flow_windows')
    meta=windows.get(window) if isinstance(windows,dict) else None
    if not isinstance(meta,dict):return False
    try:
        start=datetime.fromisoformat(str(meta.get('prior_observation_date'))).date()
        end=datetime.fromisoformat(str(meta.get('comparison_end_run_date'))).date()
        return meta.get('available') is True and start<end
    except (TypeError,ValueError):return False

def load_inputs(s3,bucket,specs,now=None):
    docs,receipts={},{}
    for key,max_age,required in specs:
        try:doc=json.loads(s3.get_object(Bucket=bucket,Key=key)['Body'].read())
        except Exception:doc=None
        receipt=inspect_donor(doc,key,max_age_hours=max_age,required_paths=required,now=now)
        shapes={'firm':dict,'policy':dict,'capital_book':dict,'all_tickers':(dict,list),'engines':list,'by_etf':dict,'names':list,'all_ranked':list,'coverage':dict}
        malformed=[field for field in required if field in shapes and isinstance(doc,dict) and not isinstance(doc.get(field),shapes[field])]
        if malformed:
            receipt['usable']=False;receipt['status']='INVALID';receipt.setdefault('errors',[]).append('invalid field types: '+', '.join(malformed))
        if isinstance(doc,dict) and (doc.get('ok') is False or doc.get('error')):
            receipt['usable']=False;receipt['status']='INVALID';receipt.setdefault('errors',[]).append('producer reported error')
        receipts[key]=safe_evidence({k:v for k,v in receipt.items() if k!='fields'})
        receipts[key]['required_paths']=list(required)
        docs[key]=safe_evidence(doc) if receipt['usable'] else {}
    return docs,receipts

def rows_by(doc,field,identity=('ticker','symbol','stock','signal_type','name')):
    rows=doc.get(field) if isinstance(doc,dict) else None
    if isinstance(rows,dict):return {str(k).upper():v for k,v in rows.items() if isinstance(v,dict)}
    return {str(next((r[k] for k in identity if r.get(k)), '')).upper():r for r in (rows or []) if isinstance(r,dict)} if isinstance(rows,list) else {}

def stamp(doc):return doc.get('generated_at') or doc.get('as_of') or doc.get('asof')
def canonical(name):return str(name or '').lower().removeprefix('eng:').removeprefix('justhodl-').replace('_','-')

def trust_factor(engine,doc,require_edge=False):
    row=next((r for r in doc.get('engines',[]) if canonical(r.get('signal_type'))==canonical(engine)),None)
    if not row:return 0.0 if require_edge else 0.5,{'status':'UNMATCHED','size_eligible':False}
    n=finite(row.get('regime_n'));lb=finite(row.get('regime_wilson_lb'));t=finite(row.get('net_alpha_t_stat'));edge=finite(row.get('net_alpha_excess_pct'))
    eligible=bool(n is not None and n>=20 and lb is not None and lb>0.5 and t is not None and t>=2 and edge is not None and edge>0 and row.get('alpha_status')=='ALPHA_PROVEN')
    trust=finite(row.get('effective_trust'));factor=max(0,min(1,trust)) if trust is not None else 0.5
    if require_edge and not eligible:factor=0.0
    return factor,{**row,'current_regime':doc.get('current_regime'),'source_as_of':stamp(doc),'size_eligible':eligible,'applied_factor':factor,'method':'one bounded engine-trust factor; not multiplied again by its underlying scorecard'}

def stock_context(stocks,docs):
    credit=docs.get('data/credit-before-equity.json',{});revision=docs.get('data/estimate-revisions.json',{});quality=docs.get('data/earnings-quality.json',{})
    cm=rows_by(credit,'names');rm={**rows_by(revision,'upward_revisions'),**rows_by(revision,'downward_revisions')};qm=rows_by(quality,'all_ranked')
    for row in stocks:
        sym=str(row.get('symbol') or row.get('ticker') or '').upper()
        row['cross_engine_context']={
            'credit':{'record':cm.get(sym),'as_of':stamp(credit),'estimate_type':'synthetic CDS / structural default model; not a traded CDS quote','coverage':{'degraded':credit.get('degraded'),'gaps':credit.get('gaps')}},
            'estimate_revisions':{'record':rm.get(sym),'as_of':stamp(revision),'comparison':'dated fiscal period; forward growth remains separate'},
            'earnings_quality':{'record':qm.get(sym),'as_of':stamp(quality),'sources':quality.get('sources'),'methodology':quality.get('methodology')},
        }
    return stocks

def conviction_members(members,trust,orth):
    # Replace the legacy scorecard multiplier with the trust composite (which already contains it).
    for m in members:
        factor,evidence=trust_factor(m['engine'],trust)
        m['skill_before_donor']=m['skill'];m['skill']=factor;m['trust_evidence']=evidence
    clusters=orth.get('clusters_high_redundancy') or []
    if finite(orth.get('snapshots_total')) is None or orth.get('snapshots_total',0)<20:return members
    for i,cluster in enumerate(clusters):
        names=cluster if isinstance(cluster,list) else cluster.get('engines',cluster.get('members',[])) if isinstance(cluster,dict) else []
        canonical_names={canonical(x) for x in names if isinstance(x,str)}
        group=[m for m in members if canonical(m['engine']) in canonical_names]
        if len(group)<2:continue
        # Empirical clusters supplement structural families; family weight becomes the more conservative grouping.
        group.sort(key=lambda m:abs(m.get('signal') or 0)*m['skill'],reverse=True)
        for rank,m in enumerate(group):
            penalty=1/(rank+1);m['skill']*=penalty;m['orthogonality']={'cluster':i,'factor':penalty,'source_as_of':stamp(orth),'effective_information_rank':orth.get('effective_information_rank'),'horizon_alignment':'donor snapshot cadence; not a new independent vote'}
    return members

def annotate_book(names,book):
    bm=rows_by(book,'equity_book')
    for r in names:
        sym=str(r.get('ticker') or r.get('symbol') or '').upper();existing=bm.get(sym)
        r['firm_exposure']={'existing':existing,'as_of':stamp(book),'book_type':'modeled desk allocation; not broker-reconciled inventory','already_owned':existing is not None,
                            'sector_exposure':book.get('sector_exposure'),'desk_conflicts':book.get('desk_conflicts')}
    return names

def true_flow_rows(metrics,doc):
    lookup=rows_by(doc,'by_etf');applied=0
    for row in metrics:
        t=str(row.get('ticker') or row.get('symbol') or '').upper();r=lookup.get(t)
        if not r:row['true_flow_status']='UNAVAILABLE';continue
        row['true_flow_context']={**r,'source_as_of':stamp(doc),'method':'delta shares × NAV','window_provenance':r.get('flow_windows'),'execution_pressure':False}
        if r.get('nav_source') in (None,'PRICE_FALLBACK_DEGRADED') or not r.get('flow_windows'):
            row['true_flow_status']='UNVERIFIED_NAV_OR_WINDOW';continue
        row['true_flow_status']='NAV_SHARE_ESTIMATE'
        for src,dst in [('net_flow_1d_usd','daily_flow_usd'),('net_flow_5d_usd','flow_5d_usd')]:
            value=finite(r.get(src))
            if value is not None and flow_window_valid(r,'1d' if src=='net_flow_1d_usd' else '5d'):row[dst+'_prior']=row.get(dst);row[dst]=value;applied+=1
        row['true_flow_20d_usd']=r.get('net_flow_20d_usd') if flow_window_valid(r,'20d') else None  # Never label 20 days as 21.
    return applied

def sector_flow_context(sectors,doc):
    lookup=rows_by(doc,'by_etf')
    for row in sectors:
        r=lookup.get(str(row.get('symbol','')).upper());row['nav_share_flow']={'record':r,'as_of':stamp(doc),'category_rotation':doc.get('category_rotation'),'complexes':doc.get('complexes'),'method':'capital flow; separate from return/price momentum'}
        if not r or r.get('nav_source') in (None,'PRICE_FALLBACK_DEGRADED') or not flow_window_valid(r,'5d'):continue
        flow=finite(r.get('net_flow_5d_usd'));tna=finite(r.get('tna'))
        if flow is None or tna is None or tna<=0:continue
        # Replace prior flow vote rather than add a second correlated flow vote.
        before=finite(row.get('rotation_score_preflow'));before=before if before is not None else finite(row.get('rotation_score'))
        if before is None:continue
        contribution=max(-10,min(10,100*flow/tna))
        row['rotation_score_before_true_flow']=row.get('rotation_score');row['rotation_score']=round(max(0,min(100,before+contribution)),1)
        row['true_flow_contribution']=contribution;row['etf_flow_5d_usd']=flow;row['true_flow_bps_tna']=round(10000*flow/tna,2)

def short_context(ticker,doc,now=None):
    row=rows_by(doc,'by_ticker').get(str(ticker).upper());result={'record':row,'source_as_of':stamp(doc),'classification':'delayed short-interest positioning; daily short-volume is separate','borrow_available':None,'locate_verified':False}
    if row:
        try:
            date=datetime.fromisoformat(str(row.get('settlement_date')).replace('Z','+00:00'));date=date.replace(tzinfo=timezone.utc) if not date.tzinfo else date
            age=((now or datetime.now(timezone.utc))-date).total_seconds()/86400;result['settlement_age_days']=age;result['positioning_usable']=0<=age<=35
        except (TypeError,ValueError):result['positioning_usable']=False
    else:result['positioning_usable']=False
    return result

def ticket_context(ticket,short_doc,options_doc):
    sym=ticket.get('ticker');ticket['short_positioning']=short_context(sym,short_doc)
    ticket['options_context']={'record':rows_by(options_doc,'board').get(str(sym).upper()),'source_as_of':stamp(options_doc),'gamma_assumption_sensitive':True,'executable_option_quote':False}
    ticket['execution_eligible']=False;ticket['execution_requirements']=['current executable bid/ask and depth','broker-reconciled buying power and limits','short locate, fee and recall status if short','contract multiplier and expiry if options']
    return ticket

def crypto_funding_context(surface,coin,doc):
    row=rows_by(doc,'by_coin').get(coin.upper());surface['perpetual_funding_context']={'record':row,'as_of':stamp(doc),'venue':doc.get('source'),'locked_carry':False}
    surface['execution_eligible']=False;surface['carry_semantics']='gross mark-based annualized basis; not obtainable net yield'
    surface['required_execution_inputs']=['executable spot/futures bid-ask and depth','fees and financing','margin/collateral and exchange risk']
    if row:
        interval=finite(row.get('funding_interval_hours'));rate=finite(row.get('current_funding_rate'))
        surface['perpetual_funding_context']['annualized_from_observed_interval_pct']=rate*24/interval*365*100 if interval and interval>0 and rate is not None else None
        surface['perpetual_funding_context']['interval_status']='OBSERVED' if interval else 'UNKNOWN'

SIZING_SPECS=[('data/liquidity-profile.json',48,('all_tickers',)),('data/liquidity-capacity.json',24,('firm',)),('data/factor-risk.json',48,('firm',)),('data/engine-trust.json',48,('engines',)),('data/crypto-basis.json',2,()),('data/khalid-risk.json',24,('policy',)),('portfolio/snapshot.json',24,('capital_book',)),('data/risk-gate.json',48,('sizing_multiplier',))]

def constrain_sizes(recs,docs,now=None):
    profile=docs.get('data/liquidity-profile.json',{});capacity=docs.get('data/liquidity-capacity.json',{});factor=docs.get('data/factor-risk.json',{});trust=docs.get('data/engine-trust.json',{})
    authority=authority_view(docs.get('data/khalid-risk.json',{}),now=now);book=capital_book_view(docs.get('portfolio/snapshot.json',{}),now=now);gate=docs.get('data/risk-gate.json',{})
    max_cap=authority['exposure_cap_pct'];entry=authority['allows_new_entries']
    multiplier=finite(gate.get('sizing_multiplier'))
    valid_authority=authority['status']=='FRESH' and book['status']=='READY' and max_cap is not None and 0<=max_cap<=100 and entry is True and multiplier is not None and 0<=multiplier<=1
    lc=capacity.get('firm') or {};fr=factor.get('firm') or {};var99=finite(fr.get('var_99_1d_pct'))
    constraints_ready=bool(profile and capacity and factor and finite(lc.get('n_unknown_volume'))==0 and var99 is not None and var99>=0)
    existing_gross_pct=sum(book['gross_weights'].values())*100+sum(book['order_weights'].values())*100
    remaining=max(0,max_cap-existing_gross_pct) if valid_authority and constraints_ready else 0.0
    lmap=rows_by(profile,'all_tickers');cmap=rows_by(capacity,'least_liquid_names');existing_by_name={};per_name={}
    # Capacity is a modeled research book. Keep its dollar basis explicit; never call it broker inventory.
    aum=book['equity_nav']
    for sym in set(book['gross_weights'])|set(book['order_weights']):
        weight=book['gross_weights'].get(sym,0)+book['order_weights'].get(sym,0)
        existing_by_name[sym]=weight*(aum or 0);per_name[sym]=weight*100
    for rec in recs:
        original=finite(rec.get('final_w_pct')) or 0;tick=str(rec.get('ticker','')).upper();liq=lmap.get(tick);cap=cmap.get(tick);reasons=[]
        tf,te=trust_factor(rec.get('engine'),trust,require_edge=True)
        # Existing quarter-Kelly uses outcome calibration; trust replaces its scorecard-derived scale, bounded to no boost.
        vol=finite((liq or {}).get('adv_usd'));bars=finite((liq or {}).get('n_bars'))
        observation=inspect_donor({'generated_at':stamp(profile),'observed_at':(liq or {}).get('observed_at')},'data/liquidity-profile.json#'+tick,48,observed_paths=('observed_at',),now=now,max_observation_age_hours=96)
        if not observation['usable']:vol=None
        liquid_cap_pct=(0.01*vol/max(aum,1)*100) if vol is not None and vol>0 and bars is not None and bars>=15 and aum is not None and aum>0 else 0.0
        if cap:
            comfortable=finite(cap.get('comfortable_position_usd'))
            if comfortable is not None and aum:liquid_cap_pct=min(liquid_cap_pct,max(0,comfortable-existing_by_name.get(tick,0))/aum*100)
        risk_factor=0 if var99 is None else max(0,min(1,(5.0-var99)/5.0))
        final=min(original*tf*(multiplier if valid_authority else 0)*risk_factor,liquid_cap_pct,remaining,max(0,5-per_name.get(tick,0)))
        if tick in ('BTC','ETH'):
            rec['crypto_basis_context']=(docs.get('data/crypto-basis.json',{}).get(tick.lower()) or None);final=0;reasons.append('crypto carry requires executable quotes, financing and collateral verification')
        if not valid_authority:reasons.append('missing, invalid or closed capital authority')
        if not constraints_ready:reasons.append('missing portfolio risk / liquidity coverage')
        if not tf:reasons.append('positive net out-of-sample edge and regime sample not established')
        if liquid_cap_pct==0:reasons.append('insufficient ADV/volume or consistent notional basis')
        final=max(0,math.floor(final*100)/100);remaining=max(0,remaining-final);per_name[tick]=per_name.get(tick,0)+final
        rec.update(final_w_pct=final,dollars_per_100k=round(final*1000),execution_eligible=False,
                   donor_constraints={'pre_donor_w_pct':original,'trust':te,'adv_usd':vol,'liquidity_observation':{k:v for k,v in observation.items() if k!='fields'},'liquidity_cap_pct':liquid_cap_pct,'risk_budget_factor':risk_factor,'var_99_1d_pct':var99,'capital_cap_pct':max_cap,'binding_reasons':reasons,
                                      'factor_model_scope':'existing modeled firm portfolio guard; not a computed marginal candidate VaR','authority':authority,'capital_book_status':book['status'],'book_errors':book['errors'],'portfolio_coverage':factor.get('coverage'),'risk_contributors':factor.get('risk_contributors'),'basis':'reconciled account NAV and reserved orders for capital; modeled liquidity/risk only'})
    return {'gross_final_pct':round(sum(r['final_w_pct'] for r in recs),2),'capital_cap_pct':max_cap,'constraints_ready':constraints_ready,'authority_usable':valid_authority,'execution_eligible':False}

def firm_board_contract(out,factor,capacity,book,now=None):
    now=now or datetime.now(timezone.utc)
    checks={
        'factor-risk':inspect_donor(factor,'data/factor-risk.json',48,required_paths=('firm','coverage'),now=now),
        'liquidity-capacity':inspect_donor(capacity,'data/liquidity-capacity.json',24,required_paths=('firm',),now=now),
        'firm-book':inspect_donor(book,'data/firm-book.json',6,required_paths=('firm',),now=now),
    }
    from donor_contract import parse_timestamp
    modeled= parse_timestamp(factor.get('firm_book_asof')) if isinstance(factor,dict) else None
    current= parse_timestamp(stamp(book)) if isinstance(book,dict) else None
    same_book=modeled is not None and current is not None and modeled==current
    unknown=finite((capacity.get('firm') or {}).get('n_unknown_volume'))
    ready=all(r['usable'] for r in checks.values()) and same_book and unknown==0
    out['donor_inputs']={k:{a:b for a,b in v.items() if a!='fields'} for k,v in checks.items()}
    out['factor_model_detail']=safe_evidence(factor)
    out['liquidity_detail']=safe_evidence(capacity)
    out['book_linkage']={'matched':same_book,'risk_model_book_asof':factor.get('firm_book_asof'),'current_book_asof':stamp(book),'book_type':'modeled desk book; execution requires separate broker reconciliation'}
    out['data_contract_status']='READY' if ready else 'DATA_HOLD'
    out['allows_new_entries']=False  # Research board does not confer order permission.
    if not ready:
        out['firm_posture_before_data_gate']=out.get('firm_posture');out['firm_posture']='DATA_HOLD'
        out['headline']='DATA_HOLD — stale/missing risk input, unaligned book snapshot or unknown exit liquidity.'
        out['cro_brief']='Risk completeness cannot be established. Recompute risk against the current modeled book and resolve unknown volume before relying on this board.'
        out['confidence']='UNVERIFIED';out['binding_constraint']={'dimension':'DATA_CONTRACT','label':'Risk input completeness','status':'DATA_HOLD','headline':out['headline']}
    return out
