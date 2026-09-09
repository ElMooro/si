"""Guarded macro-engine donor joins. All scores are descriptive review signals.

No current-vintage history is admitted to point-in-time studies, no observed
financing proxy is an execution quote, and correlated panels do not add votes.
"""
from datetime import datetime, timezone, timedelta
from donor_contract import inspect_donor, numeric, get_path, parse_timestamp

REPO = 'data/repo-market.json'
FAILS = 'data/settlement-fails.json'
CISS = 'data/ciss-stress.json'
BOND_KEYS = (REPO, 'data/nyfed-primary-dealer.json', FAILS, 'data/term-premium.json',
             'data/auction-grades.json', 'data/tic-flows.json')


def unavailable(contract, reason):
    contract.update(usable=False, status='INVALID')
    contract['errors'].append(reason)
    return contract


def repo_context(doc, now=None):
    doc = doc if isinstance(doc,dict) else {}
    c = inspect_donor(doc, REPO, 72, observed_paths=('distribution.as_of', 'as_of'),
                      required_paths=('repo_stress_score', 'distribution.tail_bps', 'spreads'),
                      max_observation_age_hours=144, now=now,
                      units={'score':'0..100 stress', 'tail':'basis points', 'volume':'USD billions'})
    score = numeric(doc, 'repo_stress_score')
    if c['usable'] and (score is None or not 0 <= score <= 100 or numeric(doc, 'distribution.tail_bps') is None):
        unavailable(c, 'repo score/tail numeric domain invalid')
    return {'contract': c, 'score': score if c['usable'] else None,
            'regime': (doc or {}).get('regime') if c['usable'] else 'UNKNOWN',
            'distribution': (doc or {}).get('distribution') if c['usable'] else None,
            'spreads': (doc or {}).get('spreads') if c['usable'] else None,
            'facilities': (doc or {}).get('facilities') if c['usable'] else None,
            'facility_verification': {'srf': 'DATED' if get_path(doc, 'facilities.srf_as_of') else 'UNKNOWN'},
            'reserves': (doc or {}).get('reserves') if c['usable'] else None,
            'calendar': (doc or {}).get('calendar') if c['usable'] else None,
            'use': 'Funding review context; overlapping repo components are one evidence family.'}


def fails_context(doc, now=None):
    doc = doc if isinstance(doc,dict) else {}
    c = inspect_donor(doc, FAILS, 72, observed_paths=('treasury.as_of','as_of'),
                      required_paths=('treasury','classes','totals'), now=now,
                      max_observation_age_hours=24*21,
                      units={'amount':'USD billions', 'frequency':'weekly NY Fed FR2004'})
    treasury = (doc or {}).get('treasury') or {}
    if not isinstance(treasury,dict):
        treasury={}; unavailable(c,'Treasury fails schema must be an object')
    if c['usable'] and (treasury.get('complete') is not True or any(numeric(treasury,k) is None for k in ('ftd_bn','ftr_bn','gross_bn'))):
        unavailable(c, 'complete Treasury fails-to-deliver/receive/gross required')
    if c['usable'] and abs(numeric(treasury,'ftd_bn')+numeric(treasury,'ftr_bn')-numeric(treasury,'gross_bn')) > .15:
        unavailable(c, 'Treasury gross fails must reconcile to FTD plus FTR')
    score = numeric(treasury, 'score')
    if c['usable'] and score is not None and not 0 <= score <= 100:
        unavailable(c, 'Treasury fails score outside 0..100')
    return {'contract':c, 'treasury':treasury if c['usable'] else None,
            'classes':(doc or {}).get('classes') if c['usable'] else None,
            'totals':(doc or {}).get('totals') if c['usable'] else None,
            'watch':('ELEVATED' if score is not None and score >= 60 else 'OBSERVED') if c['usable'] else 'UNKNOWN',
            'source':'NY Fed primary-dealer weekly FR2004; includes Treasury ex-TIPS and TIPS',
            'score_contribution':0, 'overlap_policy':'Existing raw fails barometer components already count this evidence.'}


def ciss_context(doc, now=None):
    doc = doc if isinstance(doc,dict) else {}
    c = inspect_donor(doc, CISS, 72, observed_paths=('ea_composite_date',),
                      required_paths=('ea_composite','series','provenance'), now=now,
                      max_observation_age_hours=24*14,
                      units={'ea_composite':'index 0..1', 'country_series':'producer-reported frequency'})
    value = numeric(doc,'ea_composite')
    if c['usable'] and (value is None or not 0 <= value <= 1):
        unavailable(c, 'CISS composite must be finite within 0..1')
    rows = (doc or {}).get('series') or []
    if not isinstance(rows,list):
        rows=[]; unavailable(c,'CISS series schema must be an array')
    return {'contract':c, 'ea_composite':value if c['usable'] else None,
            'ea_regime':(doc or {}).get('ea_regime') if c['usable'] else 'UNKNOWN',
            'active_series':[r for r in rows if isinstance(r,dict) and not r.get('discontinued')] if c['usable'] else [],
            'legacy_discontinued_series':[r for r in rows if isinstance(r,dict) and r.get('discontinued')] if c['usable'] else [],
            'categories':(doc or {}).get('categories') if c['usable'] else None,
            'frequency_note':(doc or {}).get('frequency_note'), 'provenance':(doc or {}).get('provenance'),
            'score_contribution':0, 'overlap_policy':'Context only; existing credit/SovCISS inputs must not vote twice.'}


def credit_donors(repo, ciss, base_score, now=None):
    r, c = repo_context(repo,now), ciss_context(ciss,now)
    base = numeric(base_score)
    # A funding warning can raise the review floor; max avoids additive double counting.
    score = max(base, r['score']) if base is not None and r['score'] is not None else base
    return {'schema_version':'1.0', 'repo_market':r, 'ciss_stress':c,
            'base_score':base, 'review_score':score,
            'score_rule':'max(base credit/liquidity score, verified repo funding stress); CISS contributes zero',
            'calibration_status':'HEURISTIC_REVIEW_ONLY', 'execution_eligible':False,
            'coverage_status':'COMPLETE' if r['contract']['usable'] and c['contract']['usable'] else 'DEGRADED'}


def bond_donors(docs, now=None):
    r = repo_context(docs.get(REPO),now)
    f = fails_context(docs.get(FAILS),now)
    pd = docs.get(BOND_KEYS[1]) or {}
    pd = pd if isinstance(pd,dict) else {}
    # Source keys verified against the live primary-dealer artifact.
    pc = inspect_donor(pd,BOND_KEYS[1],24*8,observed_paths=('as_of',),
                       required_paths=('by_tenor_usd_b','net_positions_usd_b','wow_usd_b','z_52w','financing','transactions'),now=now,max_observation_age_hours=24*21,
                       units={'positions':'USD billions; net dealer inventory', 'frequency':'weekly'})
    if pc['usable'] and any(not isinstance(pd.get(k),dict) for k in ('by_tenor_usd_b','net_positions_usd_b','wow_usd_b','z_52w','financing','transactions')):
        unavailable(pc,'Dealer inventory/financing/transactions fields must be objects')
    tenor_rows=[]
    if pc['usable']:
        for collateral,tenors in pd['by_tenor_usd_b'].items():
            if not isinstance(tenors,dict):continue
            for tenor,value in tenors.items():
                amount=numeric(value)
                if amount is not None:
                    tenor_rows.append({'collateral':collateral,'tenor_years':tenor,'net_inventory_usd_bn':amount})
    tenor_rows.sort(key=lambda row:abs(row['net_inventory_usd_bn']),reverse=True)
    zscores=[abs(numeric(v)) for v in (pd.get('z_52w') or {}).values() if numeric(v) is not None] if pc['usable'] else []
    dealer_review='ELEVATED_INVENTORY' if zscores and max(zscores)>=2 else 'OBSERVED' if pc['usable'] else 'UNKNOWN'
    tp = docs.get(BOND_KEYS[3]) or {}
    tp = tp if isinstance(tp,dict) else {}
    tc = inspect_donor(tp,BOND_KEYS[3],72,observed_paths=('latest.date',),
                       required_paths=('latest','decomposition','source'),now=now,max_observation_age_hours=144,
                       units={'yield':'percent', 'deltas':'basis points'})
    decomp = tp.get('decomposition') or {}
    decomp = decomp if isinstance(decomp,dict) else {}
    ys = [numeric(decomp,k) for k in ('acm_fitted_10y_pct','risk_neutral_10y_pct','term_premium_10y_pct')]
    identity = abs(ys[0]-ys[1]-ys[2]) if all(v is not None for v in ys) else None
    if tc['usable'] and (identity is None or identity > .02):
        unavailable(tc,'ACM fitted yield must reconcile to expected-rate component plus term premium within 2bp')
    auctions = docs.get(BOND_KEYS[4]) or {}
    auctions = auctions if isinstance(auctions,dict) else {}
    ac = inspect_donor(auctions,BOND_KEYS[4],72,required_paths=('graded_auctions',),now=now)
    if ac['usable'] and not isinstance(auctions.get('graded_auctions'),list):
        unavailable(ac,'graded_auctions must be an array')
    rows=[]
    if ac['usable']:
        clock=now or datetime.now(timezone.utc)
        for row in auctions.get('graded_auctions') or []:
            if not isinstance(row,dict): continue
            d=parse_timestamp(row.get('auction_date'))
            if not d or d > clock or (clock-d).days > 90: continue
            item=dict(row)
            tail=numeric(row,'dimensions.tail_bp.value')
            # An actual WI price/yield with its quote time is mandatory, even if a tail was populated.
            wi=numeric(row,'when_issued.yield_pct')
            quote=parse_timestamp(get_path(row,'when_issued.available_at'))
            auction_time=parse_timestamp(row.get('auction_timestamp'))
            valid=tail is not None and wi is not None and quote is not None and auction_time is not None and quote <= auction_time
            item['tail_validation']={'status':'READY' if valid else 'BLOCKED_MISSING_MATCHED_WHEN_ISSUED',
                                     'tail_bps':tail if valid else None,
                                     'requires':['CUSIP-matched when-issued yield','quote available before auction','auction timestamp']}
            rows.append(item)
    tic=docs.get(BOND_KEYS[5]) or {}
    tic=tic if isinstance(tic,dict) else {}
    ticc=inspect_donor(tic,BOND_KEYS[5],72,required_paths=('total_foreign_holdings','net_purchases'),now=now)
    # Distinguish survey valuation changes from actual SLT transactions.
    tx=tic.get('net_purchases') or {}
    tx=tx if isinstance(tx,dict) else {}
    proven=ticc['usable'] and tx.get('source_dataset') == 'SLT' and numeric(tx,'transactions_usd_bn') is not None and parse_timestamp(tx.get('period')) is not None
    if proven:
        age=((now or datetime.now(timezone.utc))-parse_timestamp(tx['period'])).days
        proven=0 <= age <= 120
    confluence='UNKNOWN'
    if r['contract']['usable'] and f['contract']['usable']:
        # Different daily/weekly horizons remain explicit; this is concurrent review evidence, not a same-day causal join.
        confluence='FUNDING_AND_SETTLEMENT_REVIEW' if r['score'] >= 60 and f['watch']=='ELEVATED' else 'FUNDING_REVIEW' if r['score'] >=60 else 'SETTLEMENT_REVIEW' if f['watch']=='ELEVATED' else 'NO_ELEVATED_DONOR_FLAGS'
    return {'schema_version':'1.0','repo_market':r,'settlement_fails':f,
            'dealer_inventory':{'contract':pc,'data':pd if pc['usable'] else None,'inventory_review':dealer_review,'tenor_inventory_rank':tenor_rows,
                'fails_normalization':{'status':'BLOCKED','reason':'Require matched-week collateral-specific transaction volume and comparable gross fails; inventory is not a volume denominator.'}},
            'term_premium':{'contract':tc,'data':tp if tc['usable'] else None,'identity_error_pct':identity,
                            'interpretation':'ACM model decomposition of fitted yield; model estimates are not observed policy expectations.'},
            'auction_quality':{'contract':ac,'graded_auctions':rows,'by_tenor':auctions.get('by_tenor') if ac['usable'] else None},
            'foreign_demand':{'contract':ticc,'transaction_status':'READY' if proven else 'BLOCKED_MISSING_SLT_TRANSACTIONS',
                'net_transactions_usd_bn':numeric(tx,'transactions_usd_bn') if proven else None,
                'holdings_context':tic.get('total_foreign_holdings') if ticc['usable'] else None,
                'interpretation':'Holdings changes include valuation; indirect auction awards do not identify foreign buyers.'},
            'funding_review':confluence,'numeric_score_contribution':0,
            'overlap_policy':'Review flags complement existing funding metrics; repo/fails/TIC never add repeated independent score votes.'}


def bis_context(doc, now=None):
    doc = doc if isinstance(doc,dict) else {}
    c=inspect_donor(doc,'data/bis-crossborder.json',24*8,observed_paths=('total.period',),
                    required_paths=('source','by_counterparty','total.latest_bn'),now=now,
                    max_observation_age_hours=24*220,units={'claims':'USD billions','frequency':'quarterly'})
    if c['usable'] and 'CBS' not in str((doc or {}).get('source','')).upper():
        unavailable(c,'Expected BIS consolidated banking statistics CBS source')
    return {'contract':c,'source':'BIS CBS consolidated foreign claims; all-currency exposure context',
            'total':(doc or {}).get('total') if c['usable'] else None,
            'by_counterparty':(doc or {}).get('by_counterparty') if c['usable'] else [],
            'offshore_centres':(doc or {}).get('offshore_centres') if c['usable'] else None,
            'em_asia':(doc or {}).get('em_asia') if c['usable'] else None,
            'errors':(doc or {}).get('errors') or [], 'score_contribution':0,
            'lbs_usd_funding':{'status':'BLOCKED_MISSING_LBS','data':None,
                              'requires':['BIS LBS currency USD','bank/non-bank counterparty sectors','FX-adjusted cross-border flows','observation and publication timestamps']}}


def capacity_donors(firm, repo, now=None):
    c=inspect_donor(firm,'data/firm-book.json',48,required_paths=('equity_book','firm'),now=now,
                    units={'weights':'percent of modeled capital'})
    if c['usable'] and not isinstance(firm.get('equity_book'),list):
        unavailable(c,'equity_book must be an array')
    if c['usable']:
        for row in firm['equity_book']:
            gross,net=numeric(row,'gross_pct'),numeric(row,'net_pct')
            if not isinstance(row,dict) or not row.get('symbol') or gross is None or net is None or gross < abs(net):
                unavailable(c,'Every modeled position requires symbol and finite gross >= abs(net)')
                break
    return {'firm_book':{'contract':c,'exposure_basis':'MODEL_DESK_GROSS_AND_NET',
                        'firm':(firm or {}).get('firm') if c['usable'] else None,
                        'desk_conflicts':(firm or {}).get('desk_conflicts') if c['usable'] else None,
                        'actual_broker_ledger':False},
            'repo_market':repo_context(repo,now),
            'funding_capacity':{'status':'BLOCKED_UNCALIBRATED','execution_eligible':False,
                'requires':['timestamped trailing dollar ADV and spread/depth','historical repo states','out-of-sample participation/impact calibration'],
                'scenario_participation_pct':[20,10,5],
                'interpretation':'Sensitivity assumptions only; systemic repo is not observed equity tradability.'}}


def vintage_net_liquidity(docs, now=None):
    """Release-time series in USD millions; date-only availability is next UTC day.

    A vintage can revise an old observation after release. We choose the latest
    observation known at each release, never move its value back to observation day.
    """
    clock=now or datetime.now(timezone.utc)
    units={'WALCL':1.0,'WTREGEN':1.0,'RRPONTSYD':1000.0}
    contracts={}; events=[]; missing=[]; vintage_identity={}; conflicts=[]
    for sid,multiplier in units.items():
        doc=docs.get(sid)
        c=inspect_donor(doc,'data/vintage/'+sid+'.json',24*8,required_paths=('vintages',),now=clock,
                        units={'source':'USD billions' if sid=='RRPONTSYD' else 'USD millions','output':'USD millions'})
        contracts[sid]=c
        if not c['usable'] or not isinstance((doc or {}).get('vintages'),list):
            missing.append(sid);continue
        count=0
        for row in doc['vintages']:
            if not isinstance(row,dict):continue
            observed=parse_timestamp(row.get('date'))
            value=numeric(row,'value')
            raw=row.get('available_at') or row.get('known_on')
            available=parse_timestamp(raw)
            if available and isinstance(raw,str) and len(raw)==10:
                available+=timedelta(days=1)
            if not available or not observed or value is None or available < observed or available > clock:continue
            identity=(sid,observed,available)
            if identity in vintage_identity and vintage_identity[identity] != value:
                conflicts.append(sid+': conflicting values for identical vintage availability')
            vintage_identity[identity]=value
            events.append((available,sid,observed,value*multiplier));count+=1
        c['valid_vintage_rows']=count
        if not count:missing.append(sid)
    result={'schema_version':'1.0','status':'BLOCKED' if missing else 'READY','point_in_time':True,
            'contracts':contracts,'missing_series':missing,'units':'USD millions','series':{},
            'availability_rule':'known_on date becomes available next UTC day; explicit available_at UTC honored; then forward fill',
            'history_semantics':'Release-time latest-known observation; contemporaneous decisions after availability only',
            'publication_eligible':False}
    if missing:return result
    if conflicts:
        result.update(status='BLOCKED',reason='; '.join(sorted(set(conflicts))))
        return result
    state={}; by_day={}
    for available,sid,observed,value in sorted(events):
        state.setdefault(sid,{})[observed]=value
        if len(state)==3:
            latest={s:max(obs) for s,obs in state.items()}
            # A missing current component is not silently forwarded indefinitely.
            if any((available-latest[s]).days>14 for s in state):continue
            levels={s:state[s][latest[s]] for s in state}
            # Intraday data are conservatively exposed on the next date unless known at midnight.
            decision=available if available.time()==datetime.min.time() else available+timedelta(days=1)
            by_day[decision.date().isoformat()]=levels['WALCL']-levels['WTREGEN']-levels['RRPONTSYD']
    if by_day:
        day=min(parse_timestamp(d) for d in by_day);last=None;last_release=None
        while day<=clock:
            key=day.date().isoformat()
            if key in by_day:last=by_day[key];last_release=day
            if last is not None and (day-last_release).days<=14 and day.weekday()<5:result['series'][key]=last
            day+=timedelta(days=1)
    result['n_points']=len(result['series'])
    if len(result['series'])<96:
        result.update(status='BLOCKED',reason='Fewer than 96 release-aware business observations for 65-day slope and 31-observation z-score warmup')
    return result


def fragmentation_context(doc, countries, now=None):
    context=ciss_context(doc,now)
    joined=[]
    for code,country in countries.items():
        for row in context['active_series']:
            if row.get('area') != code and row.get('country') not in (code,country.get('name')):continue
            joined.append({'country':code,'indicator':row.get('id'), 'category':row.get('category'),
                           'ciss_value':row.get('latest'),'ciss_as_of':row.get('latest_date'),
                           'spread_bps':country.get('spread_vs_bund_bp'),'spread_as_of':country.get('spread_as_of'),
                           'spread_maturity_years':10,
                           'join_status':'SAME_OBSERVATION_DATE' if row.get('latest_date') and row['latest_date']==country.get('spread_as_of') else 'CONTEXT_DIFFERENT_DATES',
                           'score_contribution':0})
    context['country_spread_context']=joined
    return context
