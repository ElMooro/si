"""Source-backed, deterministic Calls brief. No model, credentials or HTTP.

Only the explicit public fields below may enter the public projection. Account
snapshots, positions, notes and arbitrary upstream narratives are never copied.
"""
import math
from datetime import datetime, timezone
from pd_fails_context import project

PUBLIC_KEY = 'data/ai-brief-public.json'
METHOD = 'warehouse_deterministic_v1'


def get(doc, path):
    for key in path.split('.'):
        if not isinstance(doc, dict): return None
        doc = doc.get(key)
    return doc


def finite(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None


def day(value):
    try: return datetime.strptime(str(value)[:10], '%Y-%m-%d').date().isoformat()
    except (TypeError, ValueError): return None


def build(load, generated_at):
    now = datetime.fromisoformat(generated_at.replace('Z','+00:00'))
    rows, missing, source_dates = [], [], {}
    def read(key):
        try: doc = load(key)
        except Exception: doc = {}
        if not isinstance(doc, dict): doc = {}
        source_dates[key] = day(doc.get('generated_at'))
        return doc
    def add(doc, key, field, unit, root, observation=None, frequency='daily', note=None):
        value = finite(get(doc, field))
        observed = day(observation)
        q = doc.get('quality') if isinstance(doc.get('quality'),dict) else {}
        status = q.get('status') if q.get('status') in {'fresh','stale','partial','unavailable','unverified'} else 'unverified'
        age = (now.date()-datetime.strptime(observed,'%Y-%m-%d').date()).days if observed else None
        limit = 14 if frequency=='weekly' else 92 if frequency=='monthly' else 5
        if value is None or age is None or age<0: status='unavailable'
        elif age>limit: status='stale'
        row={'series_id':key+'#'+field,'value':value,'unit':unit,'observation_date':observed,
             'quality_status':status,'frequency':frequency,'root_ids':root,
             'source':key,'role':'monitor_only','calls_eligible':False}
        if note: row['note']=note
        rows.append(row)
        if status!='fresh': missing.append(row['series_id'])
    liq_key='data/liquidity-flow.json';liq=read(liq_key)
    add(liq,liq_key,'current.net_liquidity_b','usd_bn',['WALCL','WTREGEN','RRPONTSYD'],
        get(liq,'quality.observation_date') or liq.get('as_of'),note='WALCL - WTREGEN - RRPONTSYD. Date is the aligned packet date; individual leg observation dates are not supplied here. Not investable cash or an easing signal.')
    settlement_key='data/settlement-fails.json';settlement=project(read(settlement_key),now)
    for scope in (settlement,settlement['ust_ex_tips']):
        for field in ('ftd_bn','ftr_bn','combined_bn'):
            keyfield=('treasury.' if scope['scope_id']=='treasury_incl_tips' else 'headline.')+('gross_bn' if field=='combined_bn' and scope['scope_id']=='treasury_incl_tips' else field)
            row={'quality':scope['quality'],'treasury':{'ftd_bn':scope['ftd_bn'],'ftr_bn':scope['ftr_bn'],'gross_bn':scope['combined_bn']},
                 'headline':{'ftd_bn':scope['ftd_bn'],'ftr_bn':scope['ftr_bn'],'combined_bn':scope['combined_bn']}}
            add(row,settlement_key,keyfield,'usd_bn',['FR2004'],scope['as_of'],'weekly',
                'Treasury including TIPS' if scope['scope_id']=='treasury_incl_tips' else 'Treasury excluding TIPS; overlaps the including-TIPS scope')
    key='data/ciss-stress.json';ciss=read(key)
    add(ciss,key,'ea_composite','index_0_1',['ECB:CISS'],ciss.get('ea_composite_date'))
    key='data/capital-inflows.json';tic=read(key)
    add(tic,key,'headline.foreign_net_into_us_lt_12mo_b','usd_bn',['US_TREASURY:TIC'],tic.get('data_asof'),'monthly',
        'Rolling twelve-month securities transactions, not valuation changes or a forecast of asset buying.')
    key='data/auction-crisis.json';auction=read(key)
    add(auction,key,'composite_score','score_0_100',['US_TREASURY:AUCTIONS'],get(auction,'freshness.latest_auction_date'),
        note='Demand monitoring score; not a probability of default or a failed auction.')
    key='data/risk-regime.json';risk=read(key)
    add(risk,key,'risk_regime_score','score_minus100_plus100',['COMPOSITE:UNMAPPED'],get(risk,'quality.observation_date'),
        note='Composite; shared roots are not yet fully mapped. It receives no independent vote.')
    key='data/market-extremes.json';extremes=read(key)
    add(extremes,key,'scores.top_risk','score_0_100',['COMPOSITE:UNMAPPED'],get(extremes,'quality.observation_date'),
        note='Monitoring score, not a dated market-top prediction or a Calls vote.')
    available=[r for r in rows if r['value'] is not None]
    fresh=[r for r in rows if r['quality_status']=='fresh']
    lines=['# Source-backed market brief','',
        'Generated '+generated_at+'. Deterministic warehouse synthesis; no model API calls.',
        '', '## DATA TAPE']
    for r in rows:
        value=str(r['value']) if r['value'] is not None else 'unavailable'
        lines.append('- '+r['series_id']+': '+value+' '+r['unit']+'; observation '+str(r['observation_date'] or 'unavailable')+'; quality '+r['quality_status']+'.')
    lines += ['', '## OBSERVATIONS',
        '- '+str(len(fresh))+' of '+str(len(rows))+' displayed fields have a fresh observation contract. This is field coverage, not independent evidence or predictive accuracy.',
        '- Treasury gross and UST ex-TIPS are overlapping scopes. FTD and FTR are two-sided settlement reports; neither scope measures unique defaults, new inflows or central-bank injection.',
        '- Liquidity is WALCL - WTREGEN - RRPONTSYD. Its aligned packet date does not establish that all three legs were observed on that date.',
        '- TIC is lagged securities transactions. Private/official holder composition must be inspected before interpreting its total.',
        '', '## DISAGREEMENTS',
        '- Low systemic-stress readings and a high market-extremes score can describe different risks. They must remain separate; this compiler does not choose a trade from unvalidated weights.',
        '- Composites may reuse the same series. Six settlement fields all share FR2004; they do not become six independent votes.',
        '', '## DATA LIMITATIONS',
        '- '+str(len(missing))+' fields are stale, incomplete or unverified. Missing observation dates are shown explicitly; publication time is never substituted.',
        '- No allocation model or out-of-sample edge is validated here. All fields are monitor-only, even when fresh. No target exposure, probability or position size is inferred.',
        '', '## WATCH CONDITIONS',
        '- Reassess when the named source observations change, their dates advance, or upstream quality deteriorates.',
        '- Any future allocation change requires eligible independent evidence, a validated model and a separately enforced risk limit.',
        '', '## DECISION STATUS', '**DECISIVE CALL: WAIT**',
        'Abstain from new allocation guidance. WAIT does not mean sell existing positions or override independent risk controls.']
    result={'version':'3.0','generated_at':generated_at,'generation_method':METHOD,
        'model':None,'paid_api_calls':0,'brief_md':'\n'.join(lines)+'\n',
        'evidence':rows,'coverage':{'fields':len(rows),'available':len(available),'fresh':len(fresh),'eligible_votes':0},
        'quality':{'status':'partial' if missing else 'fresh','missing':missing},
        'call_verb':'WAIT','decision_eligible':False,'sizing_eligible':False,
        'abstain_on_error':True,'visibility':'public_projection'}
    if not available: result['error']='no_public_observations'
    return result
