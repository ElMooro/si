"""Deterministic Alpha research, with typed public inputs and no allocation authority.

Retained projections reproduce this compiler, not the original market providers.
Vocabulary matches describe candidate cohorts; they are never a validated model.
"""
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import math
import re

CONTRACT = 'alpha-research.v1'
METHOD = 'qualified-research-desk.v3'
PREFIX = 'data/alpha-research/'
CURRENT = 'data/alpha-compass.json'
BRIEF = 'data/alpha-brief.json'
SOURCES = {
    'conviction': 'data/conviction.json',
    'magnitude_distributions': 'data/magnitude-distributions.json',
    'scorecard': 'data/signal-scorecard.json',
    'regime_composite': 'data/regime-composite.json',
    'risk_regime': 'data/risk-regime.json',
    'factor_regime': 'data/factor-regime.json',
    'dollar_radar': 'data/dollar-radar.json',
    'kill_theses': 'data/kill-theses.json',
    'best_setups': 'data/best-setups.json',
    'miss_summary': 'data/miss-summary.json',
    'engine_signal_map': 'data/engine-signal-map.json',
    'settlement_fails': 'data/settlement-fails.json',
}
THEMES = ('Crypto', 'US macro / housing cycle', 'US equity — positioning',
          'US equity — value tilt', 'Broad risk / equity beta',
          'Cross-asset relative value', 'Supply shortage / scarcity')
ENGINES = {
    'Crypto Narratives': 'crypto', 'Crypto Confluence': 'crypto-synth',
    'Housing Cycle': 'macro-fundamental', 'Short Pressure': 'positioning',
    'Scarcity Radar': 'scarcity-synth', 'Crisis Composite': 'crisis-monitor',
    'Risk Regime': 'roro-synth', 'Canary Grid': 'crisis-monitor',
    'Leading Markets': 'market-regime', 'Cross-Asset RV': 'relative-value',
    'Opportunity Engine': 'equity-value', 'Best Setups': 'setups-synth',
    'Fundamentals X-Ray': 'equity-value', 'Mean Reversion': 'equity-value',
}
DIRECTIONS = ('RISK-ON / LONG', 'RISK-OFF / DEFENSIVE', 'NEUTRAL / MIXED')
REGIME_FIELDS = {'regime_composite': 'meta_regime', 'risk_regime': 'risk_regime',
                 'factor_regime': 'risk_appetite_read', 'dollar_radar': 'regime'}
REGIMES = {'RISK-ON', 'RISK-OFF', 'NEUTRAL', 'MIXED', 'DEFENSIVE', 'OFFENSIVE',
           'RISK ON', 'RISK OFF', 'LEAN PUMP', 'LEAN DUMP', 'PUMP', 'DUMP',
           'CAUTION', 'FRAGILE', 'STABLE', 'STRESS', 'CRISIS', 'BALANCED', 'UNAVAILABLE',
           'RISK_ON', 'RISK_OFF', 'MILD_RISK_ON', 'MILD_RISK_OFF'}
BLOCKS = ['source_model_not_validated', 'original_provider_lineage_incomplete',
          'point_in_time_protocol_missing', 'independent_out_of_sample_validation_missing',
          'cost_capacity_and_borrow_validation_missing', 'portfolio_risk_and_limits_not_bound']


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value): return hashlib.sha256(encoded(value)).hexdigest()


def clock(value):
    if not isinstance(value, str) or len(value) > 40: raise ValueError('dated timezone-aware clock required')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None: raise ValueError('timezone required')
    return stamp.astimezone(timezone.utc)


def number(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and abs(value) < 1e16 else None


def count(value):
    value = number(value)
    return int(value) if value is not None and 0 <= value <= 1e9 and int(value) == value else None


def token(value):
    return value if isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_:. /-]{1,100}', value) else None


def symbol(value):
    return value if isinstance(value, str) and re.fullmatch(r'[A-Z][A-Z0-9.-]{0,11}', value) else None


def rows(value, maximum=4000):
    if value is None: return []
    if not isinstance(value, list) or len(value) > maximum: raise ValueError('bounded row array required')
    if any(not isinstance(row, dict) for row in value): raise ValueError('object rows required')
    return value


def timestamp(value):
    try: clock(value); return value
    except (TypeError, ValueError, OverflowError): return None


def base(doc):
    quality = doc.get('quality') if isinstance(doc.get('quality'), dict) else {}
    return {'generated_at': timestamp(doc.get('generated_at')),
            'declared_quality': quality.get('status') if quality.get('status') in ('fresh','stale','partial','unverified','unavailable') else None,
            'declared_calls_eligible': doc.get('calls_eligible') is True,
            'declared_sizing_eligible': doc.get('sizing_eligible') is True}


def project(name, doc):
    """Idempotent whitelist: no arbitrary prose, book, owner notes or account fields."""
    if name not in SOURCES: raise ValueError('unregistered public research input')
    doc = doc if isinstance(doc, dict) else {}
    # Projection validation is separate from raw input parsing; raw source fields
    # cannot declare themselves a trusted projection by supplying a marker.
    out = base(doc)
    if name == 'conviction':
        selected = []
        for row in rows(doc.get('setups'), 200):
            if row.get('subject') not in THEMES: continue
            contributors = rows(row.get('contributing_engines'), 100)
            excluded = any(e.get('engine') not in ENGINES or ENGINES[e['engine']] != e.get('family') for e in contributors)
            selected.append({'subject': row['subject'],
                'direction': row.get('direction') if not excluded and row.get('direction') in DIRECTIONS else None,
                'reported_conviction': number(row.get('conviction')) if not excluded else None,
                'source_rank': count(row.get('rank')) if not excluded else None,
                'aggregate_withheld': excluded,
                'contributors': [{'name': e['engine'], 'family': e['family'], 'reported_signal': number(e.get('signal'))}
                                 for e in contributors if e.get('engine') in ENGINES and ENGINES[e['engine']] == e.get('family')]})
        subjects = [r['subject'] for r in selected]
        if len(subjects) != len(set(subjects)): raise ValueError('duplicate research subject')
        out['ideas'] = selected
    elif name == 'magnitude_distributions':
        selected = []
        for row in rows(doc.get('stacks')):
            signals = row.get('signals')
            if not isinstance(signals, list) or not signals or len(signals) > 100 or any(token(s) is None for s in signals): continue
            selected.append({'signals': sorted(set(signals)), 'horizon_days': count(row.get('horizon_days')),
                'n': count(row.get('n')), **{k: number(row.get(k)) for k in ('mean','median','p25','p75','win_rate')},
                'return_unit': row.get('return_unit') if row.get('return_unit') in ('percent','fraction') else None})
        out['cohorts'] = selected
    elif name == 'scorecard':
        out['cohorts'] = []
        for row in rows(doc.get('scorecard')):
            name_ = token(row.get('signal_type'))
            if not name_: continue
            out['cohorts'].append({'signal_type': name_, **{k: count(row.get(k)) for k in ('n_scored','n_quarantined','n_total','hits')},
                **{k: number(row.get(k)) for k in ('hit_rate','wilson_lb','wilson_ub','avg_return_pct')},
                'declared_sizing_eligible': row.get('sizing_eligible') is True,
                'validation_scope': row.get('validation_scope') if row.get('validation_scope') in ('DESCRIPTIVE_VERIFIED_MARKS',) else None})
    elif name == 'engine_signal_map':
        mapping = doc.get('by_family') if isinstance(doc.get('by_family'), dict) else {}
        out['by_family'] = {}
        for family in sorted(set(ENGINES.values())):
            candidates = mapping.get(family)
            if isinstance(candidates, list) and len(candidates) <= 500:
                out['by_family'][family] = sorted(set(t for t in candidates if token(t)))
    elif name in REGIME_FIELDS:
        label = doc.get(REGIME_FIELDS[name])
        out['label'] = label if label in REGIMES else None
        out['source_field'] = REGIME_FIELDS[name]
    elif name in ('best_setups','kill_theses'):
        candidates = doc.get('theses') if name == 'kill_theses' else next((doc[k] for k in ('top_setups','setups','rows','best_setups','top','items','ranked') if isinstance(doc.get(k),list)), [])
        out['symbols'] = sorted(set(t for r in rows(candidates) for t in [symbol(r.get('ticker') or r.get('symbol'))] if t))
        # Price levels, thesis prose and user-derived notes remain outside the public projection.
    elif name == 'miss_summary':
        totals = doc.get('totals') if isinstance(doc.get('totals'),dict) else {}
        out['reported_counts'] = {k: count(totals.get(k)) for k in ('out_of_universe','timing','direction','not_detected')}
    elif name == 'settlement_fails':
        for scope, key in (('treasury','gross_bn'),('headline','combined_bn')):
            row = doc.get(scope) if isinstance(doc.get(scope),dict) else {}
            quality = row.get('quality') if isinstance(row.get('quality'),dict) else {}
            declared_scope=row.get('scope_id') if row.get('scope_id') is not None else row.get('scope')
            field_units=row.get('field_units') if isinstance(row.get('field_units'),dict) else {}
            def declared_unit(value):
                return value if value in (None,'usd_bn','USD_bn_par') else '__unrecognized_unit__'
            date = row.get('as_of')
            try: datetime.strptime(date, '%Y-%m-%d')
            except (TypeError,ValueError): date = None
            out[scope] = {'as_of': date, 'scope_id': declared_scope if declared_scope in ('treasury_incl_tips','ust_ex_tips') else None,
                'unit': declared_unit(row.get('unit')),
                'field_units':{field:declared_unit(field_units.get(field)) for field in ('ftd_bn','ftr_bn',key)},
                'complete': row.get('complete') is not False, 'declared_quality': quality.get('status') if quality.get('status') in ('fresh','stale') else None,
                'ftd_bn': number(row.get('ftd_bn')), 'ftr_bn': number(row.get('ftr_bn')), 'combined_bn': number(row.get(key))}
    return out


def source_status(row, at):
    source = row['projection']; stamp = source.get('generated_at')
    acquired = clock(row['acquired_at']); current = clock(at)
    if acquired > current: raise ValueError('input acquired after decision time')
    source_at = clock(stamp) if stamp else None
    age = (current-source_at).total_seconds()/3600 if source_at else None
    status = ('unavailable' if row['status'] != 'captured_projection' else
              'clock_unverified' if age is None or age < 0 or source_at > acquired else
              'stale' if age > 26 or (current-acquired).total_seconds() > 26*3600 else 'current_packet')
    return {'status': status, 'generated_at': stamp, 'acquired_at': row['acquired_at'],
            'packet_age_hours': round(age,3) if age is not None else None, 'max_packet_age_hours':26,
            'declared_quality':source['declared_quality'], 'original_provider_verified':False,
            'note':'Packet age describes transport recency, not observation freshness or model validity.'}


def validate_projection(name,p):
    """Reconstruct only allowlisted raw fields; reject additions and invalid typed values."""
    raw={'generated_at':p.get('generated_at'),'quality':{'status':p.get('declared_quality')},
         'calls_eligible':p.get('declared_calls_eligible'),'sizing_eligible':p.get('declared_sizing_eligible')}
    if name=='conviction':
        raw['setups']=[]
        for idea in p['ideas']:
            engines=[{'engine':e['name'],'family':e['family'],'signal':e['reported_signal']} for e in idea['contributors']]
            if idea['aggregate_withheld']: engines.append({'engine':'__excluded__'})
            raw['setups'].append({'subject':idea['subject'],'direction':idea['direction'],
                'conviction':idea['reported_conviction'],'rank':idea['source_rank'],'contributing_engines':engines})
    elif name=='magnitude_distributions': raw['stacks']=p['cohorts']
    elif name=='scorecard':
        raw['scorecard']=[{**r,'sizing_eligible':r['declared_sizing_eligible']} for r in p['cohorts']]
    elif name=='engine_signal_map': raw['by_family']=p['by_family']
    elif name in REGIME_FIELDS: raw[REGIME_FIELDS[name]]=p['label']
    elif name in ('best_setups','kill_theses'):
        raw['theses' if name=='kill_theses' else 'setups']=[{'symbol':s,'ticker':s} for s in p['symbols']]
    elif name=='miss_summary': raw['totals']=p['reported_counts']
    elif name=='settlement_fails':
        for scope,key in (('treasury','gross_bn'),('headline','combined_bn')):
            r=p[scope];raw[scope]={**r,key:r['combined_bn'],'quality':{'status':r['declared_quality']}}
    if project(name,raw)!=p: raise ValueError('public Alpha projection schema or whitelist differs')


def fails(source, at, availability):
    scopes = {}
    for key, scope in (('treasury','treasury_incl_tips'),('headline','ust_ex_tips')):
        r = source[key]; reasons=[]
        if r['scope_id'] != scope: reasons.append('scope_unverified')
        units=list(r['field_units'].values())
        units_ok=r['unit'] in (None,'usd_bn','USD_bn_par') and (
            all(u=='usd_bn' for u in units) or (r['unit'] in ('usd_bn','USD_bn_par') and all(u is None for u in units)))
        if not units_ok: reasons.append('unit_unverified')
        vals=[r[k] for k in ('ftd_bn','ftr_bn','combined_bn')]
        if any(x is None or x < 0 for x in vals): reasons.append('incomplete_values')
        elif abs(Decimal(str(vals[0]))+Decimal(str(vals[1]))-Decimal(str(vals[2]))) > Decimal('0.02'): reasons.append('gross_reconciliation_failed')
        age=(clock(at).date()-datetime.strptime(r['as_of'],'%Y-%m-%d').date()).days if r['as_of'] else None
        if age is None or not 0 <= age <= 14 or availability['status'] != 'current_packet' or r['declared_quality'] != 'fresh': reasons.append('stale_or_unverified_source')
        if not r['complete']: reasons.append('incomplete_scope')
        scopes[key] = {**r, 'scope_id':scope, 'status':'context_only' if not reasons else 'withheld', 'reasons':reasons,
                       'display_values':None if reasons else {k:r[k] for k in ('ftd_bn','ftr_bn','combined_bn')}, 'original_provider_verified':False}
    return {**scopes['treasury'], 'ust_ex_tips':scopes['headline'], 'calls_eligible':False,
            'note':'Separate two-sided gross FR2004 scopes. Do not add scopes or interpret as defaults, flows or unique securities.'}


def build(inputs):
    at=inputs['generated_at']; clock(at)
    if set(inputs['sources']) != set(SOURCES): raise ValueError('complete registered source inventory required')
    sources=inputs['sources']; inventory={}
    for name,row in sources.items():
        validate_projection(name,row['projection'])
        if row['source'] != SOURCES[name] or digest(row['projection']) != row['projection_sha256']: raise ValueError('projection identity differs')
        if row['status'] not in ('captured_projection','source_unavailable','source_read_failed','source_contract_rejected'): raise ValueError('invalid source status')
        inventory[name]={'source':row['source'],'projection_sha256':row['projection_sha256'],**source_status(row,at)}
    conv=sources['conviction']['projection']; mapping=sources['engine_signal_map']['projection']['by_family']
    cards=[]; families={}
    # Source rank is retained as context; display order is fixed by the reviewed universe.
    for subject in THEMES:
        idea=next((r for r in conv['ideas'] if r['subject']==subject),None)
        if idea is None: continue
        contributors=idea['contributors']; candidates=set()
        for engine in contributors:
            family=engine['family']; families.setdefault(family,[]).append(subject)
            candidates.update((family.lower(),engine['name'].lower()))
            candidates.update(s.lower() for s in mapping.get(family,[]))
        distributions=[]; scorecards=[]
        for index,row in enumerate(sources['magnitude_distributions']['projection']['cohorts']):
            if set(s.lower() for s in row['signals']).issubset(candidates):
                distributions.append({**row,'source_row':index,'match_basis':'vocabulary_only','qualified':False})
        for index,row in enumerate(sources['scorecard']['projection']['cohorts']):
            if row['signal_type'].lower() in candidates:
                scorecards.append({**row,'source_row':index,'match_basis':'vocabulary_only','qualified':False})
        reasons=list(BLOCKS)
        if idea['aggregate_withheld']: reasons.append('unregistered_or_private_derived_contributor_excluded')
        if inventory['conviction']['status'] != 'current_packet': reasons.append('source_packet_not_current')
        if not distributions and not scorecards: reasons.append('no_matching_descriptive_cohort')
        card={'subject':subject,'direction':idea['direction'],'reported_conviction':idea['reported_conviction'],
              'source_rank':idea['source_rank'],'aggregate_withheld':idea['aggregate_withheld'],
              'engines':contributors,'source':SOURCES['conviction'],'source_projection_sha256':sources['conviction']['projection_sha256'],
              'source_generated_at':conv['generated_at'],'source_status':inventory['conviction']['status'],
              'call':None,'action':'WAIT','action_meaning':'abstain','calls_eligible':False,'sizing_eligible':False,
              'execution_eligible':False,'conviction':None,'independent_evidence_count':None,
              'stats':{'source':'unqualified_descriptive_context','n':None,'median':None,'win_rate':None,
                       'distributions':distributions,'scorecards':scorecards,'candidate_tokens':sorted(candidates),
                       'note':'No best-cohort selection, pooled counts, inferred units, or probability from a lower confidence bound.'},
              'sizing':{'kelly_pct':None,'dollar_at_100k':None,'basis':'No qualified payoff distribution or account-bound risk budget.'},
              'express':{'primary':None,'names':[],'vehicles':[],'side':'WATCH'},
              'stop_pct':None,'target_pct':None,'stop_target_basis':None,
              'portfolio_effect':{'status':'not_estimated','target_weight':None,'delta_weight':None,'expected_pnl':None,
                                  'meaning':'WAIT is abstention. It does not instruct closing or holding an existing position.'},
              'qualification_gaps':reasons}
        card['decision_id']='alpha-'+digest({'at':at,'card':card,'input_hashes':{k:v['projection_sha256'] for k,v in sources.items()}})
        cards.append(card)
    views=[{'source':SOURCES[n],'field':REGIME_FIELDS[n],'reported_label':sources[n]['projection']['label'],
            'availability':inventory[n],'calls_eligible':False} for n in REGIME_FIELDS]
    labels={v['reported_label'] for v in views if v['reported_label']}
    return {'contract':CONTRACT,'schema_version':'3.0','method':METHOD,'generated_at':at,
        'call':None,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'decision':{'verb':'WAIT','meaning':'abstain','portfolio_change':None,'reason':'Independent strategy validation and account-bound risk evidence are not established.'},
        'regime':{'label':'UNQUALIFIED','risk_multiplier':None,'sources':views,
                  'reported_labels_differ':len(labels)>1,'note':'Different labels may use different horizons and definitions. They are not votes.'},
        'top_calls':[],'watchlist':cards,'research_ideas':cards,
        'track_record':{'status':'unqualified_legacy','trail_30d':None,'trail_90d':None,'note':'Old self-graded history is retained privately, not validated by new publication.'},
        'changes':{'entered':[],'dropped':[],'moves':[],'status':'not_compared'},
        'coverage':{'fixed_theme_universe':len(THEMES),'observed_themes':len(cards),'universe_complete':False},
        'source_feeds':inventory,'quality':{'status':'partial','current_packets':sum(r['status']=='current_packet' for r in inventory.values()),
            'expected_packets':len(SOURCES),'scope':'typed_public_input_replay','original_provider_verified':False},
        'evidence_map':{'family_groups':[{'family':k,'subjects':sorted(set(v)),'independent':False} for k,v in sorted(families.items())],
                        'ancestry_complete':False,'independent_evidence_count':None,'eligible_votes':0,
                        'note':'Shared family names reveal possible overlap. They do not prove independent underlying observations.'},
        'pd_settlement_fails':fails(sources['settlement_fails']['projection'],at,inventory['settlement_fails']),
        'portfolio_consequences':{'status':'not_estimated','account_read':False,'portfolio_writes':0,
            'required':['reconciled_positions_cash_and_NAV','instrument_identity_and_exposure','dated_prices_and_FX','approved_cost_and_risk_model','limits_and_sizing_validation'],
            'risk_page':'/portfolio.html','note':'A separate hypothetical payoff calculator accepts explicit assumptions; it is not a recommendation or account estimate.'},
        'qualification_protocol':{'status':'not_registered','required':BLOCKS,'automatic_promotion':False},
        'excluded_private_inputs':['portfolio/sizer-v2.json','data/notes-index.json','wl_fusion.owner_context'],
        'khalid_panels':None,'khalid_divergences':[], 'paid_ai_calls':0,'notifications_sent':0}


def build_brief(compass, at, ref):
    clock(at)
    if compass.get('contract') != CONTRACT or any(compass.get(k) is not False for k in ('calls_eligible','sizing_eligible','execution_eligible')):
        raise ValueError('qualified Alpha research boundary required')
    age=(clock(at)-clock(compass['generated_at'])).total_seconds()/3600
    current=0 <= age <= 4
    lines=['# JustHodl Alpha research brief', '', '**WAIT — abstain.** No portfolio change is authorized by this research packet.', '',
           f"Research snapshot: {compass['generated_at']}. Status: {'current' if current else 'stale; retained context only'}.", '',
           '## Research inventory']
    for idea in compass['research_ideas']:
        direction=idea['direction'] or 'aggregate withheld'
        lines.append(f"- {idea['subject']}: reported {direction}; source packet {idea['source_status']}. Model qualification remains incomplete. Decision `{idea['decision_id']}`.")
    lines += ['', '## Evidence and disagreement',
        f"{compass['quality']['current_packets']} of {compass['quality']['expected_packets']} source packets are current under the transport-age policy. This does not establish fresh observations or predictive skill.",
        'Regime labels remain separate; family overlap is visible and no independent vote count is asserted.', '',
        '## Portfolio consequences', 'Allocation, stops, targets and expected P&L are unavailable. Reconciled holdings, instrument exposures, dated prices, costs, validated strategy evidence and approved limits are required.', '',
        '## Reproduce this brief', f"[Retained Alpha snapshot](/alpha-compass.html?run={ref['manifest_key'].split('/')[-1].removesuffix('.json')})",
        'The retained typed public projections reproduce the research compiler; they are not original-provider evidence.', '', '**WAIT**']
    return {'contract':'alpha-brief-research.v1','generated_at':at,'model':'deterministic-qualified-research',
        'brief_markdown':'\n'.join(lines),'call_verb':'WAIT','call':None,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'decision':{'verb':'WAIT','meaning':'abstain','portfolio_change':None},
        'quality':{'status':'partial' if current else 'stale','source_generated_at':compass['generated_at']},
        'source_replay':ref,'regime':'UNQUALIFIED','macro_stress_score':None,'his_research':None,
        'context_summary':{'n_research_ideas':len(compass['research_ideas']),'n_top_alpha':0,'n_confluence':0,'n_debates':0,'regime':'UNQUALIFIED'},
        'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0}
