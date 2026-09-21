"""Pure desk assembly from retained option populations, not dealer inventory."""
from collections import Counter,defaultdict
from datetime import timedelta
import json
import option_population_model as model
import option_flow_research as source_model
import option_research_rows as codec

CONTRACT='option-population-desk.v1'
CURRENT='data/option-population-research.json'
LEGACY='data/dealer-gex.json'
HISTORY='data/dealer-gex-history.json'
PERMISSIONS=model.PERMISSIONS
PREDECESSORS=(CURRENT,LEGACY,HISTORY)


def is_legacy(packet):
    return (isinstance(packet,dict) and packet.get('version')=='1.3.0'
        and isinstance(packet.get('underlyings'),dict) and isinstance(packet.get('market_composite'),dict)
        and isinstance(packet.get('calculation_config'),dict) and packet['calculation_config'].get('n_underlyings')==10)


def memberships(output,read):
    """Direct original page/row drill-down for every contributing aggregate."""
    chain=source_model.checked(output['source_chain'],read,'chains');groups=defaultdict(list)
    for part in chain['record_blocks']:
        rows=codec.unpack(source_model.checked(part['artifact'],read,'rows'))
        for row in rows:
            if not row['identity_eligible']:continue
            strike=row['metrics']['strike']['value'];strike=strike.rstrip('0').rstrip('.') if '.' in strike else strike
            values=model.row_values(row)
            groups[row['expiration_date'],strike].append({'contract_id':row['contract_id'],'contract_type':row['contract_type'],
                'source_page':row['evidence']['page'],'row_index':row['evidence']['row_index'],
                'eligible_fields':[field for field,value in values.items() if value is not None]})
    result=[]
    for group in output['by_expiry_strike']:
        key=(group['expiration_date'],group['strike_usd_per_share']);rows=groups.pop(key,[])
        if len(rows)!=group['counts']['identity_eligible_rows']:raise ValueError('Source membership population differs')
        included=Counter((row['contract_type'],field) for row in rows for field in row['eligible_fields'])
        if any(included[kind,field]!=cell['included_rows'] for kind,fields in group['sides'].items() for field,cell in fields.items()):
            raise ValueError('Source membership contribution coverage differs')
        result.append({'expiration_date':key[0],'strike_usd_per_share':key[1],'source_rows':rows})
    if groups:raise ValueError('Source membership has unaccounted strike groups')
    return {'contract':'option-population-membership.v1','underlying':output['underlying'],
        'source_run':output['source_run'],'source_chain':output['source_chain'],
        'record_blocks':output['record_blocks'],'groups':result}


def build(inputs,read,emit):
    if inputs.get('contract')!='option-population-inputs.v1':raise ValueError('Typed population inputs required')
    compiled=source_model.clock(inputs['compiled_at'])
    packet=model.verified_packet(source_model.protected(inputs['source_publication'],read),read)
    acquired=source_model.clock(packet['generated_at'])
    if acquired>compiled:raise ValueError('Source capture is later than computation')
    predecessors=inputs['predecessors']
    if set(predecessors)!=set(PREDECESSORS):raise ValueError('Whole predecessor inventory required')
    for key,ref in predecessors.items():
        if ref is None:
            if key!=CURRENT:raise ValueError('Whole legacy packet/history required')
        else:
            prior=json.loads(source_model.protected(ref,read))
            if not isinstance(prior,dict):raise ValueError('Structured predecessor required')
            if key==LEGACY and not is_legacy(prior):
                if (prior.get('contract')!='dealer-gex-compatibility.v1' or any(prior.get(k) is not False for k in PERMISSIONS)
                        or prior.get('canonical',{}).get('key')!=CURRENT):raise ValueError('Reviewed legacy or native compatibility packet required')
    underlyings={};counts=Counter();capture=Counter()
    for symbol in model.UNDERLYINGS:
        out=model.chain(packet,symbol,read);ref=model.deliver(out,emit)
        member_raw=source_model.encoded(memberships(out,read));member_ref=model.reference(member_raw,'groups');emit(member_ref['key'],member_raw)
        underlyings[symbol]={'population':ref,'source_chain':out['source_chain'],
            'source_membership':member_ref,
            'capture_status':out['capture_status'],'source_coverage':out['source_coverage'],
            'totals':out['totals'],'expiry_strike_groups':len(out['by_expiry_strike'])}
        counts.update(out['totals']['counts']);capture[out['capture_status']]+=1
    due=acquired+timedelta(hours=2)
    available=counts['identity_eligible_rows']>0
    quality={'status':'unavailable' if not available else 'stale_capture' if compiled>due else 'descriptive',
        'source_capture_completed_at':packet['generated_at'],'acquisition_review_due_at':due.isoformat(),
        'source_age_at_computation_seconds':round((compiled-acquired).total_seconds(),3),
        'capture_status_counts':dict(capture),'counts':dict(counts),
        'independent_oi_greek_clocks_verified':False,'dealer_ownership_observed':False,
        'exchange_chain_completeness_verified':False,
        'rule':'Compilation does not refresh source clocks. Two-hour acquisition review is distinct from unknown OI/Greek observation dates.'}
    return {'contract':CONTRACT,'generated_at':inputs['compiled_at'],
        'source_capture_completed_at':packet['generated_at'],'source_run':packet['replay'],
        'source_packet_key':source_model.CURRENT,'universe':list(model.UNDERLYINGS),
        'universe_scope':'Ten declared continuity underlyings; not the whole options market.',
        'underlyings':underlyings,'quality':quality,'predecessors':predecessors,
        'evidence_roots':[{'key':source_model.CURRENT,'replay':packet['replay']}],
        'observed_dealer_inventory':None,'zero_gamma_flip':None,'dealer_hedging_flow':None,
        'call':None,'score':None,'portfolio_action':'WAIT','independent_investment_votes':0,**PERMISSIONS,
        'interpretation':'Call and put populations stay separate. Ownership signs, live Greek clocks and predictive performance are not established.',
        'references':['https://massive.com/docs/rest/options/snapshots/option-chain-snapshot',
            'https://www.optionseducation.org/advancedconcepts/gamma']}


def compatibility(packet):
    if packet.get('contract')!=CONTRACT or not packet.get('replay'):raise ValueError('Retained native population required')
    return {'contract':'dealer-gex-compatibility.v1','engine':'justhodl-dealer-gex',
        'generated_at':packet['generated_at'],'source_capture_completed_at':packet['source_capture_completed_at'],
        'canonical':{'key':CURRENT,'replay':packet['replay']},'quality':packet['quality'],
        'status':'superseded_by_captured_population_research','underlyings':{},'market_composite':{},
        'squeeze_candidates':[],'top_squeezes':[],'n_underlyings_modeled':0,
        'legacy_history':{'key':HISTORY,'meaning':'Retained predecessor model output; unqualified dealer assumptions, not an observed exposure history.'},
        'retained_predecessor':packet['predecessors'][LEGACY],
        'evidence_note':'Legacy dealer regimes, flip levels and directional rankings are retired. Empty legacy lists are not zero open interest or zero exposure. Use the canonical captured populations and explicit assumptions.',
        'call':None,'score':None,'portfolio_action':'WAIT','independent_investment_votes':0,**PERMISSIONS}
