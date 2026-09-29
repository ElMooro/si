"""Unpublished Calls input-lineage candidate using complete retained originals.

No transport, AWS client, current-key read, publication or investment authority.
The caller provides one complete packet and an immutable-artifact byte reader.
Archived compiler text is compared with reviewed source, never executed.
"""
from copy import deepcopy
from datetime import date
import hashlib,re,sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-liquidity-flow/source'),str(ROOT/'aws/shared'),str(ROOT/'scripts')]
import liquidity_flow_store as store
import verify_liquidity_arithmetic as arithmetic_check

CONTRACT='calls-liquidity-lineage-candidate.v1'
SID_ORDER=('WALCL','WTREGEN','RRPONTSYD')
SIGNS={'WALCL':1,'WTREGEN':-1,'RRPONTSYD':-1}
FAMILIES={'WALCL':'fed_h41','WTREGEN':'fed_h41','RRPONTSYD':'fed_rrp_operations'}
MAX_ARTIFACTS=128
MAX_TOTAL=256*1024*1024


def encoded(value):return store.model.encoded(value)
def digest(value):return hashlib.sha256(encoded(value)).hexdigest()


class ImmutableReader:
    """Cache whole verified bytes; reject current/private keys before transport."""
    def __init__(self,read):self.read=read;self.cache={};self.bytes=0
    def __call__(self,key):
        if not isinstance(key,str) or not store.allowed(key) or key in (store.SOURCE,store.SETTLEMENT,store.model.CURRENT):
            raise ValueError('Only reviewed immutable original/compiled artifacts are readable')
        if key in self.cache:return self.cache[key]
        if len(self.cache)>=MAX_ARTIFACTS:raise ValueError('Complete artifact count exceeds bound')
        raw=self.read(key)
        if not isinstance(raw,bytes) or not 0<len(raw)<=store.MAX:raise ValueError('Whole bounded artifact bytes required')
        if self.bytes+len(raw)>MAX_TOTAL:raise ValueError('Complete retained replay exceeds byte bound')
        match=re.search(r'/([a-f0-9]{64})\.(?:json|py|bin\.gz)$',key)
        if not match or hashlib.sha256(raw).hexdigest()!=match[1]:raise ValueError('Immutable artifact content hash differs')
        self.bytes+=len(raw);self.cache[key]=raw;return raw


def inspect(raw_packet,read,as_of):
    if not isinstance(raw_packet,bytes) or not 0<len(raw_packet)<=store.MAX:raise ValueError('Complete bounded packet bytes required')
    now=store.model.clock(as_of)
    packet=store.strict(raw_packet)
    if not isinstance(packet,dict) or packet.get('contract')!=store.model.CONTRACT:raise ValueError('Reviewed native liquidity packet required')
    retained=ImmutableReader(read)
    output=store.replay(packet['replay'],retained)
    if not store.same_json(output,{k:v for k,v in packet.items() if k!='replay'}):raise ValueError('Whole supplied packet differs from retained replay')
    generated=store.model.clock(output['generated_at']);source_at=store.model.clock(output['source_generated_at'])
    if generated>now or source_at>now:raise ValueError('Future research cutoff or source packet')

    # Reuse complete originals already authenticated by replay. The separate
    # Fraction verifier checks every original row, all 180 dates and four windows.
    manifest=store.strict(retained(packet['replay']['manifest_key']))
    inputs=store.checked(manifest['input'],'inputs',retained)
    macro=store.checked(inputs['macro'],'snapshots',retained)
    originals=store.canonical.restore(macro,SID_ORDER,retained)
    candidate=store.arithmetic.build(macro,originals,output['generated_at'])
    proof=arithmetic_check.verify(candidate,macro,originals)
    for field in ('series','comparisons','calendar_history_180d','last_reconstructed_snapshot'):
        if not store.same_json(output[field],candidate[field]):raise ValueError('Native and independent comparison inputs differ: '+field)

    sources={};observations={};clock_issues=[]
    for sid in SID_ORDER:
        series=output['series'][sid]
        acquired=store.model.clock(series['acquired_at']);age=(now-acquired).total_seconds()
        if not 0<=age<=26*3600:clock_issues.append(sid+':acquisition_age')
        refs={}
        for kind in ('definition','observations'):
            ref=series['evidence'][kind]
            refs[kind]={k:ref[k] for k in ('key','sha256','bytes','provider','first_received_at')}
        sources[sid]={'series_id':sid,'unit':series['native_unit'],'frequency':series['frequency'],
            'measurement_basis':series['measurement_basis'],'to_usd_bn':series['to_usd_bn'],
            'coefficient':SIGNS[sid],'source_family':FAMILIES[sid],'acquired_at':acquired.isoformat(),
            'acquisition_age_seconds':age,'max_acquisition_age_seconds':26*3600,
            'max_observation_age_days':series['max_observation_age_days'],'originals':refs,
            'original_row_count':len(series['history']),'original_publication_time_verified':False}

    def leg(sid,row):
        selected=row['selected'];identity=None
        if selected is not None:
            index=selected['original_row']
            if type(index)is not int or index<0:raise ValueError('Exact original row coordinate required')
            original=originals[sid]['observations']['observations'][index]
            if original['date']!=selected['observation_date'] or original.get('value')!=selected['native_value']:
                raise ValueError('Selected original row differs')
            observation={'series_id':sid,'response_sha256':sources[sid]['originals']['observations']['sha256'],
                'original_row':index,'observation_date':selected['observation_date'],
                'reported_native_value':selected['native_value'],'native_unit':sources[sid]['unit'],
                'frequency':sources[sid]['frequency']}
            identity='fred-observation-'+digest(observation)
            previous=observations.setdefault(identity,observation)
            if previous!=observation:raise ValueError('Observation identity conflict')
        return {'series_id':sid,'observation_id':identity,'valuation_date':row['valuation_date'],
            'carry_days':row['carry_days'],'status':row['status'],'normalized_value':deepcopy(row['value']),
            'coefficient':SIGNS[sid]}

    def sample(row):
        value={'valuation_date':row['valuation_date'],'components':{sid:leg(sid,row['legs'][sid]) for sid in SID_ORDER},
               'net':deepcopy(row['net']),'status':row['status']}
        return {'calculation_id':'liquidity-calculation-'+digest(value),**value}

    current=sample(output['last_reconstructed_snapshot'])
    history=[sample(row) for row in output['calendar_history_180d']]
    comparisons={}
    for horizon,row in output['comparisons'].items():
        comparisons[horizon]={'current_valuation_date':row['current_valuation_date'],
            'baseline_valuation_date':row['baseline_valuation_date'],'change':deepcopy(row['change']),
            'components':{sid:{'current':leg(sid,item['current']),'baseline':leg(sid,item['baseline']),
                'reported_level_change':deepcopy(item['reported_level_change']),
                'signed_formula_contribution':deepcopy(item['signed_formula_contribution']),
                'same_observation_carried':item['same_observation_carried']} for sid,item in row['legs'].items()},
            'causal_flow_estimate':False,'historical_information_availability_verified':False}
    if (now-generated).total_seconds()>26*3600:clock_issues.append('native_packet:publication_age')
    if (now-source_at).total_seconds()>26*3600:clock_issues.append('macro_packet:publication_age')
    for sid,row in current['components'].items():
        observed=observations.get(row['observation_id'],{}).get('observation_date')
        age=(now.date()-date.fromisoformat(observed)).days if observed else None
        if age is None or not 0<=age<=sources[sid]['max_observation_age_days']:clock_issues.append(sid+':observation_age')
    if output['quality']['status']!='fresh' or candidate['current'] is None:clock_issues.append('native_current:unavailable')
    # The whole replay bound these public scalar aliases to the exact selected
    # native calculation. Expose both; never substitute history for current.
    usable=not clock_issues
    return {'contract':CONTRACT,'as_of':now.isoformat(),'packet_sha256':hashlib.sha256(raw_packet).hexdigest(),
        'source_generated_at':output['generated_at'],'macro_generated_at':output['source_generated_at'],
        'source_replay':deepcopy(packet['replay']),'formula':'WALCL - WTREGEN - RRPONTSYD','unit':'usd_bn',
        'reported_headline':output['current']['net_liquidity_b'],
        'current_research_value':output['current']['net_liquidity_b'] if usable else None,
        'current_use':{'eligible':usable,'issues':clock_issues,'scope':'descriptive original-source calculation only'},
        'sources':sources,'observations':observations,'latest_reconstructed':current,
        'calendar_history_180d':history,'comparisons':comparisons,
        'source_families':[{'family':family,'series':[sid for sid in SID_ORDER if FAMILIES[sid]==family]}
                           for family in sorted(set(FAMILIES.values()))],
        'coverage':{'original_rows':sum(s['original_row_count'] for s in sources.values()),'calendar_dates':len(history),
            'comparison_windows':len(comparisons),'distinct_selected_observations':len(observations),
            'retained_artifacts':len(retained.cache),'retained_uncompressed_bytes':retained.bytes},
        'retained_artifact_inventory':[{'key':key,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
                                       for key,raw in sorted(retained.cache.items())],
        'independent_arithmetic':proof,'independent_evidence_count':None,
        'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'publication_eligible':False,
        'scope':'Complete original-source replay plus dated arithmetic ancestry. Repeated carries reference the same observation; source-family counts do not establish statistical independence. Current-vintage history is not point-in-time history or a return forecast.'}
