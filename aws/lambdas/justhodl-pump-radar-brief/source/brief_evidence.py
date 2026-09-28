"""Deterministic research brief from complete, privately retained source packets.

Only allowlisted metadata is published. Source clocks are not observation clocks;
availability is not independence, predictive skill, market temperature or sizing.
"""
import zlib
from context_evidence_store import clock,strict,validate_ref,sha

CONTRACT='pump-brief-evidence.v1'
PRIVATE='audit-private/20260909-originals/momentum-leaders-research/pump-brief-context/'
HEAD='data/pump-radar-brief.json'
INPUTS={
    'convergence':'data/convergence-radar.json',
    'positioning':'data/pump-positioning.json',
    'mechanics':'data/pump-mechanics.json',
    'analytics':'data/portfolio-analytics.json',
    'pairs':'data/pair-trades.json',
    'nlp':'data/pump-earnings-nlp.json',
    'research':'data/ticker-research-bundle.json',
    'synthesis':'data/ai-website-synthesis.json',
    'earnings_cal':'data/earnings-tracker.json',
    'momentum':'data/momentum-leaders.json',
    'catalysts':'data/catalysts.json',
    'clusters':'data/catalyst-clusters.json',
    'early':'data/velocity-acceleration.json',
}
FLAGS=('calls_eligible','ranking_eligible','sizing_eligible','execution_eligible')
KNOWN={'momentum':'leader-price-observations.v1','catalysts':'catalyst-context-research.v1',
       'clusters':'catalyst-cluster-abstention.v1'}
LIMITATIONS=[
    'WAIT means abstain from a new recommendation; it does not recommend holding an existing position.',
    'Source availability and publication age do not establish observation freshness or independent evidence.',
    'Shared upstream roots, historical vintages, predictive performance and portfolio consequences remain unqualified.',
    'No market temperature, conviction grade, position size, pair alpha or new/removed signal is inferred.',
]


def build(attempts,sources,generated_at):
    generated=clock(generated_at)
    if generated is None or not isinstance(attempts,dict) or set(attempts)!=set(INPUTS) or not isinstance(sources,dict):
        raise ValueError('Exact declared brief inputs and publication clock required')
    records=[];objects=0;received=0
    for name,key in INPUTS.items():
        a=attempts[name]
        if not isinstance(a,dict) or a.get('source_key')!=key:raise ValueError('Wrong declared brief source')
        requested=clock(a.get('requested_at'));ended=clock(a.get('received_at'))
        if requested is None or ended is None or not requested<=ended<=generated:raise ValueError('Ordered source acquisition clocks required')
        row={'source':name,'source_key':key,'requested_at':a['requested_at'],'received_at':a['received_at'],
             'status':'source_read_unavailable','original_ref':None,'source_generated_at':None,
             'source_generation_age_seconds':None,'source_clock_status':'unknown',
             'recognized_contract':None,'observation_freshness':'unqualified',**dict.fromkeys(FLAGS,False)}
        if a.get('status')=='source_read_unavailable':
            if a.get('original_ref') is not None:raise ValueError('Unavailable read cannot claim an original')
        elif a.get('status')=='received':
            ref=validate_ref(a.get('original_ref'),PRIVATE,'sources');raw=sources.get(ref['key'])
            if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Whole original differs from declared identity')
            row['original_ref']=dict(ref);received+=1
            try:doc=strict(raw,a.get('content_encoding',''))
            except (ValueError,UnicodeError,zlib.error):row['status']='invalid_json'
            else:
                if not isinstance(doc,dict) or not doc:row['status']='unrecognized_shape'
                elif doc.get('status')=='error' or doc.get('error'):row['status']='source_error'
                else:
                    objects+=1;row['status']='received_object_unqualified'
                    stamp=clock(doc.get('generated_at'))
                    if stamp is not None:
                        row['source_generated_at']=stamp.isoformat()
                        if stamp>generated:row['source_clock_status']='future'
                        else:
                            row['source_clock_status']='reported_publication_clock'
                            row['source_generation_age_seconds']=round((generated-stamp).total_seconds(),3)
                    if name in KNOWN and doc.get('measurement_contract')==KNOWN[name]:row['recognized_contract']=KNOWN[name]
        else:raise ValueError('Unrecognized acquisition outcome')
        records.append(row)
    if not objects:raise ValueError('No structured source context; preserve previous publication')
    summary=(f'WAIT — evidence review only. {objects} of {len(INPUTS)} declared inputs supplied nonempty JSON objects; '
             f'{received} complete source responses were retained privately. These counts describe availability, not independent votes or market strength. '
             'The available inputs do not establish a validated action, conviction grade or position size.')
    markdown=summary+'\n\n'+'\n'.join('- '+s for s in LIMITATIONS)+'\n\n**WAIT**'
    return {'schema_version':'2.0','measurement_contract':CONTRACT,'generated_at':generated_at,
        'status':'research_only','quality':{'status':'research_only','observation_freshness':'unqualified'},
        'model':None,'model_requests':0,'notifications_sent':0,'call':'WAIT','call_semantics':'abstain',
        **dict.fromkeys(FLAGS,False),'executive_summary':summary,'brief_markdown':markdown,
        'conviction_grade':None,'macro_frame':None,'market_temperature':{'score':None,'rank':None,'components':{},'label_from_ai':None},
        'top_3_long_ideas':[],'top_2_pair_trades':[],'what_to_watch_today':[],
        'risk_warnings':list(LIMITATIONS),'whats_changed_narrative':None,
        'whats_changed_data':{'status':'unavailable_incomparable_populations','new_signals':[],'removed_signals':[]},
        'source_versions':{r['source']:r['source_generated_at'] for r in records},
        'sources':records,'coverage':{'declared_inputs':len(INPUTS),'whole_received':received,'structured_objects':objects,
            'independent_roots':None,'eligible_votes':0,'denominator':'Declared input paths, not independent evidence or market coverage'},
        'disclaimer':'Deterministic research evidence; no model interpretation, market forecast or portfolio instruction. Complete context originals stay private.'}
