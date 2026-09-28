"""Redacted source-availability summary; abstention cannot reopen position fallback."""
import zlib
from context_evidence_store import clock,strict,sha,validate_ref
CONTRACT='prepump-summary-evidence.v1'
PRIVATE='audit-private/20260909-originals/momentum-leaders-research/summary-context/'
HEAD='data/pump-radar-summary.json'
INPUTS={'brief':'data/pump-radar-brief.json','positioning':'data/pump-positioning.json',
        'catalysts':'data/catalysts.json','clusters':'data/catalyst-clusters.json','early':'data/velocity-acceleration.json'}
FLAGS=('calls_eligible','ranking_eligible','sizing_eligible','execution_eligible')
ALIASES=('data/pump-radar-summary.json.gz','data/pump-radar-summary.plain.json')


def build(attempts,sources,generated_at):
    generated=clock(generated_at)
    if generated is None or not isinstance(attempts,dict) or set(attempts)!=set(INPUTS):raise ValueError('Exact summary acquisition and clock required')
    rows=[];objects=0
    for name,key in INPUTS.items():
        a=attempts[name]
        if not isinstance(a,dict) or a.get('source_key')!=key:raise ValueError('Wrong summary input')
        requested=clock(a.get('requested_at'));received=clock(a.get('received_at'))
        if requested is None or received is None or not requested<=received<=generated:raise ValueError('Ordered acquisition clocks required')
        row={'source':name,'source_key':key,'requested_at':a['requested_at'],'received_at':a['received_at'],
             'status':'source_read_unavailable','original_ref':None,'source_generated_at':None,
             'source_generation_age_seconds':None,'source_clock_status':'unknown','observation_freshness':'unqualified'}
        if a.get('status')=='source_read_unavailable':
            if a.get('original_ref') is not None:raise ValueError('Unavailable input cannot claim an original')
        elif a.get('status')=='received':
            ref=validate_ref(a.get('original_ref'),PRIVATE,'sources');raw=sources.get(ref['key'])
            if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Complete summary input identity differs')
            row['original_ref']=dict(ref)
            try:doc=strict(raw,a.get('content_encoding',''))
            except (ValueError,UnicodeError,zlib.error):row['status']='invalid_json'
            else:
                if not isinstance(doc,dict) or not doc:row['status']='unrecognized_shape'
                elif doc.get('status')=='error' or doc.get('error'):row['status']='source_error'
                else:
                    objects+=1;row['status']='received_object_unqualified';stamp=clock(doc.get('generated_at'))
                    if stamp is not None:
                        row['source_generated_at']=stamp.isoformat()
                        if stamp>generated:row['source_clock_status']='future'
                        else:
                            row['source_clock_status']='reported_publication_clock'
                            row['source_generation_age_seconds']=round((generated-stamp).total_seconds(),3)
        else:raise ValueError('Unrecognized source outcome')
        rows.append(row)
    if not objects:raise ValueError('No structured context; preserve previous summary')
    return {'schema_version':'2.0','measurement_contract':CONTRACT,'generated_at':generated_at,'status':'research_only',
        'call':'WAIT','call_semantics':'abstain',**dict.fromkeys(FLAGS,False),'model_requests':0,'notifications_sent':0,
        'conviction':None,'temperature':{'label':None,'score':None},'top_picks':[],
        'executive_summary':'WAIT — research only. Source availability does not validate a grade, trade, position size or recommendation to hold.',
        'sources':rows,'sources_loaded':{r['source']:r['status']=='received_object_unqualified' for r in rows},
        'sources_freshness':{r['source']+'_seconds':r['source_generation_age_seconds'] for r in rows},
        'sources_freshness_semantics':'Reported publication age, not observation freshness; unknown/future clocks are null.',
        'coverage':{'declared_inputs':len(INPUTS),'structured_objects':objects,'independent_roots':None,'eligible_votes':0},
        'catalysts':{'n_a_grade':None,'n_b_grade':None,'n_c_grade':None,'n_d_grade':None,'n_classified':None,'flagged':[]},
        'basket':{'n_positions':None,'n_pump_confirmed':None,'total_exposure':None},'clusters':[],'suggested_additions':[],
        'early':{'trading_date':None,'n_confirmed_today':None,'n_fresh':None,'n_aging':None,'n_actionable':None,'actionable':[]},
        'compatibility_outputs':{'keys':list(ALIASES),'atomic_across_keys':False,
            'require_matching_contract_and_generated_at':True},
        'limitations':['Complete context originals are retained privately.',
            'No fallback from an abstaining brief to current portfolio weights or unvalidated model prose.',
            'Source independence, observation vintages, forecast performance and portfolio consequences remain unqualified.']}
