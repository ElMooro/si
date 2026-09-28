"""Complete-source availability and duplicate-payload evidence, never conviction."""
import zlib
from context_evidence_store import clock,strict,validate_ref,sha

CONTRACT='apex-context-evidence.v1'
HEAD='data/apex-fusion.json'
PRIVATE='audit-private/20260909-originals/momentum-leaders-research/apex-context/'
INPUTS={'scorecard':'data/signal-scorecard.json','cascade_validation':'data/cascade-validation-log.json',
        'cascade_context':'data/theme-cascade-calibrated.json','positioning':'data/pump-positioning.json',
        'momentum':'data/momentum-leaders.json','squeeze':'data/microcap-float-squeeze.json',
        'flow':'data/options-flow-scanner.json','insider':'data/insider-clusters.json','regime':'data/report.json'}
FLAGS=dict.fromkeys(('calls_eligible','ranking_eligible','sizing_eligible','execution_eligible','forecast_qualified'),False)
KNOWN={'positioning':'positioning-price-observations.v1','momentum':'leader-price-observations.v1',
       'squeeze':'microcap-flow-observations.v1','flow':'options-flow-observations.v1'}


def build(attempts,sources,generated_at):
    generated=clock(generated_at)
    if generated is None or not isinstance(attempts,dict) or set(attempts)!=set(INPUTS) or not isinstance(sources,dict):
        raise ValueError('Exact Apex source graph and aware publication clock required')
    records=[];objects=0;received=0;groups={}
    for name,key in INPUTS.items():
        a=attempts[name]
        if not isinstance(a,dict) or a.get('source_key')!=key:raise ValueError('Wrong declared source')
        requested=clock(a.get('requested_at'));ended=clock(a.get('received_at'))
        if requested is None or ended is None or not requested<=ended<=generated:raise ValueError('Ordered source acquisition clocks required')
        row={'source':name,'source_key':key,'requested_at':a['requested_at'],'received_at':a['received_at'],
             'status':'source_read_unavailable','original_ref':None,'source_generated_at':None,
             'source_generation_age_seconds':None,'source_clock_status':'unknown','recognized_contract':None,
             'observation_freshness':'unqualified',**FLAGS}
        if a.get('status')=='source_read_unavailable':
            if a.get('original_ref') is not None:raise ValueError('Unavailable source cannot claim an original')
        elif a.get('status')=='received':
            ref=validate_ref(a.get('original_ref'),PRIVATE,'sources');raw=sources.get(ref['key'])
            if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Whole original differs')
            row['original_ref']=dict(ref);received+=1;groups.setdefault(ref['sha256'],[]).append(name)
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
                    if name in KNOWN and doc.get('measurement_contract')==KNOWN[name]:row['recognized_contract']=KNOWN[name]
        else:raise ValueError('Unrecognized source acquisition')
        records.append(row)
    if not objects:raise ValueError('No structured source context; do not replace prior publication')
    return {'engine':'apex-fusion','version':'2.0','schema_version':'2.0','measurement_contract':CONTRACT,
        'generated_at':generated_at,'status':'research_only','call':'WAIT','call_semantics':'abstain',**FLAGS,
        'quality':{'status':'research_only','observation_freshness':'unqualified'},
        'weights_used':{},'weight_sources':{},'tier_inversion':{'active':False,'status':'unqualified','alert_tier_hit_pct':None,'n_validated':None},
        'n_universe':None,'n_scored':None,'by_tier':{},'top':[],'n_logged_to_ddb':0,'notifications_sent':0,'model_requests':0,
        'sources':records,'identical_payload_groups':[{'sha256':h,'sources':names} for h,names in groups.items() if len(names)>1],
        'coverage':{'declared_inputs':len(INPUTS),'whole_received':received,'structured_objects':objects,
            'distinct_received_payloads':len(groups),'independent_roots':None,'eligible_votes':0,
            'denominator':'Declared input paths; neither distinct hashes nor path counts establish independence.'},
        'read':f'WAIT / abstain. {objects} of {len(INPUTS)} declared inputs supplied nonempty objects. No fusion score, probability, tier or portfolio action is qualified.',
        'limitations':['The scorecard, validation and market context are preserved privately, not reinterpreted as an investment permission.',
            'Repeated whole-payload hashes identify exact duplicates only; different hashes do not establish independent evidence.',
            'Publication clocks do not establish observation freshness, historical vintages or causal availability.',
            'Hit rates alone cannot justify a trading inversion, and a normalized score is not a calibrated probability.',
            'WAIT means abstain, not a recommendation to hold. Original inputs and prior publications remain private.']}
