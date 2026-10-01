"""Deterministic research availability compiler; no investment authority."""
from datetime import date,datetime,timezone
import hashlib,json,re

FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')
PAGE_INPUTS={'auction-crisis':('auction_crisis','auction_crisis_ai'),'macro-frontrun':('macro_frontrun',),'crisis':('crisis_brief','global_stress'),'bonds':('bonds',),'repo':('repo',),'regime':('regime','signal_board'),'correlation':('correlations',),'sentiment':('sentiment',),'volatility':('volatility',)}
READ_STATES=('parsed','missing','unavailable','malformed','oversize','reported_error')

def _timestamp(value):
    if type(value) is not str:return None
    try:
        stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
        if stamp.tzinfo is None:return None
        return stamp.astimezone(timezone.utc).isoformat()
    except (ValueError,OverflowError):return None

def _observation_claim(value):
    if type(value) is str and re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        try:return date.fromisoformat(value).isoformat()
        except ValueError:return None
    return _timestamp(value)


def compile_status(specs,snapshots,generated_at,producer_sha256):
    """Replay availability decisions from complete typed input metadata only.

    This compiler does not claim original-source measurement replay. A parsed
    donor, self-reported eligibility or a recent storage timestamp never votes.
    """
    if not isinstance(specs,dict) or not specs:raise ValueError('Named input inventory required')
    stamp=_timestamp(generated_at)
    if stamp is None:raise ValueError('Timezone-aware publication timestamp required')
    if type(producer_sha256) is not str or not re.fullmatch('[a-f0-9]{64}',producer_sha256):raise ValueError('Producer source digest required')
    snapshots=snapshots if isinstance(snapshots,dict) else {}
    inputs={}
    for name,spec in specs.items():
        if type(name) is not str or not re.fullmatch('[a-z_]+',name) or not isinstance(spec,dict) or type(spec.get('key')) is not str:
            raise ValueError('Fixed reviewed input identity required')
        source=snapshots.get(name);source=source if isinstance(source,dict) else {}
        meta=source.get('_availability');meta=meta if isinstance(meta,dict) else {}
        state=meta.get('read_status') if meta.get('read_status') in READ_STATES else 'unavailable'
        digest=meta.get('body_sha256');size=meta.get('body_bytes')
        complete=(type(digest) is str and bool(re.fullmatch('[a-f0-9]{64}',digest)) and type(size) is int and size>=0)
        if state=='parsed' and (not complete or size==0):state='unavailable'
        inputs[name]={'artifact':spec['key'],'read_status':state,'body_sha256':digest if complete else None,'body_bytes':size if complete else None,
                      'reported_generated_at':_timestamp(meta.get('reported_generated_at')),'reported_as_of':_observation_claim(meta.get('reported_as_of')),
                      'storage_last_modified':_timestamp(meta.get('storage_last_modified')),'observation_freshness_verified':False,
                      'original_source_evidence_verified':False,'decision_eligible':False}
    loaded=sum(row['read_status']=='parsed' for row in inputs.values())
    synthesis={'global_posture':'WAIT','headline':f'Research status: {loaded} of {len(inputs)} input artifacts parsed; no investment vote is qualified.',
               'thesis':'This deterministic status report records artifact availability. Parsing and storage publication times do not establish current source observations, independent evidence, a forecast edge or a suitable portfolio action.',
               'key_drivers':[],'key_dissonances':[],'decisive_call':'WAIT — abstain from a new portfolio recommendation until the underlying evidence and portfolio constraints are qualified.',
               'watch_list':['Review original observation dates and source evidence.','Validate independent inputs and outcomes before any position-sizing decision.'],
               'per_page_focus':{page:'Descriptive research only; '+str(sum(inputs.get(name,{}).get('read_status')=='parsed' for name in names))+' of '+str(len(names))+' associated input artifacts parsed. Source evidence and decision eligibility remain unverified.' for page,names in PAGE_INPUTS.items()}}
    metadata_bytes=json.dumps(inputs,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    return {'schema_version':'2.0','contract':'website-research-status.v1','status':'unqualified','generated_at':stamp,
            'model':'deterministic-research-status-v1','producer_source_sha256':producer_sha256,
            'engines_loaded':loaded,'engines_total':len(inputs),'snapshot_age_min':dict.fromkeys(inputs,None),
            'input_status':inputs,'input_status_sha256':hashlib.sha256(metadata_bytes).hexdigest(),'call':None,**dict.fromkeys(FLAGS,False),
            'decision':{'action':'WAIT','abstain':True,'eligible_votes':0,'portfolio_effect':None},'synthesis':synthesis,
            'replay_scope':'Availability metadata and deterministic abstention only; original measurement evidence has not been replayed.',
            'alert_info':{'sent':False,'reason':'unqualified_research_status'}}
