"""Descriptive research availability with retained evidence and no model calls.

The twelve existing inputs remain complete acquisition attempts. Their bodies
and compiler bytes are retained under the protected evidence prefix before a
conditional current-head write. No donor grants a forecast or portfolio vote.
Legacy public hourly archives remain untouched; new immutable replay records
use the protected content-addressed store. Native schedules are preserved.
"""
from pathlib import Path
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor
import hashlib,json
import boto3
from botocore.config import Config
import context_evidence_store
from context_evidence_store import ContextStore
import research_status

S3_BUCKET="justhodl-dashboard-live"
OUTPUT_KEY="data/ai-website-synthesis.json"
MODEL="deterministic-research-status-v1"
CONTRACT="website-research-status.v1"
PRIVATE="audit-private/20260909-originals/website-research-status/"
PRIOR_STATE_KEY="data/_alerts/website-synthesis-state.json"  # Retained identity; never read or written.
LEGACY_ARCHIVE_PREFIX="data/archive/ai-website-synthesis/"  # Retained history; never overwritten.
SYSTEM_PROMPT="Model synthesis is disabled. Research availability cannot authorize an investment action."
s3=None

ENGINE_INPUTS = {
    "signal_board":     {"key": "data/signal-board.json",
                           "fields": ["composite_posture", "composite_signal",
                                       "n_live", "n_stale", "categories"]},
    "auction_crisis":   {"key": "data/auction-crisis.json",
                           "fields": ["regime", "composite_score", "interpretation",
                                       "n_recent_auctions_14d", "tail_risk",
                                       "tenor_decomposition", "triggers"]},
    "auction_crisis_ai":{"key": "data/auction-crisis-ai.json",
                           "fields": ["regime", "composite", "ai_commentary"]},
    "macro_frontrun":   {"key": "data/macro-frontrun-sniffer.json",
                           "fields": ["overall_macro_score", "macro_regime",
                                       "headline", "thesis", "pillars",
                                       "loudest_macro_anomaly"]},
    "crisis_brief":     {"key": "data/crisis-brief.json",
                           "fields": ["regime", "score", "headline", "key_risks"]},
    "bonds":            {"key": "data/bond-trace.json",
                           "fields": ["regime", "hy_oas", "hy_oas_velocity",
                                       "ig_oas", "stress_score"]},
    "repo":             {"key": "data/repo.json",
                           "fields": ["sofr", "iorb", "spread", "regime"]},
    "regime":           {"key": "data/regime.json",
                           "fields": ["regime", "score", "drivers"]},
    "correlations":     {"key": "data/correlations.json",
                           "fields": ["regime", "breakdown_count", "headline"]},
    "global_stress":    {"key": "data/global-stress.json",
                           "fields": ["global_stress_index", "global_stress_level"]},
    "sentiment":        {"key": "data/sentiment.json",
                           "fields": ["regime", "score", "putcall", "vix"]},
    "volatility":       {"key": "data/vol-radar.json",
                           "fields": ["regime", "vix", "vvix", "move"]},
}

def call_anthropic(*args,**kwargs):
    raise RuntimeError("All model requests are disabled for this producer")


def send_telegram(*args,**kwargs):
    return False


def maybe_alert_posture_change(*args,**kwargs):
    return {"sent":False,"reason":"unqualified_research_status"}


def extract_json(text):
    return context_evidence_store.strict(text.encode('utf-8'))


def _client():
    return boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=2,read_timeout=3,retries={'max_attempts':0}))


def _metadata(raw,encoding=''):
    meta={'read_status':'malformed','body_sha256':hashlib.sha256(raw).hexdigest(),'body_bytes':len(raw)}
    try:
        packet=context_evidence_store.strict(raw,encoding)
        if not isinstance(packet,dict):return meta,None
        status=packet.get('status')
        reported_error=(isinstance(status,str) and status.lower() in ('error','failed','failure','unavailable')) or bool(packet.get('error'))
        meta.update(read_status='reported_error' if reported_error else 'parsed',reported_generated_at=packet.get('generated_at'),reported_as_of=packet.get('as_of'))
        return meta,packet
    except (ValueError,TypeError,OverflowError,UnicodeError,RecursionError):return meta,None


def _context(name,raw,encoding=''):
    meta,packet=_metadata(raw,encoding)
    out={'_availability':meta,'_age_min':None,'status':'ABSTAIN',
         'current_observation_freshness_verified_by_consumer':False,
         **dict.fromkeys(research_status.FLAGS,False)}
    if name=='signal_board':
        # Preserve the shared decision boundary, never serialize donor scores.
        boundary=__import__('signal_board_authority').context(packet)
        assert boundary['status']=='ABSTAIN' and boundary['calls_eligible'] is False
    elif name=='global_stress':
        boundary=__import__('gsi_authority').decision_view(packet)
        assert boundary['calls_eligible'] is False
    if name=='bonds':out['_error']='Bond TRACE remains unqualified descriptive research.'
    return out


def fetch_engine(engine_name,spec):
    """Compatibility accessor: complete bounded artifact metadata, no scores."""
    if engine_name not in ENGINE_INPUTS or spec.get('key')!=ENGINE_INPUTS[engine_name]['key']:
        raise ValueError('Undeclared research input')
    client=s3 if s3 is not None else _client()
    try:
        obj=client.get_object(Bucket=S3_BUCKET,Key=spec['key']);stream=obj['Body']
        try:raw=stream.read(context_evidence_store.MAX_BYTES+1)
        finally:stream.close()
        if len(raw)>context_evidence_store.MAX_BYTES:
            return engine_name,{'_availability':{'read_status':'oversize'},'_age_min':None,'status':'ABSTAIN','current_observation_freshness_verified_by_consumer':False}
        if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw):raise ValueError('Incomplete artifact')
        return engine_name,_context(engine_name,raw,str(obj.get('ContentEncoding') or '').strip().lower())
    except Exception as exc:
        state='missing' if context_evidence_store.code(exc) in ('404','NoSuchKey') else 'unavailable'
        return engine_name,{'_availability':{'read_status':state},'_age_min':None,'status':'ABSTAIN','current_observation_freshness_verified_by_consumer':False,**dict.fromkeys(research_status.FLAGS,False)}


def fetch_all_engines():
    with ThreadPoolExecutor(max_workers=4) as pool:
        return dict(pool.map(lambda pair:fetch_engine(*pair),ENGINE_INPUTS.items()))


def build_user_prompt(snapshots):
    # Compatibility-only diagnostic, never dispatched to a provider.
    lines=['Research availability only. All investment votes remain withheld.']
    for name in ENGINE_INPUTS:
        snap=snapshots.get(name,{}) if isinstance(snapshots,dict) else {}
        state=(snap.get('_availability') or {}).get('read_status','unavailable') if isinstance(snap,dict) else 'unavailable'
        lines.append(name.upper()+' — UNAVAILABLE' if name=='bonds' else name.upper()+': '+(state if state in research_status.READ_STATES else 'unavailable'))
    return '\n'.join(lines)


def _project(attempts,sources,generated_at):
    snapshots={}
    for name,spec in ENGINE_INPUTS.items():
        attempt=attempts.get(name)
        if not isinstance(attempt,dict) or attempt.get('source_key')!=spec['key']:raise ValueError('Complete fixed input inventory required')
        if attempt.get('status')!='received':
            snapshots[name]={'_availability':{'read_status':'unavailable'}};continue
        ref=context_evidence_store.validate_ref(attempt.get('original_ref'),PRIVATE,'sources')
        raw=sources.get(ref['key'])
        if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or context_evidence_store.sha(raw)!=ref['sha256']:raise ValueError('Exact retained input required')
        snapshots[name]=_context(name,raw,attempt.get('content_encoding',''))
    output=research_status.compile_status(ENGINE_INPUTS,snapshots,generated_at,context_evidence_store.sha(Path(__file__).read_bytes()))
    output['measurement_contract']=CONTRACT
    output['model_requests']=0;output['notifications_sent']=0;output['claude_elapsed_sec']=None
    output['archive_policy']={'legacy_hourly_prefix':LEGACY_ARCHIVE_PREFIX,'legacy_history_untouched':True,'new_records':'Protected content-addressed evidence store; see replay references.'}
    output['replay_scope']='Retained donor artifact bytes, acquisition attempts and exact compiler sources support replay of availability and abstention only. Original provider measurement evidence and forecasting have not been qualified.'
    return output


def _write_error(message,**extras):
    # Never overwrite current research with an error or claim Lambda success.
    raise RuntimeError('Research status acquisition or publication failed')


def lambda_handler(event,context):
    # Caller payload cannot alter data keys, compiler paths, output or policy.
    here=Path(__file__).resolve().parent
    store=ContextStore(_client(),S3_BUCKET,OUTPUT_KEY,{name:spec['key'] for name,spec in ENGINE_INPUTS.items()},PRIVATE,CONTRACT,
        {'lambda_function.py':here/'lambda_function.py','research_status.py':here/'research_status.py',
         'context_evidence_store.py':Path(context_evidence_store.__file__),
         'signal_board_authority.py':Path(__import__('signal_board_authority').__file__),
         'gsi_authority.py':Path(__import__('gsi_authority').__file__)},acquisition_budget_s=60,publication_budget_s=150)
    packet,ref=store.publish(_project)
    return {'statusCode':200,'body':json.dumps({'status':'unqualified','generated_at':packet['generated_at'],
        'contract':CONTRACT,'engines_loaded':packet['engines_loaded'],'engines_total':len(ENGINE_INPUTS),
        'global_posture':'WAIT','call':None,'output_sha256':ref['sha256'],'model_requests':0,'notifications_sent':0})}
