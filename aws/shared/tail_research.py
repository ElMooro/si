"""Option snapshot context never supplies unvalidated crash or hedge-timing authority."""
from datetime import datetime,timedelta,timezone
import hashlib,json,re
CONTRACT='tail-native-research.v1'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}

def context(packet,at=None):
    absent={'available':False,'reason':'verified_option_snapshot_context_unavailable','samples':{},**PERMISSIONS}
    try:
        if not isinstance(packet,dict) or packet.get('contract')!=CONTRACT:return absent
        if any(packet.get(k) is not False for k in PERMISSIONS):return absent
        if any(packet.get(k) is not None for k in ('call','score','regime','system_tail_gauge','tail_valuation','tail_regime')):return absent
        def clock(s):
            d=datetime.fromisoformat(s.replace('Z','+00:00'))
            if d.tzinfo is None:raise ValueError('Aware clock required')
            return d
        at=at or datetime.now(timezone.utc);start=clock(packet['freshness']['collection_started_at']);generated=clock(packet['generated_at']);due=clock(packet['freshness']['sample_valid_until'])
        if not start<=generated<=at<due or due!=start+timedelta(hours=26) or not 0<=(generated-start).total_seconds()<=150:return absent
        ref=packet['replay'];body={k:v for k,v in packet.items() if k!='replay'}
        if not re.fullmatch(r'data/tail-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']):return absent
        if hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()!=ref['output_sha256']:return absent
        if len(packet['indices'])!=3 or {r['ticker'] for r in packet['indices']}!={'SPY','QQQ','IWM'}:return absent
        samples={}
        for row in packet['indices']:
            if any(row.get(k) is not None for k in ('tail_stress','p_drop_10','p_drop_20')) or any(row.get(k) is not False for k in PERMISSIONS):return absent
            sample=row['sample'];count=sample['eligible_identity_rows']
            if type(count) is not int or count<0 or sample['capture_is_atomic'] is not False or sample['exchange_chain_completeness_verified'] is not False:return absent
            samples[row['ticker']]={k:sample[k] for k in ('returned_rows','eligible_identity_rows','pages_received','pagination_complete','stop_reason','with_qualified_quote','with_vendor_iv')}
        return {**absent,'available':any(x['eligible_identity_rows'] for x in samples.values()),'reason':'captured_vendor_model_context_only',
            'samples':samples,'generated_at':packet['generated_at'],'source_valid_until':due.isoformat(),'replay':ref,
            'note':'No independent IV clock, qualified density, physical crash probability, hedge price or investment vote.'}
    except (KeyError,ValueError,TypeError,AttributeError,ArithmeticError):return absent

def qualified_score(packet):return None
def decision_view(packet):
    return {'research_context':context(packet),'generated_at':packet.get('generated_at') if isinstance(packet,dict) else None,
        'indices':[],'system_tail_gauge':None,'tail_regime':None,'tail_valuation':None,'score':None,'regime':None,'call':None,'portfolio_action':'WAIT',**PERMISSIONS}
def guard(key,packet):return decision_view(packet) if key=='data/tail-risk.json' else packet
