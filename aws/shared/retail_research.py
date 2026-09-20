"""Typed vendor attention context; it cannot supply a ranking or timing vote."""
from datetime import datetime,timezone
import hashlib,json,re
CONTRACT='retail-native-research.v1'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}

def context(packet,at=None):
    absent={'available':False,'reason':'current_verified_attention_sample_unavailable','communities':{},**PERMISSIONS}
    try:
        if not isinstance(packet,dict) or packet.get('contract')!=CONTRACT:return absent
        if any(packet.get(k) is not False for k in PERMISSIONS) or packet.get('market_regime') is not None or packet.get('call') is not None:return absent
        def clock(x):
            d=datetime.fromisoformat(x.replace('Z','+00:00'))
            if d.tzinfo is None:raise ValueError('Aware clock required')
            return d
        at=at or datetime.now(timezone.utc);generated=clock(packet['generated_at']);fresh=packet['freshness'];first=clock(fresh['collection_started_at']);valid=clock(fresh['sample_valid_until'])
        if not first<=generated<=at<valid<=clock(fresh['pipeline_check_due_at']) or not 0<(valid-first).total_seconds()<=7200:return absent
        if packet['quality']['status']!='partial' or packet['quality']['population_complete'] is not False or packet.get('as_of') is not None:return absent
        ref=packet['replay'];raw=json.dumps({k:v for k,v in packet.items() if k!='replay'},sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
        if hashlib.sha256(raw).hexdigest()!=ref['output_sha256'] or not re.fullmatch(r'data/retail-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']):return absent
        if set(packet['communities'])!={'all-stocks','wallstreetbets','stocks','investing'}:return absent
        communities={}
        for key,c in packet['communities'].items():
            if c['population_complete'] is not False or c['source_observed_at'] is not None or c['unit']!='vendor_mention_count':return absent
            if type(c['eligible_symbols']) is not int or c['eligible_symbols']!=len(c['rows']) or c['eligible_symbols']<0:return absent
            communities[key]={k:c[k] for k in ('available','eligible_symbols','reported_symbol_count','sample_mentions','baseline_status_counts','time_window','population_complete','sources')}
        return {**absent,'available':any(c['available'] for c in communities.values()),'reason':'bounded_vendor_attention_context_only',
            'generated_at':packet['generated_at'],'source_valid_until':fresh['sample_valid_until'],'communities':communities,'replay':ref,
            'note':'Unknown vendor cutoff; current ranked sample only. Communities overlap, provider symbols are not issuer identities; no retail flow or sentiment forecast.'}
    except (KeyError,ValueError,TypeError,AttributeError,ArithmeticError):return absent

def qualified_signal(packet):return None
def decision_view(packet):
    return {'research_context':context(packet),'generated_at':packet.get('generated_at') if isinstance(packet,dict) else None,
        'market_regime':None,'market_regime_signal':None,'market_regime_data':{},'top_30_by_mentions':[],
        'ranked':{},'biggest_velocity_surges':[],'stocktwits_trending':[],'recent_alerts':[],'signal_persistence':None,
        'call':None,'portfolio_action':'WAIT',**PERMISSIONS}
def guard(key,packet):return decision_view(packet) if key=='data/retail-sentiment.json' else packet
