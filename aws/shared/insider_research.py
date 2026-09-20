"""Insider filing samples are descriptive context, never a smart-money vote."""
from datetime import datetime,timezone
import hashlib,json,re
CONTRACT='insider-native-research.v1'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}

def context(packet,at=None):
    absent={'available':False,'reason':'current_verified_insider_sample_unavailable','windows':{},'filing_windows':{},**PERMISSIONS}
    try:
        if not isinstance(packet,dict) or packet.get('contract')!=CONTRACT:return absent
        if any(packet.get(k) is not False for k in PERMISSIONS) or packet.get('regime') is not None or packet.get('call') is not None:return absent
        def clock(x):
            d=datetime.fromisoformat(x.replace('Z','+00:00'))
            if d.tzinfo is None:raise ValueError('Aware source clock required')
            return d
        at=at or datetime.now(timezone.utc);generated=clock(packet['generated_at']);due=clock(packet['freshness']['pipeline_check_due_at'])
        if not generated<=at<due or not at<clock(packet['freshness']['sample_valid_until']) or (due-generated).total_seconds()>80*3600:return absent
        if packet['quality']['status']!='partial' or packet['coverage']['population_complete'] is not False:return absent
        ref=packet['replay'];raw=json.dumps({k:v for k,v in packet.items() if k!='replay'},sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
        if hashlib.sha256(raw).hexdigest()!=ref['output_sha256'] or not re.fullmatch(r'data/insider-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']):return absent
        if set(packet['windows'])!= {'last_7d','last_30d','last_90d'} or set(packet['filing_windows'])!=set(packet['windows']):return absent
        for group in (packet['windows'],packet['filing_windows']):
            for w in group.values():
                if any(type(w.get(k)) is not int or w[k]<0 for k in ('buy_count','sell_count','representation_count')):return absent
                if w['buy_count']+w['sell_count']!=w['representation_count'] or w.get('population_complete') is not False:return absent
        return {**absent,'available':True,'reason':'bounded_vendor_filing_sample_only','generated_at':packet['generated_at'],
            'source_valid_until':packet['freshness']['sample_valid_until'],'as_of':packet['as_of'],
            'windows':packet['windows'],'filing_windows':packet['filing_windows'],'coverage':packet['coverage'],
            'quality':packet['quality'],'replay':ref,'note':'No SEC population, plan, currency, economic-transaction identity or investment qualification.'}
    except (KeyError,ValueError,TypeError,ArithmeticError):return absent

def qualified_signal(packet):return None
def decision_view(packet):
    return {'research_context':context(packet),'regime':None,'signal':None,'headline_ratio_30d_dollar':None,
        'windows':{},'notable_cluster_buys':[],'call':None,'portfolio_action':'WAIT',**PERMISSIONS}
def guard(key,packet):return decision_view(packet) if key=='data/insider-aggregate.json' else packet
