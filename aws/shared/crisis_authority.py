"""Crisis research must earn decision authority through an independently reviewed model."""
import re


def research_error(packet):
    """Schema validation only; it never qualifies a forecasting or sizing policy."""
    if not isinstance(packet,dict) or packet.get('contract')!='crisis-research.v1':return 'native Crisis research contract required'
    if any(packet.get(k) is not False for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_eligible')):return 'Crisis research cannot grant decision authority'
    if any(packet.get(k) is not None for k in ('master_crisis_score','composite_score','score','defcon_level','playbook')):return 'unqualified Crisis scores must be null'
    if not isinstance(packet.get('measurements'),dict) or not packet['measurements']:return 'native Crisis measurements required'
    if not isinstance(packet.get('quality'),dict) or packet['quality'].get('status') not in ('fresh','degraded','unavailable'):return 'native Crisis quality required'
    ref=packet.get('replay')
    if not isinstance(ref,dict) or not re.fullmatch(r'data/crisis-research/runs/[a-f0-9]{64}\.json',str(ref.get('manifest_key',''))) or not re.fullmatch(r'[a-f0-9]{64}',str(ref.get('output_sha256',''))):return 'Crisis replay reference required'
    return None


def qualified_score(packet):
    # No qualified crisis forecasting/sizing model is registered. Packet flags,
    # legacy DEFCON levels and producer-generated confidence cannot grant one.
    return None


def decision_view(packet):
    source=packet if isinstance(packet,dict) else {}
    return {'master_crisis_score':None,'composite_score':None,'score':None,
        'defcon_level':None,'defcon_name':None,'trend':None,'playbook':None,
        'primary_drivers':[],'components':[],'components_available':0,
        'contract':'crisis-decision-view.v1','generated_at':source.get('generated_at'),
        'quality':{'status':'research_only'},'qualification':'research_only','calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'reason':'Crisis observations have no independently qualified forecasting, allocation or alert policy.'}


def guard(key,packet):
    return decision_view(packet) if key in ('data/crisis-composite.json','data/defcon.json') else packet


def filter_history(rows):
    return [{**row,'scores':{k:v for k,v in (row.get('scores') or {}).items()
        if k not in ('crisis','crisis_composite','defcon')}} for row in rows if isinstance(row,dict)]
