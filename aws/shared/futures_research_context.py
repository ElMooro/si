"""Dated futures research references cannot recreate retired trading signals."""
from copy import deepcopy
from datetime import datetime,timezone
import re
CURRENT='data/futures-research.json'
FLAGS=('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')


def reference(value):
    return (isinstance(value,dict) and set(value)=={'manifest_key','output_sha256'}
        and isinstance(value.get('manifest_key'),str) and re.fullmatch(r'data/futures-research/runs/[a-f0-9]{64}\.json',value['manifest_key'])
        and isinstance(value.get('output_sha256'),str) and re.fullmatch('[a-f0-9]{64}',value['output_sha256']))


def context(packet):
    p=packet if isinstance(packet,dict) else {};canonical=p.get('canonical') if isinstance(p.get('canonical'),dict) else {}
    permitted=all(p.get(k) is False for k in FLAGS)
    native=permitted and p.get('contract')=='futures-original-research.v1' and reference(p.get('replay'))
    alias=(permitted and p.get('contract')=='futures-original-compatibility.v1' and canonical.get('key')==CURRENT
        and reference(canonical.get('replay')) and p.get('signals')==[] and p.get('product_data')=={} and p.get('identity_ok') is False)
    try:
        generated=datetime.fromisoformat(p['generated_at'].replace('Z','+00:00'))
        acquired=datetime.fromisoformat(p['source_capture_completed_at'].replace('Z','+00:00'))
        clock_ok=generated.tzinfo is not None and acquired.tzinfo is not None and acquired<=generated<=datetime.now(timezone.utc)
    except (KeyError,ValueError,TypeError,AttributeError):clock_ok=False
    available=bool((native or alias) and clock_ok);replay=p.get('replay') if native else canonical.get('replay')
    return {'contract':'futures-original-context.v1','native_reference_available':available,
        'status':'descriptive_native_reference' if available else 'unqualified_legacy_or_unavailable',
        'canonical':{'key':CURRENT,'replay':deepcopy(replay)} if available else None,
        'generated_at':p.get('generated_at') if available else None,
        'source_capture_completed_at':p.get('source_capture_completed_at') if available else None,
        'reference_hashes_independently_checked_by_consumer':False,'freshness_independently_checked_by_consumer':False,
        'signals':[],'product_data':{},'identity':{},'identity_ok':False,
        'call':None,'score':None,'portfolio_action':'WAIT','independent_investment_votes':0,**dict.fromkeys(FLAGS,False),
        'meaning':'Dated prices and common-session spreads are descriptive. They do not establish synchronized quotes, roll profit, supply shortages or investment authority.'}
