"""FX quote research is descriptive; legacy regimes cannot supply authority."""
from copy import deepcopy
from datetime import datetime,timezone
import re
CURRENT='data/fx-quote-research.json'
LEGACY='data/polygon-fx-regime.json'
FLAGS=('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')


def reference(value):
    return (isinstance(value,dict) and set(value)=={'manifest_key','output_sha256'}
        and isinstance(value.get('manifest_key'),str) and re.fullmatch(r'data/fx-quote-research/runs/[a-f0-9]{64}\.json',value['manifest_key'])
        and isinstance(value.get('output_sha256'),str) and re.fullmatch('[a-f0-9]{64}',value['output_sha256']))
def context(packet):
    p=packet if isinstance(packet,dict) else {};canonical=p.get('canonical') if isinstance(p.get('canonical'),dict) else {}
    permitted=all(p.get(k) is False for k in FLAGS)
    native=permitted and p.get('contract')=='fx-original-quote-research.v1' and reference(p.get('replay'))
    alias=(permitted and p.get('contract')=='fx-original-compatibility.v1' and canonical.get('key')==CURRENT
        and reference(canonical.get('replay')) and p.get('regime_signals')==[] and p.get('pair_data')=={} and p.get('regime_metrics')=={})
    try:
        parsed=datetime.fromisoformat(p.get('generated_at').replace('Z','+00:00'))
        clock_ok=parsed.tzinfo is not None and parsed<=datetime.now(timezone.utc)
    except (ValueError,TypeError,AttributeError):clock_ok=False
    available=bool((native or alias) and clock_ok);replay=p.get('replay') if native else canonical.get('replay')
    return {'contract':'fx-original-context.v1','native_reference_available':available,
        'status':'descriptive_native_reference' if available else 'unqualified_legacy_or_unavailable',
        'canonical':{'key':CURRENT,'replay':deepcopy(replay)} if available else None,
        'generated_at':p.get('generated_at') if available else None,'reference_hashes_independently_checked_by_consumer':False,
        'pair_data':{},'fx_roro':{},'regime_signals':[],'regime_metrics':{},
        'call':None,'score':None,'portfolio_action':'WAIT','independent_investment_votes':0,**dict.fromkeys(FLAGS,False),
        'meaning':'Retained dated spot quote comparisons do not establish a dollar index, funding cost, carry profit, risk regime or investment recommendation.'}
