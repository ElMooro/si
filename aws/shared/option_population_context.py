"""Consumer boundary: captured options do not establish dealer trade signals."""
from copy import deepcopy
import re

FLAGS=('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')
NOTE=('Captured call/put populations do not reveal dealer ownership, hedge transactions, '
      'a zero-gamma flip or a validated directional edge. Use the canonical source '
      'membership for research; no dealer score, level, veto or investment vote is qualified.')


def context(packet):
    packet=packet if isinstance(packet,dict) else {}
    canonical=packet.get('canonical');canonical=canonical if isinstance(canonical,dict) else {}
    replay=canonical.get('replay');replay=replay if isinstance(replay,dict) else {}
    native=(packet.get('contract')=='dealer-gex-compatibility.v1'
        and all(packet.get(k) is False for k in FLAGS)
        and packet.get('underlyings')=={} and packet.get('market_composite')=={}
        and canonical.get('key')=='data/option-population-research.json'
        and isinstance(replay.get('manifest_key'),str)
        and re.fullmatch(r'data/option-population-research/runs/[a-f0-9]{64}\.json',replay['manifest_key'])
        and isinstance(replay.get('output_sha256'),str) and re.fullmatch('[a-f0-9]{64}',replay['output_sha256']))
    return {'contract':'option-population-context.v1','source_key':'data/dealer-gex.json',
        'status':'descriptive_native_reference' if native else 'unqualified_legacy_or_unavailable',
        'native_reference_available':bool(native),'canonical':{'key':canonical['key'],'replay':deepcopy(replay)} if native else None,
        'source_capture_completed_at':packet.get('source_capture_completed_at') if native else None,
        'generated_at':packet.get('source_capture_completed_at') if native else None,
        'underlyings':{},'market_composite':{},'squeeze_candidates':[],
        'call':None,'score':None,'portfolio_action':'WAIT','independent_investment_votes':0,
        **{k:False for k in FLAGS},'evidence_note':NOTE}


def guard(key,packet):
    return context(packet) if key=='data/dealer-gex.json' else packet
