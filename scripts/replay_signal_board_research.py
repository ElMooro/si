"""Reconstruct the complete board from immutable whole derived sidecars."""
from pathlib import Path
import json, sys, urllib.request
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/lambdas/justhodl-signal-board/source'))
import board_store as store

def read(key):
    if not store.allowed(key): raise ValueError('Unreviewed artifact path')
    request = urllib.request.Request('https://justhodl.ai/'+key+'?exact=1&nogen=1', headers={'User-Agent': 'justhodl-verify-release/1.0'})
    with urllib.request.urlopen(request, timeout=45) as response: return store.bounded(response)

def verify(packet, reader=read):
    if packet.get('contract') != store.candidate.CONTRACT: raise ValueError('Normal native publication not yet verified')
    proof = store.replay(packet, reader)
    return {'contract': packet['contract'], 'generated_at': packet['generated_at'], 'complete_derived_inventory_replay': True,
            'proof': proof, 'original_provider_verified': False, 'predictive_validation_performed': False}

if __name__ == '__main__': print(json.dumps(verify(json.loads(read(store.CURRENT))), sort_keys=True))
