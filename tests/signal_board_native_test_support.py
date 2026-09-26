"""Exercise the native complete inventory in older donor-consumer regressions."""
from pathlib import Path
import importlib.util
ROOT = Path(__file__).resolve().parents[1]
path = ROOT/'aws/lambdas/justhodl-signal-board/source/signal_board_candidate.py'
spec = importlib.util.spec_from_file_location('native_board_candidate', path)
candidate = importlib.util.module_from_spec(spec); spec.loader.exec_module(candidate)

def assert_abstention(key, packets):
    import json
    rows = json.loads((path.parent/'board_registry.json').read_bytes())
    assert key in {r['source_key'] for r in rows}
    for packet in packets:
        raw = candidate.encoded(packet); digest = candidate.sha(raw)
        captures = {r['source_key']: {'source_key': r['source_key'], 'status': 'transport_or_size_unavailable'} for r in rows}
        for private in candidate.PRIVATE: captures[private] = {'source_key': private, 'status': 'excluded_private_account_input', 'requested': False}
        captures[key] = {'source_key': key, 'status': 'public_sidecar_retained', 'http_status': 200,
                         'original': {'key': 'test/'+digest+'.bin', 'sha256': digest, 'bytes': len(raw)}}
        out = candidate.build(rows, captures, lambda ref: raw, '2026-09-26T12:00:00Z')
        assert out['composite_signal'] is None and out['calls_eligible'] is False
        assert all(row['signal'] is None and row['calls_eligible'] is False for row in out['engines'])
