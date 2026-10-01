"""Exercise the native complete inventory in older donor-consumer regressions."""
from pathlib import Path
import hashlib, importlib.util
ROOT = Path(__file__).resolve().parents[1]
path = ROOT/'aws/lambdas/justhodl-signal-board/source/signal_board_candidate.py'
spec = importlib.util.spec_from_file_location('native_board_candidate', path)
candidate = importlib.util.module_from_spec(spec); spec.loader.exec_module(candidate)

def assert_compiler_pin():
    assert hashlib.sha256(path.read_bytes()).hexdigest() == '5fdb0968d19c5c8c9332d887a79a6ae921aef858932ec4d9ebfe7c7a438aa3c6'

def assert_abstention(key, packets):
    import json
    assert_compiler_pin()
    rows = json.loads((path.parent/'board_registry.json').read_bytes())
    assert key in {r['source_key'] for r in rows}
    for packet in packets:
        raw = candidate.encoded(packet); digest = candidate.sha(raw)
        captures = {r['source_key']: {'source_key': r['source_key'], 'status': 'transport_or_size_unavailable'} for r in rows}
        for private in candidate.PRIVATE: captures[private] = {'source_key': private, 'status': 'excluded_private_account_input', 'requested': False}
        captures[key] = {'source_key': key, 'status': 'public_sidecar_retained', 'http_status': 200,
                         'original': {'key': 'test/'+digest+'.bin', 'sha256': digest, 'bytes': len(raw)}}
        out = candidate.build(rows, captures, lambda ref: raw, '2026-09-26T12:00:00Z')
        assert out['n_engines'] == len(rows) == 99
        assert set(out['sources']) == {r['source_key'] for r in rows}
        assert len(out['sources']) == 98
        assert [(r['engine'], r['category'], r['source_key']) for r in out['engines']] == [
            (r['engine'], r['category'], r['source_key']) for r in rows]
        assert any(r['source_key'] == key for r in out['engines'])
        assert out['sources'][key]['status'] == 'derived_packet_retained'
        assert out['sources'][key]['original']['sha256'] == digest
        assert out['n_live'] == 0
        assert out['composite_signal'] is None
        assert out['composite_posture'] == out['decision']['verb'] == 'WAIT'
        assert out['decision']['qualified_votes'] == 0
        graph = out['dependency_graph']
        assert graph['independent_votes_qualified'] == 0
        assert graph['registered_views'] == len(rows)
        assert graph['distinct_derived_sources'] == len(out['sources'])
        assert set(out['categories']) == {r['category'] for r in rows}
        for category, value in out['categories'].items():
            assert value['n'] == 0 and value['signal'] is None
            assert value['registered_rows'] == sum(r['category'] == category for r in rows)
        for level in (out, *out['sources'].values(), *out['engines']):
            assert all(level[permission] is False for permission in candidate.PERMISSIONS)
        for row in out['engines']:
            assert row['signal'] is None and row['signal_label'] == 'ABSTAIN'
        for permission in candidate.PERMISSIONS:
            if packet.get(permission) is True:
                assert out['sources'][key]['permission_declarations'][permission] == 'declared_true'
