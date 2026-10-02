from pathlib import Path
import hashlib,json
ROOT=Path(__file__).resolve().parents[2]
D=ROOT/'tests/fixtures/worker-crypto-source'
def test_source_attribution_preserves_every_unrelated_worker_byte():
    from crypto_source_preservation import restore_crypto_source
    current=(ROOT/'cloudflare/workers/justhodl-data-proxy/src/index.js').read_text(encoding='utf-8')
    restore_crypto_source(current)
    try:restore_crypto_source(current.replace('async function extendCryptoDaily','async function changedCryptoDaily',1))
    except AssertionError:pass
    else:raise AssertionError('unreviewed Worker mutation accepted')
def test_existing_snapshot_preservation_keeps_all_prior_assertions():
    t=json.loads((D/'python-test-hook.json').read_bytes());old=(D/'snapshot-test-before.py.txt').read_text(encoding='utf-8')
    digest=lambda s:hashlib.sha256(s.encode()).hexdigest()
    assert digest(old)==t['before_sha256'] and old.count(t['before'])==1
    actual=(ROOT/t['path']).read_text(encoding='utf-8')
    assert actual==old.replace(t['before'],t['after']) and digest(actual)==t['after_sha256']
