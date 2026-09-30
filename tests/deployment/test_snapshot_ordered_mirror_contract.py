"""Whole inert predecessors, unchanged native/risk sources and route boundaries."""
from pathlib import Path
import hashlib,json
ROOT=Path(__file__).resolve().parents[2]


def test_snapshot_ordering_preserves_whole_predecessors_and_unchanged_native_risk_code():
    audit=json.loads((ROOT/'docs/audit/2026-09-30/portfolio-snapshot-ordered-mirror.json').read_bytes())
    for row in audit['fixtures'].values():
        raw=(ROOT/row['path']).read_bytes();assert len(raw)==row['bytes'] and hashlib.sha256(raw).hexdigest()==row['sha256']
    for path,row in audit['unchanged_sources'].items():
        retained = ROOT/'tests/fixtures/pre-snapshot-native-ordering'/ (Path(path).name+'.txt') if path.startswith('aws/lambdas/justhodl-portfolio-snapshot/source/') else ROOT/path
        raw=retained.read_bytes();assert len(raw)==row['bytes'] and hashlib.sha256(raw).hexdigest()==row['sha256']
    assert audit['snapshot_native_ordering_implemented'] is False and audit['two_destination_atomicity'] is False


def test_snapshot_ordering_does_not_rewrite_unrelated_worker_handlers():
    prior=(ROOT/'tests/fixtures/pre-snapshot-ordered-mirror/index.js.txt').read_text(encoding='utf-8')
    current=(ROOT/'cloudflare/workers/justhodl-data-proxy/src/index.js').read_text(encoding='utf-8')
    current=current.replace("import { handleSnapshotPublication, routeSnapshotPublication } from './snapshot-publication.js';","import { publishSnapshot } from './portfolio-snapshot.js';",1)
    a=current.index("    if (new URL(request.url).pathname === '/snapshot-publication') {")
    b=current.index("    if (new URL(request.url).pathname === '/risk-publication') {",a)
    current=current[:a]+current[b:]
    a=current.index("      if (kind === 'portfolio-snapshot') {")
    b=current.index('      const key = "private-artifact:" + kind;',a)
    current=current[:a]+current[b:]
    needle='        const raw = await boundedBody(request, 20000000);'
    current=current.replace(needle,"        if (kind === 'portfolio-snapshot') return publishSnapshot(request, env, corsHeaders());\n"+needle,1)
    assert current==prior
