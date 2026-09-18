import copy
import importlib.util
import io
import json
from datetime import datetime, timezone
from pathlib import Path
import runpy
import sys
import types
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parents[1] / 'shared'))
fixtures = runpy.run_path(str(ROOT.parent / 'justhodl-ai-brief/tests/replay_tests.py'))
from calls_research_replay import prepare, persist, canonical
from calls_contract import make_snapshot

def setup():
    store = fixtures['Store']()
    bundle = prepare(lambda key: fixtures['DOCS'].get(key, {}), fixtures['AT'])
    ref = persist(store, 'fixture', bundle)
    public = copy.deepcopy(bundle['payload']['output']); public['research_replay'] = ref
    row = make_snapshot({'as_of': fixtures['AT']}, public); public['snapshot_id'] = row['snapshot_id']
    store.objects['data/ai-brief-public.json'] = canonical(public)
    store.objects['data/decisive-call-history.json'] = canonical({'snapshots': [row]})
    with patch.dict(sys.modules, {'boto3': types.SimpleNamespace(client=lambda *a, **kw: store)}):
        spec = importlib.util.spec_from_file_location('audit_fixture', ROOT / 'source/lambda_function.py')
        mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod, store, public, row

mod, store, public, row = setup()
proof = mod.verify_current(store, 'fixture', datetime(2026, 9, 18, 17, 5, tzinfo=timezone.utc))
assert proof['status'] == 'reproduced' and proof['inputs'] == 7 and proof['private_account_data_read'] is False
assert proof['publication_overdue'] is False and proof['snapshot_id'] == row['snapshot_id']
mod.BUCKET = 'fixture'
assert mod.lambda_handler({}, None)['statusCode'] == 200
assert 'data/calls-research-audit.json' in store.objects
assert any(key.startswith('data/calls-research-audit-events/') for key in store.objects)
for kind in ('bundle', 'current', 'history', 'sizing'):
    mod, store, public, row = setup()
    if kind == 'bundle': store.objects[public['research_replay']['bundle_key']] += b' '
    if kind == 'current':
        public['brief_md'] += 'unrecorded narrative'; store.objects['data/ai-brief-public.json'] = canonical(public)
    if kind == 'history':
        row['brief_sha256'] = '0'*64; store.objects['data/decisive-call-history.json'] = canonical({'snapshots':[row]})
    if kind == 'sizing':
        row['sizing_eligible'] = True; store.objects['data/decisive-call-history.json'] = canonical({'snapshots':[row]})
    try: mod.verify_current(store, 'fixture')
    except ValueError: pass
    else: raise AssertionError('audit accepted '+kind+' mismatch')
mod, store, public, row = setup()
mod.BUCKET = 'fixture'
mod.lambda_handler({}, None)
store.objects[public['research_replay']['bundle_key']] += b' '
try: mod.lambda_handler({}, None)
except RuntimeError: pass
else: raise AssertionError('scheduled audit failure was swallowed')
proof_key = 'data/calls-research-proofs/'+public['research_replay']['payload_sha256']+'.json'
assert json.loads(store.objects[proof_key])['status'] == 'failed'
assert json.loads(store.objects['data/calls-research-audit.json'])['status'] == 'failed'
print('Independent Calls audit passed: valid replay, append-only proof, bundle/current/history/authority tampering')
