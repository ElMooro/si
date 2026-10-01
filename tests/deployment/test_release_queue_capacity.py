"""Keep every waiting release and direct acceptance in the same serial group."""
from pathlib import Path
import hashlib,json,re
ROOT=Path(__file__).resolve().parents[2]
FIXTURE=ROOT/'tests/fixtures/release-queue'

def test_release_and_direct_acceptance_queues_preserve_waiters_and_running_jobs():
    entries=json.loads((FIXTURE/'preservation.json').read_bytes())
    assert set(entries)=={'.github/workflows/deploy-lambdas.yml','.github/workflows/run-ops-direct.yml'}
    for path,entry in entries.items():
        text=(ROOT/path).read_text(encoding='utf-8')
        blocks=re.findall(r'^concurrency:\n((?:[ \t]+[^\n]*\n)+)',text,re.M)
        assert len(blocks)==1
        # The fixed group must remain shared across commits; a per-SHA group
        # would allow simultaneous AWS updates rather than preserving waiters.
        assert blocks[0].splitlines()==['  group: '+entry['group'],'  cancel-in-progress: false','  queue: max']

def test_queue_change_preserves_all_workflow_steps_and_previous_dependency_repair():
    entries=json.loads((FIXTURE/'preservation.json').read_bytes())
    for path,entry in entries.items():
        before=(ROOT/entry['archive']).read_bytes()
        assert hashlib.sha256(before).hexdigest()==entry['predecessor_sha256']
        text=before.decode('utf-8')
        for old,new in entry['edits']:
            assert text.count(old)==1
            text=text.replace(old,new)
        assert text==(ROOT/path).read_text(encoding='utf-8')
    prior=json.loads((FIXTURE/'prior-numpy-preservation.json').read_bytes())
    prior['.github/workflows/deploy-lambdas.yml']['edits'].extend(entries['.github/workflows/deploy-lambdas.yml']['edits'])
    assert prior==json.loads((ROOT/'tests/fixtures/no-paid-adapters/numpy-preflight-repair.json').read_bytes())
