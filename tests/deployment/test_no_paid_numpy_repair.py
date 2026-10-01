"""The CI repair installs the tested dependency and preserves all existing gates."""
from pathlib import Path
import hashlib,json
def test_exact_preflight_dependency_repair_preserves_workflow_and_tests():
    root=Path(__file__).resolve().parents[2];fixture=root/'tests/fixtures/no-paid-adapters'
    for path,entry in json.loads((fixture/'numpy-preflight-repair.json').read_bytes()).items():
        raw=(fixture/entry['archive']).read_bytes();assert hashlib.sha256(raw).hexdigest()==entry['predecessor_sha256'];text=raw.decode()
        for old,new in entry['edits']:assert text.count(old)==1;text=text.replace(old,new)
        assert text==(root/path).read_text(encoding='utf-8')
    text=(root/'.github/workflows/deploy-lambdas.yml').read_text(encoding='utf-8')
    preflight=text.split('- name: Run deployment preflight tests',1)[1].split('- name:',1)[0]
    assert 'numpy==2.3.5' in preflight.split('for fn in',1)[0]
