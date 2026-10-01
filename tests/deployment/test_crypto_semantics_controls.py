"""Read-only named baseline guards; no producer or application-packet calls."""
from pathlib import Path
import hashlib,json,subprocess,sys
R=Path(__file__).resolve().parents[2];F=R/'tests/fixtures/crypto-semantics-controls'
def test_crypto_semantic_baseline_and_complete_predecessors():
    plan=json.loads((F/'preservation.json').read_bytes())
    for archive,target,prefix,edits in [
        ('controls-before.py.txt','aws/ops/staged/ops_6434_crypto_semantics_controls.py','baseline','source_edits'),
        ('controls-tests-before.py.txt','aws/ops/checks/test_crypto_semantics_controls.py','test','test_edits')]:
        raw=(F/archive).read_bytes();assert hashlib.sha256(raw).hexdigest()==plan[prefix+'_predecessor_sha256'];text=raw.decode('utf-8')
        for before,after in plan[edits]:assert text.count(before)==1;text=text.replace(before,after)
        assert text==(R/target).read_text(encoding='utf-8');assert hashlib.sha256(text.encode()).hexdigest()==plan[prefix+'_candidate_sha256']
    subprocess.run([sys.executable,str(R/'aws/ops/checks/test_crypto_semantics_controls.py')],cwd=R,check=True)
