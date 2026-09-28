from pathlib import Path
import ast,subprocess,sys
ROOT=Path(__file__).resolve().parents[2]


def test_complete_genealogy_retention_and_publication_boundary():
    result=subprocess.run([sys.executable,str(ROOT/'tests/test_genealogy_native_pipeline.py')],cwd=ROOT,capture_output=True,text=True,timeout=60)
    assert result.returncode==0,result.stdout+result.stderr


def test_genealogy_promoted_candidates_preserve_accepted_algorithms():
    base=ROOT/'aws/ops/checks';candidate=base/'genealogy_native_candidate'
    for name in ('genealogy_registration_model.py','genealogy_capture_timing.py'):
        assert (base/name).read_bytes()==(candidate/name).read_bytes()
    original=(base/'genealogy_public_archive.py').read_text(encoding='utf-8')
    expected=original.replace('def audit(client, inventories, workers=8):','def audit(client, inventories, workers=8, capture_observer=None):')
    expected=expected.replace('value = validate_capture(doc, receipt)','value = validate_capture(doc, receipt)\n                if capture_observer is not None:\n                    capture_observer(doc, receipt)')
    assert (candidate/'genealogy_public_archive.py').read_text(encoding='utf-8')==expected


def test_genealogy_candidate_never_writes_with_the_actual_iam_client():
    path=ROOT/'aws/ops/staged/ops_6308_genealogy_pipeline_candidate.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6308_genealogy_pipeline_candidate.py'
    source=path.read_text(encoding='utf-8');tree=ast.parse(source)
    # The fake in-memory writer exercises the real retention code. The IAM
    # client is used only by the reviewed original reader in model.collect.
    for node in ast.walk(tree):
        if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute) and isinstance(node.func.value,ast.Name) and node.func.value.id=='client':
            assert node.func.attr not in {'put_object','invoke','scan','query','get_secret_value','update_function_configuration'}
    assert 'model.collect(client,inventories)' in source and 'store.retain(memory,' in source
    assert 'sys.exit(1)' in source
