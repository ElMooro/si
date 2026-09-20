"""Dynamic policy guards must redeploy through both native pushes and apply-lane dispatches."""
from pathlib import Path
import sys,tempfile
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from shared_dependents import dependents


def test_literal_dynamic_and_unbounded_transitive_imports_are_deployed():
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp);shared=root/'aws/shared';shared.mkdir(parents=True)
        (shared/'guard.py').write_text('VALUE=False\n')
        for i in range(6):(shared/f'layer{i}.py').write_text('import '+('guard' if i==0 else 'layer'+str(i-1))+'\n')
        for name,text in {'direct':"x=__import__('guard')\n",'aliased':"from importlib import import_module as load\nx=load('guard')\n",
                          'transitive':"import layer5\n",'module':"import importlib\nx=importlib.import_module('guard')\n",
                          'unrelated':"text='guard'\n"}.items():
            p=root/'aws/lambdas'/name/'source';p.mkdir(parents=True);(p/'lambda_function.py').write_text(text)
        archived=root/'aws/lambdas/_archived/source';archived.mkdir(parents=True);(archived/'lambda_function.py').write_text('import guard\n')
        assert dependents(root,['aws/shared/guard.py'])==['aliased','direct','module','transitive']


def test_native_workflow_uses_same_transitive_analysis_as_package_evidence():
    source=(ROOT/'.github/workflows/deploy-lambdas.yml').read_text()
    assert 'python3 scripts/shared_dependents.py $shared_changed' in source
    assert 'for _pass in 1 2 3' not in source
    assert 'if [ -n "$shared_changed" ]' in source
