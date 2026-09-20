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


def test_workflow_shared_filter_excludes_test_files_in_mixed_release():
    import re
    source=(ROOT/'.github/workflows/deploy-lambdas.yml').read_text(encoding='utf-8')
    filters=re.findall(r"shared_changed=.*?grep -E '([^']+)'",source)
    assert len(filters)==2
    changed=['aws/shared/canonical_fred_replay.py','aws/shared/eurodollar_research.py',
             'aws/shared/tests/test_canonical_fred_replay.py','aws/lambdas/engine/source/lambda_function.py']
    for pattern in filters:
        assert [p for p in changed if re.search(pattern,p)]==changed[:2]
        assert not re.search(pattern,'aws/shared/nested/not_a_bundled_module.py')


def test_detection_failure_is_captured_and_included_in_failure_report():
    source=(ROOT/'.github/workflows/deploy-lambdas.yml').read_text(encoding='utf-8')
    detect=source.split('- name: Detect changed Lambdas',1)[1].split('- name: Run deployment preflight tests',1)[0]
    assert 'exec > >(tee -a "$RUNNER_TEMP/detect.log") 2>&1' in detect
    report=source.split('- name: Commit a redacted failure report',1)[1]
    assert "steps.detect.outcome == 'failure'" in report
    assert "steps.preflight.outcome == 'failure' && 'preflight' || 'detect'" in report
