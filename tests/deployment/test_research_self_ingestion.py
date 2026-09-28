from pathlib import Path
import hashlib
import ast
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[2]


def test_whole_research_self_ingestion_predecessors_are_preserved():
    expected={'harvester':(18280,'ff0a4bee2f221787af73bcf7592921e254dc6aa7e96e96c159b887bd6c6fddc2'),
              'evaluator':(11327,'87523c78b2184c57a15e1d6e36f93981decd505899d8cae9bf9f15e19efe3a57'),
              'identity':(1638,'e5f56284e3e3ddde69e86b68e7bf4beb186f49874ddb5d09b48510d4e4da5ec3')}
    for name,(size,sha) in expected.items():
        raw=(ROOT/f'tests/fixtures/pre-research-self-ingestion-{name}.py.txt').read_bytes()
        assert len(raw)==size and hashlib.sha256(raw).hexdigest()==sha


def test_harvester_and_evaluator_reject_their_own_reports_without_erasing_history():
    for engine in ('justhodl-signal-harvester','justhodl-prospective-evaluator'):
        result=subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/engine/'tests/run_tests.py')],
                              cwd=ROOT,capture_output=True,text=True,timeout=90)
        assert result.returncode==0,result.stdout+result.stderr


def test_self_ingestion_acceptance_does_not_read_downstream_outputs_or_invoke_engines():
    path=ROOT/'aws/ops/staged/ops_6305_research_self_ingestion_acceptance.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6305_research_self_ingestion_acceptance.py'
    source=path.read_text(encoding='utf-8');tree=ast.parse(source)
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls.intersection({'scan','query','invoke','put_object','put_item','update_schedule','put_rule','urlopen','get_secret_value','get_parameter'})
    assert 'data/prospective-outcomes.json' not in source and 'sys.exit(1)' in source
    assert 'new_normal_scheduled_capture_verified=new' in source
