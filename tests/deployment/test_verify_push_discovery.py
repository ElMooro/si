"""A transient Actions discovery response cannot erase known deployment runs."""
from pathlib import Path
from unittest.mock import patch
from contextlib import redirect_stdout
import importlib.util,io

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('verify_push_discovery_test',ROOT/'scripts/verify_push.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
SHA='a'*40


def run(status='in_progress',conclusion=None,run_id=7):
    return {'id':run_id,'head_sha':SHA,'event':'push','status':status,'conclusion':conclusion,
            'created_at':'2026-09-28T12:00:00Z','path':'.github/workflows/deploy-lambdas.yml','html_url':'https://github.com/owner/repo/actions/runs/'+str(run_id)}


def test_empty_discovery_refreshes_known_run_by_its_id():
    observed={7:run()}
    with patch.object(m,'runs_for',return_value=[]),patch.object(m,'get',return_value=run('completed','failure')) as get:
        result=m.run_snapshot('owner/repo',SHA,observed)
    assert result[0]['conclusion']=='failure' and observed[7]['conclusion']=='failure'
    get.assert_called_once_with('/repos/owner/repo/actions/runs/7')


def test_partial_discovery_preserves_missing_and_new_runs():
    observed={7:run()}
    with patch.object(m,'runs_for',return_value=[run('completed','success',8)]),patch.object(m,'get',return_value=run('completed','success')):
        result=m.run_snapshot('owner/repo',SHA,observed)
    assert {r['id'] for r in result}=={7,8}


def test_changed_known_run_identity_cannot_borrow_success():
    for replacement in ({**run('completed','success'),'head_sha':'b'*40},{**run('completed','success'),'id':True},{**run('completed','success'),'event':'schedule'}):
        observed={7:run()}
        with patch.object(m,'runs_for',return_value=[]),patch.object(m,'get',return_value=replacement):
            try:m.run_snapshot('owner/repo',SHA,observed)
            except ValueError:pass
            else:raise AssertionError('Changed workflow identity accepted')
        assert observed[7]['status']=='in_progress'


def test_main_failure_survives_temporarily_empty_list():
    commit={'commit':{'message':'Synthetic change','author':{'name':'Fixture'}},'files':[]}
    def get(path):
        if '/commits/' in path:return commit
        if path.endswith('/runs/7'):return run('completed','failure')
        if path.endswith('/jobs?per_page=50'):return {'jobs':[]}
        raise AssertionError(path)
    with patch.object(m,'get',side_effect=get),patch.object(m,'runs_for',side_effect=[[run()],[]]),patch.object(m.time,'sleep'),redirect_stdout(io.StringIO()) as out:
        code=m.main([SHA,'--repo','owner/repo','--wait','1'])
    assert code==1 and 'A RUN FAILED' in out.getvalue() and 'no push/dispatch run' not in out.getvalue()


def test_unavailable_known_run_is_inconclusive_never_no_runs_success():
    commit={'commit':{'message':'Synthetic change','author':{'name':'Fixture'}},'files':[]}
    def get(path):
        if '/commits/' in path:return commit
        raise OSError('Synthetic unavailable')
    with patch.object(m,'get',side_effect=get),patch.object(m,'runs_for',side_effect=[[run()],[]]),patch.object(m.time,'sleep'),redirect_stdout(io.StringIO()) as out:
        code=m.main([SHA,'--repo','owner/repo','--wait','1'])
    assert code==2 and 'unavailable' in out.getvalue() and 'no push/dispatch run' not in out.getvalue()


def test_initial_no_workflows_remains_explicit_without_deployment_claim():
    observed={}
    with patch.object(m,'runs_for',return_value=[]),patch.object(m,'get') as get:
        assert m.run_snapshot('owner/repo',SHA,observed)==[]
    get.assert_not_called();assert observed=={}
