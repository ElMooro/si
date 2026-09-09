"""Reproduce recovery across the failed push rather than only the fixing commit."""
import json
import os
import re
import runpy
import subprocess
import tempfile
import textwrap
from pathlib import Path
from types import SimpleNamespace

from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / '.github/workflows/deploy-lambdas.yml'


def detect_script(head):
    text = WORKFLOW.read_text()
    start = text.index('        run: |\n', text.index('- name: Detect changed Lambdas'))
    end = text.index('\n      - name:', start)
    body = textwrap.dedent(text[start+len('        run: |\n'):end])
    values = {'github.event.after || github.sha':head, 'github.event.before':'',
              'github.event.inputs.function':'', 'github.event_name':'workflow_dispatch',
              'secrets.AWS_REGION':'us-east-1', 'github.sha':head}
    return re.sub(r'\$\{\{\s*(.*?)\s*\}\}', lambda match: values[match.group(1)], body)


def test_recovery_redeploys_original_source_changes_and_shared_importers():
    with tempfile.TemporaryDirectory() as temp:
        repo = Path(temp)
        def git(*args):
            return subprocess.check_output(['git',*args],cwd=repo,text=True,stderr=subprocess.DEVNULL).strip()
        def write(name, data):
            path=repo/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(data)
        git('init');git('config','user.name','test');git('config','user.email','test@example.invalid')
        write('.github/workflows/deploy-lambdas.yml', WORKFLOW.read_text())
        write('aws/lambdas/engine-a/source/lambda_function.py','old=1\n')
        write('aws/lambdas/engine-c/source/lambda_function.py','from donor import value\n')
        write('aws/shared/donor.py','value=1\n')
        git('add','.');git('commit','-m','baseline');base=git('rev-parse','HEAD')
        write('aws/lambdas/engine-a/source/lambda_function.py','fixed=1\n')
        write('aws/lambdas/engine-b/source/lambda_function.py','new=1\n')
        write('aws/shared/donor.py','value=2\n')
        git('add','.');git('commit','-m','failed release')
        write('scripts/deploy_lambdas.sh','#!/bin/bash\ntrue\n')
        git('add','.');git('commit','-m','workflow repair');head=git('rev-parse','HEAD')
        git('remote','add','origin',str(repo))
        assert not any(name.startswith('aws/') for name in git('diff','--name-only','HEAD^','HEAD').splitlines())
        output=repo/'outputs'
        env={**os.environ,'RECOVERY_BASE_SHA':base,'EXPECTED_RELEASE_SHA':head,'GITHUB_OUTPUT':str(output)}
        result=subprocess.run(['bash'],input=detect_script(head),text=True,cwd=repo,env=env,capture_output=True)
        assert result.returncode==0, result.stderr
        assert set(output.read_text().strip().split('targets=')[-1].split())=={'engine-a','engine-b','engine-c'}
        # Inputs are data; reject non-SHA text before any fetch or shell evaluation.
        env['RECOVERY_BASE_SHA']='$(touch forbidden-marker)'
        result=subprocess.run(['bash'],input=detect_script(head),text=True,cwd=repo,env=env,capture_output=True)
        assert result.returncode!=0 and not (repo/'forbidden-marker').exists()
        env['RECOVERY_BASE_SHA']=base;env['EXPECTED_RELEASE_SHA']='0'*40
        result=subprocess.run(['bash'],input=detect_script(head),text=True,cwd=repo,env=env,capture_output=True)
        assert result.returncode!=0 and 'commit changed' in result.stdout
        # A later commit can change the branch after dispatch. Deploy precisely
        # the requested source; newer changes must not contaminate its target set.
        write('aws/lambdas/engine-unrequested/source/lambda_function.py','later=1\n')
        git('add','aws/lambdas/engine-unrequested');git('commit','-m','branch advanced after dispatch')
        observer=git('rev-parse','HEAD')
        pin=runpy.run_path(str(ROOT/'scripts/pin_release_checkout.py'))['pin_checkout']
        receipt=pin(repo,head,observer,base=base)
        assert receipt['release_sha']==head and git('rev-parse','HEAD')==head
        assert not (repo/'aws/lambdas/engine-unrequested').exists()
        output.unlink();env['EXPECTED_RELEASE_SHA']=head
        result=subprocess.run(['bash'],input=detect_script(observer),text=True,cwd=repo,env=env,capture_output=True)
        assert result.returncode==0, result.stderr
        assert set(output.read_text().strip().split('targets=')[-1].split())=={'engine-a','engine-b','engine-c'}


def test_all_lambda_workflow_run_blocks_remain_below_expression_limit():
    lines=WORKFLOW.read_text().splitlines()
    for index,line in enumerate(lines):
        if re.match(r'\s+run: [|>]',line):
            indent=len(line)-len(line.lstrip());body=[]
            for following in lines[index+1:]:
                if following.strip() and len(following)-len(following.lstrip())<=indent:break
                body.append(following)
            # Leave headroom for expression evaluation/serialization overhead.
            assert len('\n'.join(body))<19000, (index+1,len('\n'.join(body)))
    deploy=(ROOT/'scripts/deploy_lambdas.sh').read_text()
    assert '${{' not in deploy and 'DEPLOY_TARGETS' in deploy and 'DEPLOY_AWS_REGION' in deploy
    subprocess.run(['bash','-n'],input=deploy,text=True,check=True)


def test_ops_recovery_dispatch_pins_workflow_and_original_base_without_aws():
    path=ROOT/'aws/ops/pending/ops_5233_retry_audit_core_release.py'
    if not path.exists():path=ROOT/'aws/ops/ran'/path.name
    scope=runpy.run_path(str(path))
    payload=scope['dispatch_payload']('a'*40)
    assert payload=={'ref':'main','inputs':{'base_sha':'c167a7541b9b02f849343bad1b4a231e758cadd8','expected_sha':'a'*40}}
    try:scope['dispatch_payload']('main')
    except ValueError:pass
    else:raise AssertionError('Unpinned workflow recovery accepted')
    assert 'boto3' not in path.read_text()
    assert 'actions: write' in (ROOT/'.github/workflows/run-ops.yml').read_text()


def recovery_5235():
    path=ROOT/'aws/ops/pending/ops_5235_retry_alias_protected_release.py'
    if not path.exists():path=ROOT/'aws/ops/ran'/path.name
    assert 'boto3' not in path.read_text()
    return runpy.run_path(str(path))['main']


def recovery_5236():
    path=ROOT/'aws/ops/pending/ops_5236_retry_pinned_core_release.py'
    if not path.exists():path=ROOT/'aws/ops/ran'/path.name
    assert 'boto3' not in path.read_text()
    return runpy.run_path(str(path))['main']


def recovery_5237():
    path=ROOT/'aws/ops/pending/ops_5237_retry_pinned_core_release.py'
    if not path.exists():path=ROOT/'aws/ops/ran'/path.name
    assert 'boto3' not in path.read_text()
    return runpy.run_path(str(path))['main']


def test_alias_recovery_uses_distinct_receipt_and_exact_head_once():
    with tempfile.TemporaryDirectory() as temp:
        _alias_recovery_once(Path(temp))


def _alias_recovery_once(tmp_path, operation='5235'):
    main={'5235':recovery_5235,'5236':recovery_5236,'5237':recovery_5237}[operation]();scope=main.__globals__
    assert scope['REPORT'].name=='ops_'+operation+'_core_recovery_dispatch.json'
    report=tmp_path/scope['REPORT'].name
    # A failed earlier attempt must not suppress the independently numbered retry.
    (tmp_path/'ops_5233_core_recovery_dispatch.json').write_text('{"request_sent":true}')
    scope['REPORT']=report
    head='a'*40;git_calls=[];requests=[]
    scope['subprocess']=SimpleNamespace(
        check_output=lambda *args,**kw:head+'\n',
        run=lambda *args,**kw:git_calls.append((args,kw)))
    def request(repository,token,path,payload=None):
        requests.append((path,payload))
        assert json.loads(report.read_text())['request_sent'] is True
        if payload is not None:
            assert payload=={'ref':'main','inputs':{
                'base_sha':'c167a7541b9b02f849343bad1b4a231e758cadd8','expected_sha':head}}
            return 200,{'workflow_run_id':123}
        return 200,{'head_sha':head if operation=='5235' else 'b'*40}
    scope['request']=request
    with patch.dict(os.environ,{'GITHUB_REPOSITORY':'owner/repo','GH_API_TOKEN':'test-secret-never-logged'}):
        main();main()
    assert len(requests)==2 and len(git_calls)==2
    saved=json.loads(report.read_text())
    assert saved['operation']==operation and saved['status']=='DISPATCHED'
    assert saved['workflow_sha_matches'] is (operation=='5235') and saved['aws_calls']==0
    if operation in ('5236','5237'):
        assert saved['expected_checkout_sha']==head and saved['checkout_verification']=='REQUIRED_IN_RELEASE_JOB'
    assert 'test-secret' not in report.read_text()


def test_pinned_retry_preserves_requested_commit_when_dispatch_branch_advances():
    with tempfile.TemporaryDirectory() as temp:
        _alias_recovery_once(Path(temp), '5236')


def test_metadata_retry_is_a_distinct_exact_source_dispatch():
    with tempfile.TemporaryDirectory() as temp:
        _alias_recovery_once(Path(temp), '5237')


def test_recovery_checkout_is_verified_before_credentials_and_code_staging():
    workflow=WORKFLOW.read_text()
    assert workflow.index('- name: Pin exact recovery checkout') < workflow.index('- name: Configure AWS credentials')
    assert workflow.index('- name: Pin exact recovery checkout') < workflow.index('- name: Detect changed Lambdas')
    assert 'triggering_sha="$EXPECTED_RELEASE_SHA"' in workflow


def test_alias_recovery_uncertain_request_never_auto_duplicates_or_logs_body():
    with tempfile.TemporaryDirectory() as temp:
        _alias_recovery_uncertain(Path(temp))


def _alias_recovery_uncertain(tmp_path):
    main=recovery_5235();scope=main.__globals__
    report=tmp_path/'receipt.json';calls=[]
    scope['REPORT']=report
    scope['subprocess']=SimpleNamespace(
        check_output=lambda *args,**kw:'b'*40,
        run=lambda *args,**kw:None)
    def request(*args):
        calls.append(args)
        raise RuntimeError('sensitive-response-body')
    scope['request']=request
    with patch.dict(os.environ,{'GITHUB_REPOSITORY':'owner/repo','GH_API_TOKEN':'test-secret'}):
        try:main()
        except RuntimeError as error:assert 'no automatic duplicate' in str(error)
        else:raise AssertionError('Uncertain request reported success')
        main()
    assert len(calls)==1
    saved=json.loads(report.read_text())
    assert saved['status']=='DISPATCH_STATUS_UNCERTAIN'
    assert 'sensitive-response-body' not in report.read_text()
