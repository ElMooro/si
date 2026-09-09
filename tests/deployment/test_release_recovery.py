"""Reproduce recovery across the failed push rather than only the fixing commit."""
import json
import os
import re
import runpy
import subprocess
import tempfile
import textwrap
from pathlib import Path

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
