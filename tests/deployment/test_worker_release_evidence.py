"""An opaque provider identity and an exact complete build are both required."""
from pathlib import Path
import base64
import copy
import json
import os
import sys
import tempfile
import subprocess
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'aws/ops/checks'),str(ROOT/'scripts'),str(Path(__file__).parent)]
import worker_release_evidence as release
import worker_source_evidence as capture
from test_worker_source_evidence import FakeControl, multipart, CODE, refuses


def evidence():
    client=FakeControl()
    raw, headers=multipart(bodies=[('index.js',CODE,'application/javascript+module')])
    headers['ETag']='"opaque-synthetic-tag"'
    client.values['/content/v2']=(raw,headers)
    return capture.capture(client)


def run_case(change=None, file_change=None, version=release.WRANGLER_VERSION):
    value=evidence();baseline=copy.deepcopy(value['configuration'])
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp);(root/'index.js').write_bytes(CODE)
        if change:change(value)
        if file_change:file_change(root)
        return release.verify(value,root,'a'*40,baseline,version)


def test_worker_release_matches_exact_complete_bundle_without_executing_it():
    result=run_case()
    assert result['commit']=='a'*40 and result['status']=='matched'
    assert result['intended_repo_build_verified'] and result['source_active_version_binding_verified']
    assert result['private_reads']==result['worker_invocations']==0
    assert result['normal_private_publication_verified'] is False
    assert 'raw_body_base64' not in json.dumps(result)


def test_worker_release_requires_matching_strong_provider_representation():
    for tag in [None,'','W/"opaque-synthetic-tag"','opaque-synthetic-tag','"another-tag"','"opaque-synthetic-tag\r\n"']:
        refuses(lambda:run_case(lambda d:d['transport']['headers'].update(ETag=tag)))
    refuses(lambda:run_case(lambda d:d.update(version_script_etag='another-tag')))
    refuses(lambda:run_case(lambda d:d.update(version_script_etag=None)))


def test_worker_release_rejects_config_cron_entrypoint_and_tool_drift():
    refuses(lambda:run_case(lambda d:d['configuration'].update(compatibility_date='2026-01-01')))
    refuses(lambda:run_case(lambda d:d['configuration'].update(schedule_count=1)))
    refuses(lambda:run_case(lambda d:d['transport']['headers'].update({'cf-entrypoint':'other.js'})))
    refuses(lambda:run_case(version='latest'))


def test_worker_release_rejects_truncated_modified_and_extra_build_modules():
    refuses(lambda:run_case(file_change=lambda p:(p/'index.js').write_bytes(CODE[:-1])))
    refuses(lambda:run_case(file_change=lambda p:(p/'index.js').write_bytes(CODE+b'//changed')))
    refuses(lambda:run_case(file_change=lambda p:(p/'extra.js').write_bytes(b'export default 1;')))
    refuses(lambda:run_case(file_change=lambda p:(p/'index.js').unlink()))
    assert run_case(file_change=lambda p:(p/'index.js.map').write_text('synthetic-debug-map',encoding='utf-8'))['status']=='matched'


def test_worker_release_rejects_relabelled_or_partial_captured_evidence():
    refuses(lambda:run_case(lambda d:d['transport'].update(bytes=d['transport']['bytes']-1)))
    refuses(lambda:run_case(lambda d:d['transport'].update(sha256='0'*64)))
    refuses(lambda:run_case(lambda d:d['source_members']['index.js'].update(sha256='0'*64)))
    refuses(lambda:run_case(lambda d:d['transport'].update(raw_body_base64=base64.b64encode(b'incomplete').decode())))
    refuses(lambda:run_case(lambda d:d.update(worker='other-worker')))


def test_worker_release_transaction_retains_complete_predecessor_before_receipt():
    sys.path.insert(0,str(ROOT/'scripts'))
    import worker_release as cli
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp);build=root/'build';build.mkdir();(build/'index.js').write_bytes(CODE)
        original=evidence();baseline=root/'baseline.json';baseline.write_text(json.dumps(original),encoding='utf-8')
        identity={'commit':'a'*40,'repository_files':{'synthetic.js':{'sha256':'synthetic'}}}
        with patch.object(cli,'ROOT',root),patch.object(cli,'BASELINE',baseline),patch.object(cli,'build_identity',return_value=identity),patch.object(cli,'safe_capture',side_effect=[original,original]):
            assert cli.execute('before',build,root/'evidence','a'*40,'123')['phase']=='before'
            target=root/'data/ops/releases/worker-justhodl-data-proxy.json'
            assert not target.exists()
            assert cli.execute('after',build,root/'evidence','a'*40,'123')['status']=='matched'
            receipt=json.loads(target.read_bytes());artifact=root/receipt['capture_artifact']['path']
            retained=json.loads(artifact.read_bytes())
            assert retained['before']==original and retained['after']==original
            assert receipt['capture_artifact']['sha256']==capture.digest(artifact.read_bytes())


def test_worker_release_transaction_refuses_changed_build_before_capture_or_receipt():
    import worker_release as cli
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp);baseline=root/'baseline.json';original=evidence();baseline.write_text(json.dumps(original),encoding='utf-8')
        folder=root/'evidence';folder.mkdir();(folder/'before.json').write_text(json.dumps(original),encoding='utf-8')
        (folder/'build.json').write_text('{"commit":"original"}',encoding='utf-8')
        with patch.object(cli,'ROOT',root),patch.object(cli,'BASELINE',baseline),patch.object(cli,'build_identity',return_value={'commit':'changed'}),patch.object(cli,'safe_capture') as network:
            refuses(lambda:cli.execute('after',root/'build',folder,'a'*40,'123'))
            network.assert_not_called()
            assert not (root/'data').exists()


STRAY={'name':'JH_EDGE_REVALIDATE','type':'plain_text','text':'1'}


def evidence_client(extra_binding=None):
    client=FakeControl()
    raw, headers=multipart(bodies=[('index.js',CODE,'application/javascript+module')])
    headers['ETag']='"opaque-synthetic-tag"'
    client.values['/content/v2']=(raw,headers)
    if extra_binding:client.values['/settings']['bindings'].append(copy.deepcopy(extra_binding))
    return client


def test_hiding_wrapper_hides_only_the_one_known_stray_binding():
    import worker_release as cli
    base=capture.capture(evidence_client())['configuration']
    assert capture.capture(evidence_client(STRAY))['configuration']!=base
    hidden=cli.HidingStrayBinding(evidence_client(STRAY))
    assert capture.capture(hidden)['configuration']==base and hidden.stray_seen
    for other in ({'name':'OTHER_VAR','type':'plain_text','text':'1'},{'name':'JH_EDGE_REVALIDATE','type':'secret_text'}):
        hidden=cli.HidingStrayBinding(evidence_client(other))
        assert capture.capture(hidden)['configuration']!=base and not hidden.stray_seen
    clean=cli.HidingStrayBinding(evidence_client())
    assert capture.capture(clean)['configuration']==base and not clean.stray_seen


def test_configuration_difference_names_parts_and_never_values():
    import worker_release as cli
    base=capture.capture(evidence_client())['configuration']
    now=capture.capture(evidence_client(STRAY))['configuration']
    text=cli.configuration_difference(now,base)
    assert 'bindings added JH_EDGE_REVALIDATE' in text and 'configuration_sha256' in text
    assert now['bindings'][0]['configuration_sha256'] not in text and 'plain_text' not in text
    assert cli.configuration_difference(base,base)=='no difference found in the named parts'
    assert cli.configuration_difference(None,base)=='difference not describable'


def run_release(live_before,live_after,tmp):
    """Run the real before/after phases against scripted live configurations; returns the retained evidence."""
    import worker_release as cli
    root=Path(tmp);build=root/'build';build.mkdir();(build/'index.js').write_bytes(CODE)
    baseline=root/'baseline.json';baseline.write_text(json.dumps(evidence()),encoding='utf-8')
    retired=root/'tests/security/retired-secret-sha256.json';retired.parent.mkdir(parents=True)
    retired.write_bytes((ROOT/'tests/security/retired-secret-sha256.json').read_bytes())
    identity={'commit':'a'*40,'repository_files':{'synthetic.js':{'sha256':'synthetic'}}}
    live=[live_before,live_after]
    def make(account,token):
        return evidence_client(live[0] if state['phase']=='before' else live[1])
    state={'phase':'before'}
    env={'CLOUDFLARE_ACCOUNT_ID':'a'*32,'CLOUDFLARE_API_TOKEN':'synthetic-token-for-tests'}
    with patch.object(cli,'ROOT',root),patch.object(cli,'BASELINE',baseline),patch.object(cli,'build_identity',return_value=identity),\
         patch.object(cli,'ControlPlane',make),patch.dict(os.environ,env):
        cli.execute('before',build,root/'evidence','a'*40,'123')
        state['phase']='after'
        return cli.execute('after',build,root/'evidence','a'*40,'123'),root


def test_worker_release_before_accepts_the_one_known_leftover_binding_and_after_demands_the_baseline():
    with tempfile.TemporaryDirectory() as tmp:
        result,root=run_release(STRAY,None,tmp)
        assert result['status']=='matched'
        saved=json.loads((root/'evidence/before.json').read_bytes())
        assert saved['tolerated_stray_bindings']==['JH_EDGE_REVALIDATE']
        assert saved['configuration']==evidence()['configuration']
        receipt=json.loads((root/'data/ops/releases/worker-justhodl-data-proxy.json').read_bytes())
        assert receipt['configuration']==evidence()['configuration'] and receipt['configuration_changes']==0
    with tempfile.TemporaryDirectory() as tmp:
        refuses(lambda:run_release(STRAY,STRAY,tmp))                      # the deploy did not remove it: no receipt
        assert not (Path(tmp)/'data').exists()


def test_worker_release_before_refuses_every_other_configuration_change():
    other={'name':'SOMETHING_ELSE','type':'plain_text','text':'1'}
    for live in (other,{'name':'JH_EDGE_REVALIDATE','type':'secret_text'}):
        with tempfile.TemporaryDirectory() as tmp:
            refuses(lambda:run_release(live,None,tmp))
            assert not (Path(tmp)/'evidence/before.json').exists()
    with tempfile.TemporaryDirectory() as tmp:                           # the leftover AND another change
        client=evidence_client(STRAY);client.values['/settings']['compatibility_date']='2026-01-01'
        import worker_release as cli
        root=Path(tmp);build=root/'build';build.mkdir();(build/'index.js').write_bytes(CODE)
        baseline=root/'baseline.json';baseline.write_text(json.dumps(evidence()),encoding='utf-8')
        retired=root/'tests/security/retired-secret-sha256.json';retired.parent.mkdir(parents=True)
        retired.write_bytes((ROOT/'tests/security/retired-secret-sha256.json').read_bytes())
        env={'CLOUDFLARE_ACCOUNT_ID':'a'*32,'CLOUDFLARE_API_TOKEN':'synthetic-token-for-tests'}
        with patch.object(cli,'ROOT',root),patch.object(cli,'BASELINE',baseline),patch.object(cli,'build_identity',return_value={'commit':'a'*40}),\
             patch.object(cli,'ControlPlane',lambda a,t:client),patch.dict(os.environ,env):
            refuses(lambda:cli.execute('before',build,root/'evidence','a'*40,'123'))


def test_worker_release_unchanged_live_configuration_needs_no_allowance():
    with tempfile.TemporaryDirectory() as tmp:
        result,root=run_release(None,None,tmp)
        assert result['status']=='matched'
        assert 'tolerated_stray_bindings' not in json.loads((root/'evidence/before.json').read_bytes())


def test_worker_release_workflow_pins_captures_verifies_and_retains_failures():
    workflow=(ROOT/'.github/workflows/deploy-workers.yml').read_text(encoding='utf-8')
    assert 'wrangler@4.144.0' in workflow and 'wrangler@latest' not in workflow
    assert workflow.index('wrangler deploy --dry-run') < workflow.index('tests/portfolio-publication-runtime.cjs') < workflow.index('worker_release.py before')
    assert workflow.index('worker_release.py before') < workflow.index('\n            wrangler deploy\n') < workflow.index('worker_release.py after')
    assert workflow.index('worker_release.py after') < workflow.index('aws s3 cp data/ops/releases/worker-')
    assert 'cancel-in-progress: false' in workflow and 'source-evidence-${{ github.sha }}' in workflow
    assert 'REQUESTED_WORKER: ${{ github.event.inputs.worker }}' in workflow
    assert 'targets="${{ github.event.inputs.worker }}"' not in workflow
    assert 'publish_worker_evidence.py --build-dir' in workflow and 'contents: write' in workflow
    assert workflow.index('publish_worker_evidence.py --build-dir') < workflow.index('aws s3 cp data/ops/releases/worker-')
    assert 'git pull --rebase' not in workflow


def test_worker_build_identity_rejects_untracked_and_ignored_source_inputs():
    import worker_release as cli
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp);src=root/cli.WORKER_PATH/'src';src.mkdir(parents=True)
        (src/'index.js').write_bytes(CODE);build=root/'build';build.mkdir();(build/'index.js').write_bytes(CODE)
        def git(*args):return subprocess.check_output(['git',*args],cwd=root,stderr=subprocess.DEVNULL)
        git('init');git('add','--',cli.WORKER_PATH)
        git('-c','user.name=Synthetic','-c','user.email=synthetic@example.invalid','commit','-m','Synthetic Worker fixture')
        commit=git('rev-parse','HEAD').decode().strip()
        with patch.object(cli,'ROOT',root),patch.object(cli,'TOOLS',[]):
            assert cli.build_identity(build,commit)['commit']==commit
            (src/'untracked.js').write_bytes(b'export default 1;')
            refuses(lambda:cli.build_identity(build,commit))
            (root/'.gitignore').write_text('**/untracked.js\n',encoding='utf-8')
            refuses(lambda:cli.build_identity(build,commit))


if __name__=='__main__':
    tests=[v for k,v in sorted(globals().items()) if k.startswith('test_') and callable(v)]
    for test in tests:test()
    print('Worker exact release evidence tests passed:',len(tests))
