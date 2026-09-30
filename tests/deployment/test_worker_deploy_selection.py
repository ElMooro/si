"""Run the actual workflow selector against complete local invented Git trees."""
from pathlib import Path
import os
import shutil
import subprocess
import tempfile
import textwrap

ROOT=Path(__file__).resolve().parents[2]


def script():
    text=(ROOT/'.github/workflows/deploy-workers.yml').read_text(encoding='utf-8')
    block=text.split('      - name: Detect changed Workers\n',1)[1].split('      - name: Fetch AI_CHAT_TOKEN',1)[0]
    return textwrap.dedent(block.split('        run: |\n',1)[1])


class Repository:
    def __init__(self,root):
        self.root=Path(root)
        self.git('init');self.git('config','core.autocrlf','false')
        for name in ('justhodl-data-proxy','justhodl-other'):
            self.write('cloudflare/workers/'+name+'/wrangler.toml','name="'+name+'"\n')
            self.write('cloudflare/workers/'+name+'/src/index.js','export default {};\n')
        self.base=self.commit('Complete invented baseline')
    def git(self,*args):return subprocess.check_output(['git',*args],cwd=self.root,stderr=subprocess.DEVNULL).decode().strip()
    def write(self,name,text):
        path=self.root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(text,encoding='utf-8',newline='\n')
    def commit(self,message):
        self.git('add','.');self.git('-c','user.name=Synthetic','-c','user.email=synthetic@example.invalid','commit','-m',message)
        return self.git('rev-parse','HEAD')
    def select(self,paths=(),before=None,requested=''):
        for name in paths:self.write(name,'Complete invented modified file\n')
        head=self.commit('Complete invented change') if paths else self.git('rev-parse','HEAD')
        self.write('detect.sh',script())
        target=self.root/'result.out'
        if target.exists():target.unlink()
        env=os.environ.copy();env.update(REQUESTED_WORKER=requested,BEFORE_SHA=self.base if before is None else before,GITHUB_SHA=head,GITHUB_OUTPUT='result.out')
        bash=shutil.which('bash');assert bash,'Actual workflow Bash runtime required'
        result=subprocess.run([bash,'detect.sh'],cwd=self.root,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        output=target.read_text(encoding='utf-8').strip() if target.exists() else None
        return result,output


def test_worker_test_only_push_does_not_redeploy_any_code():
    for name in ('tests/worker-snapshot-ordering.test.js','tests/reviewed-artifacts.test.js','tests/deployment/test_worker_release_evidence.py'):
        with tempfile.TemporaryDirectory() as root:
            result,output=Repository(root).select([name]);assert result.returncode==0,result.stderr;assert output=='targets='


def test_worker_tools_select_proxy_and_keep_other_changed_workers():
    for tool in ('.github/workflows/deploy-workers.yml','scripts/worker_release.py','scripts/publish_worker_evidence.py','aws/ops/checks/worker_source_evidence.py','aws/ops/checks/worker_release_evidence.py'):
        with tempfile.TemporaryDirectory() as root:
            result,output=Repository(root).select([tool,'cloudflare/workers/justhodl-other/src/index.js'])
            assert result.returncode==0,result.stderr;assert output=='targets=justhodl-data-proxy justhodl-other'


def test_worker_selector_uses_every_commit_in_the_actual_push_range():
    with tempfile.TemporaryDirectory() as root:
        repo=Repository(root);repo.write('cloudflare/workers/justhodl-other/src/index.js','export const first = true;\n');repo.commit('First source change')
        result,output=repo.select(['tests/worker-snapshot-ordering.test.js'])
        assert result.returncode==0,result.stderr;assert output=='targets=justhodl-other'


def test_worker_selector_refuses_missing_or_unrelated_range_without_guessing():
    for before in ('','0'*40,'f'*40):
        with tempfile.TemporaryDirectory() as root:
            result,output=Repository(root).select(before=before);assert result.returncode!=0 and output is None
    with tempfile.TemporaryDirectory() as root:
        repo=Repository(root);base=repo.base;repo.write('elsewhere.txt','Invented unrelated descendant\n');future=repo.commit('Unrelated tip')
        repo.git('checkout','--detach',base)
        result,output=repo.select(before=future);assert result.returncode!=0 and output is None


def test_worker_manual_target_is_exact_and_validated():
    for requested,success in [('justhodl-other',True),('justhodl-missing',False),('../justhodl-data-proxy',False),('justhodl-other justhodl-data-proxy',False)]:
        with tempfile.TemporaryDirectory() as root:
            result,output=Repository(root).select(before='',requested=requested)
            assert (result.returncode==0)==success
            assert output==('targets=justhodl-other' if success else None)


def test_worker_tool_selection_survives_a_complete_large_push_range():
    with tempfile.TemporaryDirectory() as root:
        paths=['.github/workflows/deploy-workers.yml']+[f'docs/invented-history/source-{i:04d}-complete-test-only-record.txt' for i in range(2048)]
        result,output=Repository(root).select(paths)
        assert result.returncode==0,result.stderr
        assert output=='targets=justhodl-data-proxy',output


if __name__=='__main__':
    tests=[v for k,v in list(globals().items()) if k.startswith('test_') and callable(v)]
    for test in tests:test()
    print('Worker deployment selection tests:',len(tests),'passed')
