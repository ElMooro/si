"""Standalone replay commands must locate the actual producer without caller imports."""
from pathlib import Path
import os,re,subprocess,sys
ROOT=Path(__file__).resolve().parents[2]

def test_declared_native_replay_source_directories_exist():
    for path in (ROOT/'scripts').glob('replay*_research.py'):
        for source in re.findall(r'aws/lambdas/[a-z0-9-]+/source',path.read_text(encoding='utf-8')):
            assert (ROOT/source).is_dir(),(path.name,source)

def test_vrp_and_valuation_entrypoints_start_without_preloaded_producer_modules():
    env=os.environ.copy();env.pop('PYTHONPATH',None);env['AWS_EC2_METADATA_DISABLED']='true'
    for script in ('replay_vrp_research.py','replay_valuation_research.py'):
        p=subprocess.run([sys.executable,str(ROOT/'scripts'/script),'--help'],cwd=ROOT,env=env,capture_output=True,text=True,timeout=20)
        assert p.returncode==0,(script,p.stderr)
        assert '--packet' in p.stdout
