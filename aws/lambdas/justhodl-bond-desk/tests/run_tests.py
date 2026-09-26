from pathlib import Path
import sys
import subprocess
sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
from funding_consumer_test_support import run
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'source'))
from required_region_composite import required_region_composite
if __name__=='__main__':
    run('justhodl-bond-desk')
    regions={'us':{'fresh':True,'score':0},'other':{'fresh':True,'score':100}};weights={'us':.4,'other':.6}
    assert required_region_composite(regions,weights)[0]==60
    for value in (None,float('nan'),False):
        regions['us']['score']=value
        assert required_region_composite(regions,weights)==(None,'UNAVAILABLE',None)
    regions['us']={'fresh':False,'score':0}
    assert required_region_composite(regions,weights)==(None,'UNAVAILABLE',None)
    regions['us']={'fresh':True,'score':0,'calls_eligible':False}
    assert required_region_composite(regions,weights)==(None,'UNAVAILABLE',None)
    print('Required regional votes: no partial reweighting or empty max passed')
    subprocess.run([sys.executable,str(Path(__file__).with_name('test_cohort_native.py'))],check=True)
    subprocess.run([sys.executable,str(Path(__file__).with_name('test_credit_native.py'))],check=True)
