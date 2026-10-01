"""Offline invented fixture through both actual producers; no cloud client available."""
import json,runpy,sys,types
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-khalid/source')]
class NoCloud:
    def __getattr__(self,name):raise AssertionError('Unexpected cloud access')
sys.modules['boto3']=types.SimpleNamespace(client=lambda *a,**kw:NoCloud())
scope=runpy.run_path(str(ROOT/'aws/lambdas/justhodl-khalid/tests/test_risk_diagnostics.py'))
print(json.dumps(scope['packet'](),allow_nan=False))
