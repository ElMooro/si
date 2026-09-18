"""Runtime probe isolation and complete-body capture regression."""
import ast
from datetime import datetime,timezone,timedelta
import hashlib
import io
import json
from pathlib import Path
import sys
import urllib.parse
import urllib.request
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared/tests')]
from raw_capture_test_support import run
from test_raw_snapshot import load

def main():
    run('plumbing-panel')
    archive,store=load()
    path=Path(__file__).resolve().parents[1]/'source/lambda_function.py'
    nodes=[n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name in ('_capture_probe','lambda_handler')]
    env={'datetime':datetime,'timezone':timezone,'timedelta':timedelta,'json':json,'hashlib':hashlib,'urllib':urllib,
         'snapshot':archive.snapshot,'snapshot_receipt':archive.snapshot_receipt}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),env)
    raw=json.dumps({'data':[{'cusip':'FIXTURE'}],'padding':'x'*700000}).encode()
    with patch('urllib.request.urlopen',return_value=io.BytesIO(raw)):
        result=env['lambda_handler']({'snapshot_validation_only':True},None)
    assert result['statusCode']==200 and result['derived_data_writes']==0
    assert result['receipt']['bytes']==len(raw) and result['receipt']['sha256']==hashlib.sha256(raw).hexdigest()
    assert archive.read_snapshot(result['receipt']['key'])==raw
    assert all(key.startswith('data/raw/v2/') for key in store.docs)
    print('plumbing-panel: actual probe writes only a complete verified raw archive, no derived packet')

if __name__=='__main__':main()
