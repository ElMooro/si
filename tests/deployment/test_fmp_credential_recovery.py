import json
from pathlib import Path
import runpy

VALIDATE=runpy.run_path(str(Path(__file__).resolve().parents[2]/'aws/ops/checks/audit_20260909_fmp_credentials.py'))['validate']


def test_fmp_credential_requires_valid_requested_quote_and_never_reports_secret():
    for price in (None,False,0,-1,float('nan')):
        result=VALIDATE(lambda *args:([{'symbol':'AAPL','price':price}],{}),'SYNTHETIC_SECRET')
        assert result['valid'] is False
    result=VALIDATE(lambda *args:([{'symbol':'AAPL','price':123}],{}),'SYNTHETIC_SECRET')
    assert result['valid'] is True and 'SYNTHETIC_SECRET' not in json.dumps(result)


def test_fmp_missing_or_rejected_credential_does_not_claim_recovery():
    assert VALIDATE(lambda *args:(_ for _ in ()).throw(AssertionError('must not fetch')),'')['status']=='ABSENT'
    result=VALIDATE(lambda *args:([],{'http_status':401,'body':'SYNTHETIC_SECRET'}),'SYNTHETIC_SECRET')
    assert result['valid'] is False and result['http_status']==401
    assert 'SYNTHETIC_SECRET' not in json.dumps(result)
