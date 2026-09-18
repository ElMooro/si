"""Verify original-price evidence gate and refresh the full descriptive scorecard."""
import json
from pathlib import Path
import subprocess
import sys
import time
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared')]
from ops_report import report


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=920,retries={'max_attempts':0}))
    bucket='justhodl-dashboard-live'
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/outcome_integrity.py'],text=True).strip()
    def read(key):return json.loads(s3.get_object(Bucket=bucket,Key=key)['Body'].read())
    def invoke(fn,event):
        result=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=json.dumps(event).encode())
        response=json.loads(result['Payload'].read())
        assert not result.get('FunctionError') and response.get('statusCode',200)<400,fn+' invoke failed'
        return response
    with report('ops_5719_outcome_archive_gate_verify') as r:
        deadline=time.monotonic()+1500
        for fn in ('justhodl-signal-scorecard','justhodl-engine-trust','justhodl-outcome-checker'):
            while True:
                receipt=read('data/ops/releases/'+fn+'.json')
                if receipt['commit']==expected:break
                assert time.monotonic()<deadline,fn+' exact receipt unavailable'
                time.sleep(15)
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==receipt['code_sha256']
            r.log(fn+' exact release/runtime hash '+expected)
        # Regression fixtures are local, explicit synthetic examples; none enter S3 or the ledger.
        subprocess.run([sys.executable,str(ROOT/'aws/shared/tests/test_outcome_price_evidence.py')],check=True,capture_output=True)
        check=invoke('justhodl-outcome-checker',{'validation_only':True})
        assert check['validation_only'] is True and check['ledger_writes']==0
        invoke('justhodl-signal-scorecard',{'suppress_alerts':True})
        doc=read('data/signal-scorecard.json'); integrity=doc['integrity']
        assert doc['schema_version']=='2.3' and integrity['price_archive_verification_required'] is True
        assert integrity['price_evidence_contract']=='provider-price-replay.v1' and integrity['scan_complete'] is True
        assert doc['n_outcomes_scanned']>=202552 and doc['n_promoted']==0 and doc['alpha']['n_alpha_proven']==0
        assert all(row['performance_multiplier']==1 and row['sizing_eligible'] is False and row['promotion_eligible'] is False for row in doc['scorecard'])
        invoke('justhodl-engine-trust',{'suppress_alerts':True})
        r.kv(commit=expected,generated_at=doc['generated_at'],schema_version=doc['schema_version'],
             scanned=doc['n_outcomes_scanned'],scored=doc['n_outcomes_scored'],quarantined=doc['n_outcomes_quarantined'],
             price_archive_gate='required',archive_checks=integrity['price_evidence_checks'],
             ledger_mutations=0,sizing_authority=False,prospective_protocol='still required')
        r.ok('Exact releases, complete ledger scan, price-archive gate and neutral permissions verified; no synthetic observations published')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
