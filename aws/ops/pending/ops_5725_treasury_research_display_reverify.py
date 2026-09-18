"""Verify the exact Treasury desk release and its non-actionable research contract."""
import json
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/ops'))
from ops_report import report


def main():
    bucket='justhodl-dashboard-live';fn='justhodl-auction-desk'
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+fn+'/source/lambda_function.py'],text=True).strip()
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=920,retries={'max_attempts':0}))
    def read(key):return json.loads(s3.get_object(Bucket=bucket,Key=key)['Body'].read())
    with report('ops_5725_treasury_research_display_reverify') as r:
        deadline=time.monotonic()+1500
        while True:
            try:receipt=read('data/ops/releases/'+fn+'.json')
            except ClientError as exc:
                if exc.response['Error']['Code'] not in ('404','NoSuchKey'):raise
                receipt={}
            if receipt.get('commit')==expected:break
            assert time.monotonic()<deadline,'exact release receipt missing'
            time.sleep(15)
        config=lam.get_function_configuration(FunctionName=fn)
        assert config['CodeSha256']==receipt['code_sha256'],'runtime hash differs'
        started=datetime.now(timezone.utc)
        response=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
        payload=json.loads(response['Payload'].read())
        assert not response.get('FunctionError') and payload.get('ok'), 'desk invoke failed'
        doc=read('data/auction-desk.json')
        assert doc['version']=='1.4.1' and datetime.fromisoformat(doc['generated_at'].replace('Z','+00:00'))>=started
        assert doc['chart_cohort_contract']=='treasury-comparable-chart.v1' and doc['chart_cohorts']
        for label,rows in doc['chart_cohorts'].items():
            assert len({(row['instrument_kind'],row['quote_basis'],row['reopening']) for row in rows})==1,label
        for row in doc['auctions']:
            if row['instrument_kind'] in ('TIPS','FRN','UNKNOWN'):assert row['tail_bp'] is None
        reactions=doc['reactions']
        assert reactions['contract']=='treasury-reaction-research.v1'
        assert reactions['partial_windows_excluded_from_distributions'] is True
        assert reactions['price_lineage_verified'] is False and reactions['sizing_eligible'] is False
        assert reactions['same_day_clock']=='PRIOR_CLOSE_TO_AUCTION_DAY_CLOSE'
        for row in reactions['prediction']:
            assert row['call'] is None and row['confidence']=='unvalidated'
            assert row['decision_eligible'] is False and row['sizing_eligible'] is False
        assert doc['decision']['call'] is None and doc['decision']['sizing_eligible'] is False
        note=doc['today'].get('ai_note')
        if note:assert note['paid_api_calls']==0 and note['generation_method']=='deterministic_treasury_v1'
        r.kv(commit=expected,code_sha256=receipt['code_sha256'],generated_at=doc['generated_at'],
             auction_rows=len(doc['auctions']),comparable_chart_cohorts=len(doc['chart_cohorts']),
             retained_investigative_prediction_rows=len(reactions['prediction']),
             historical_price_lineage_verified=False,sizing_authority=False,
             newest_auction=doc['freshness']['newest_auction'])
        r.ok('Exact runtime, fresh public packet, comparable charts, descriptive-only reactions and no paid explanation verified')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
