"""Migrate fiscal originals, prove merge survival and verify exact live consumers."""
import gzip
import json
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/shared'), str(ROOT/'scripts')]
from ops_report import report
from treasury_fiscal_model import CONTRACT, DATASETS
from replay_treasury_fiscal import replay

FUNCTIONS = ('treasury-fiscal-full', 'backfill-orchestrator', 'warm-bridge', 'plumbing-aggregator', 'symdir')


def main():
    bucket = 'justhodl-dashboard-live'
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=920, retries={'max_attempts': 0}))
    expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/ops/pending/ops_5729_treasury_dimensions_reverify.py'], text=True).strip()
    def raw(key):
        body = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
        return gzip.decompress(body) if key.endswith('.gz') else body
    def read(key): return json.loads(raw(key))
    def invoke(short, event=None):
        response = lam.invoke(FunctionName='justhodl-'+short, InvocationType='RequestResponse',
                              Payload=json.dumps({'suppress_alerts': True, **(event or {})}).encode())
        result = json.loads(response['Payload'].read())
        assert not response.get('FunctionError'), (short, result)
        return result
    with report('ops_5729_treasury_dimensions_reverify') as r:
        deadline = time.monotonic()+1800
        for short in FUNCTIONS:
            fn = 'justhodl-'+short
            while True:
                try: receipt = read('data/ops/releases/'+fn+'.json')
                except ClientError as exc:
                    if exc.response['Error']['Code'] not in ('404','NoSuchKey'): raise
                    receipt = {}
                if receipt.get('commit') == expected: break
                assert time.monotonic() < deadline, 'missing exact release '+fn
                time.sleep(15)
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256'] == receipt['code_sha256']
            r.log(fn+' exact release/runtime matched')
        result = invoke('treasury-fiscal-full')
        assert result.get('statusCode') == 200, result
        first = read('data/warm/treasury/tga_operating_cash.json.gz')
        first_count = first['n_records']
        result = invoke('backfill-orchestrator', {'dataset': 'tga_operating_cash'})
        assert result.get('statusCode') == 200, result
        deep = read('data/warm/treasury/tga_operating_cash.json.gz')
        assert deep['n_records'] >= first_count
        result = invoke('treasury-fiscal-full')
        assert result.get('statusCode') == 200, result
        refreshed = read('data/warm/treasury/tga_operating_cash.json.gz')
        assert refreshed['n_records'] >= deep['n_records'], 'daily refresh erased backfill'
        audits = {}
        for ds in DATASETS:
            warehouse = read('data/warm/treasury/'+ds+'.json.gz')
            assert warehouse['contract'] == CONTRACT and warehouse['legacy_rows_promoted'] == 0
            manifest = read(warehouse['replay']['manifest_key'])
            reproduced = replay(manifest, read=raw)
            assert reproduced['n_records'] == warehouse['n_records']
            assert all(warehouse[k] == v for k, v in reproduced.items()), 'live warehouse differs from replay'
            audits[ds] = {'records': warehouse['n_records'], 'original_sources': len(warehouse['sources']),
                          'date': warehouse['latest']['date'], 'current_decimal': warehouse['latest']['value_decimal'],
                          'reconciliation': warehouse['latest']['reconciliation'], 'replay': warehouse['replay']}
            r.log(ds+' original-row replay passed: '+str(warehouse['n_records'])+' rows')
        assert audits['debt_to_penny']['reconciliation']['status'] == 'reconciled'
        assert audits['tga_operating_cash']['reconciliation']['status'] == 'within_reported_rounding'
        for ds in ('rates_of_exchange','avg_interest_rates','interest_expense'):
            assert audits[ds]['current_decimal'] is None, 'mixed dataset acquired an arbitrary scalar'
        result = invoke('warm-bridge', {'feed': 'treasury'})
        assert result.get('statusCode') == 200 and json.loads(result['body'])['ok'], result
        hot = read('data/treasury-fiscal.json')
        assert len([ds for ds in DATASETS if hot[ds].get('dimension_contract') == CONTRACT]) == 6
        assert hot['debt_outstanding']['freshness']['cadence'] == 'fiscal_annual'
        assert hot['tga_operating_cash']['value_decimal'] == audits['tga_operating_cash']['current_decimal']
        # Normal production read path, including cache validation. A mixed FX
        # dataset must return an explicit selection error rather than a line.
        chart = invoke('symdir', {'mode': 'series', 'id': 'treasury:debt_to_penny'})
        assert chart.get('statusCode') == 200, chart
        chart_body = json.loads(chart['body'])
        assert chart_body['dimension_contract'] == CONTRACT and chart_body['measurement_evidence']
        mixed = invoke('symdir', {'mode': 'series', 'id': 'treasury:rates_of_exchange'})
        assert mixed.get('statusCode') == 500 and 'explicit Treasury' in json.loads(mixed['body']).get('error',''), mixed
        result = invoke('plumbing-aggregator')
        assert result.get('statusCode') == 200, result
        plumbing = read('data/plumbing-stress.json')['raw_indicators']['TGA_DAILY']
        assert plumbing['unit'] == 'USD_millions' and plumbing['date'] == audits['tga_operating_cash']['date']
        assert str(plumbing['value_decimal']) == audits['tga_operating_cash']['current_decimal']
        proof = {'contract':'treasury-fiscal-verification.v1', 'commit':expected,
                 'generated_at':datetime.now(timezone.utc).isoformat(), 'datasets':audits,
                 'tga_history_before_backfill':first_count, 'tga_history_after_backfill':deep['n_records'],
                 'tga_history_after_daily_refresh':refreshed['n_records'], 'exact_runtimes':list(FUNCTIONS),
                 'chart_series_verified':True, 'ambiguous_chart_withheld':True,
                 'legacy_history_preserved_as_evidence':True, 'publication_time_verified':False,
                 'notifications_sent':0, 'paid_api_calls':0, 'sizing_eligible':False}
        s3.put_object(Bucket=bucket, Key='data/treasury-fiscal-verification.json', Body=json.dumps(proof).encode(),
                      ContentType='application/json', CacheControl='no-cache')
        r.kv(commit=expected, generated_at=proof['generated_at'], datasets=audits,
             tga_history=[first_count,deep['n_records'],refreshed['n_records']], exact_runtimes=5,
             normal_chart_read_verified=True, ambiguous_chart_withheld=True, notifications_sent=0, paid_api_calls=0)
        r.ok('Original provider rows reproduce all six datasets; deep history survives daily refresh; exact chart, bridge and plumbing consumers verified')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
