"""Verify exact releases, refresh adapters and replay their archived measurements."""
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'aws/ops'), str(ROOT / 'aws/shared'),
               str(ROOT / 'aws/lambdas/justhodl-canary-macro/source')]
from ops_report import report
from evidence_store import read_verified
from canary_measurements import csv_observations, diagnostics

BUCKET = 'justhodl-dashboard-live'
FUNCTIONS = ('justhodl-warm-bridge', 'justhodl-canary-macro', 'justhodl-fabrication-weekly',
             'justhodl-market-tape', 'justhodl-tradingview')


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=920, retries={'max_attempts': 0}))
    expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/shared/provenance.py'], text=True).strip()
    def read(key): return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())
    def invoke(fn):
        result = lam.invoke(FunctionName=fn, InvocationType='RequestResponse', Payload=b'{}')
        payload = json.loads(result['Payload'].read())
        assert not result.get('FunctionError'), fn+' runtime failed: '+str(payload.get('errorType'))
        assert payload.get('statusCode', 200) < 400, fn+' returned failure'
    with report('ops_5714_provenance_replay_verify') as r:
        deadline = time.monotonic()+1500
        for fn in FUNCTIONS:
            while True:
                try: receipt = read('data/ops/releases/'+fn+'.json')
                except ClientError as exc:
                    if exc.response['Error']['Code'] not in ('NoSuchKey', '404'): raise
                    receipt = {}
                if receipt.get('commit') == expected: break
                assert time.monotonic() < deadline, fn+' exact receipt not available'
                time.sleep(15)
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256'] == receipt['code_sha256']
            r.log(fn+' exact receipt and runtime hash: '+expected)
        started = datetime.now(timezone.utc).replace(microsecond=0)
        invoke('justhodl-warm-bridge')
        soma = read('data/soma-holdings.json')
        bls = read('data/bls-macro.json')
        bea = read('data/bea-gdp.json')
        for doc in (soma, bls, bea):
            assert doc['schema_version'] == '2.0' and datetime.fromisoformat(doc['generated_at']) >= started
            assert doc['sizing_eligible'] is False
        assert soma['reconciliation']['status'] == 'reconciled'
        assert soma['total']['unit'] == 'USD' and soma['total']['field'] == 'total'
        warehouse = json.loads(read_verified(s3, BUCKET, soma['total']['evidence']))
        original = max(warehouse['payload']['soma']['summary'], key=lambda row: row['asOfDate'])
        assert soma['total']['value'] == float(original['total'])
        assert soma['total']['as_of'] == original['asOfDate']
        cpi = bls['cpi_headline']
        assert cpi['source']['series_id'] == 'CUUR0000SA0' and 'Index' in cpi['unit']
        original = json.loads(read_verified(s3, BUCKET, cpi['evidence']))
        monthly = [row for row in original['data'] if row['period'] in {'M%02d' % m for m in range(1,13)}]
        latest = max(monthly, key=lambda row: (row['year'], row['period']))
        assert cpi['value'] == float(latest['value']) and cpi['as_of'] == latest['year']+'-'+latest['period']
        gdp = bea['real_gdp_qq_pct']
        original = json.loads(read_verified(s3, BUCKET, gdp['evidence']))
        latest = max((row for row in original['rows'] if row['LineNumber'] == '1'), key=lambda row: row['TimePeriod'])
        assert gdp['value'] == float(latest['DataValue'].replace(',', '')) and gdp['as_of'] == latest['TimePeriod']
        r.log('SOMA total, BLS CPI and BEA real GDP replayed from exact warehouse inputs; original-provider provenance remains separate')
        invoke('justhodl-canary-macro')
        canary = read('data/canary-macro.json')
        assert canary['schema_version'] == '2.0' and datetime.fromisoformat(canary['generated_at']) >= started
        assert canary['calls_eligible'] is False and canary['sizing_eligible'] is False
        traces, verified = set(), []
        for sid, row in canary.items():
            if not isinstance(row, dict) or not isinstance(row.get('source'), dict): continue
            if row.get('value') is None or row['source'].get('kind') != 'fred': continue
            raw = read_verified(s3, BUCKET, row['evidence'])
            observations = csv_observations(raw, sid)
            assert (row['as_of'], row['value']) == observations[0]
            assert row['source']['series_id'] == sid and row['field'] == sid
            assert row['trace_id'] not in traces and len(row['trace_id']) == 64
            assert row['confidence'] is None and row['quality']['replay_verified'] is False
            traces.add(row['trace_id']); verified.append(sid)
        assert {'ICSA', 'IC4WSA', 'WALCL', 'WRESBAL', 'SOFR', 'IORB'}.issubset(verified), 'core canary inputs unavailable'
        replay = diagnostics(canary, datetime.fromisoformat(canary['generated_at']))
        assert canary['flags'] == replay, 'diagnostic replay mismatch'
        r.log('Original FRED responses and canary diagnostics replayed for '+str(len(verified))+' series')
        invoke('justhodl-fabrication-weekly')
        audit = read('data/audit/fabrication-weekly.json')
        assert audit['schema_version'] == '2.0' and audit['coverage_contract'] == 'provenance-coverage.v2'
        assert audit['replay_verified_pct'] == 0 and audit['sizing_eligible'] is False
        invoke('justhodl-market-tape')
        tape = read('data/market-tape.json')
        dollar = next(row for row in tape['items'] if row['label'] == 'USD BROAD')
        assert dollar['quality']['max_observation_age_days'] == 11 and 'weekly' in dollar['quality']['basis']
        r.kv(commit=expected, soma_generated_at=soma['generated_at'], soma_observation=soma['total']['as_of'],
             cpi_value=cpi['value'], cpi_period=cpi['as_of'], bea_gdp=gdp['value'], bea_period=gdp['as_of'],
             canary_generated_at=canary['generated_at'], canary_replayed=len(verified),
             reserve_composition_proxy_pct=canary['flags']['reserve_composition_proxy_pct'],
             audit_structural_coverage_pct=audit['flagship_coverage_avg_pct'], audit_replay_verified_pct=audit['replay_verified_pct'])
        r.ok('Exact releases, complete source bodies, measurement identities and diagnostics verified')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
