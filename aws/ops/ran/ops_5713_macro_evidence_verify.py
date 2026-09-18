"""Refresh the macro chain and replay published growth from archived AWS evidence."""
import hashlib
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
sys.path.insert(0, str(ROOT / 'aws/ops'))
sys.path.insert(0, str(ROOT / 'aws/shared'))
from ops_report import report
from evidence_store import read_verified
from macro_observations import CURATED_YOY, calendar_yoy, valid_yoy_row

BUCKET = 'justhodl-dashboard-live'
FUNCTIONS = ('justhodl-tradingview', 'justhodl-indicator-bus', 'justhodl-market-tape')


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=920, retries={'max_attempts': 0}))
    expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--',
        'aws/lambdas/justhodl-tradingview/source/lambda_function.py'], text=True).strip()
    def read(key):
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())
    def invoke(fn, event):
        result = lam.invoke(FunctionName=fn, InvocationType='RequestResponse', Payload=json.dumps(event).encode())
        payload = json.loads(result['Payload'].read())
        assert not result.get('FunctionError'), fn + ' runtime failed: ' + str(payload.get('errorType'))
        assert payload.get('statusCode', 200) < 400 and payload.get('ok', True) is not False, fn + ' returned failure'
        return payload
    with report('ops_5713_macro_evidence_verify') as r:
        deadline = time.monotonic() + 1500
        for fn in FUNCTIONS:
            while True:
                try:
                    receipt = read('data/ops/releases/' + fn + '.json')
                except ClientError as exc:
                    if exc.response['Error']['Code'] not in ('NoSuchKey', '404'): raise
                    receipt = {}
                if receipt.get('commit') == expected: break
                assert time.monotonic() < deadline, fn + ' exact receipt not available'
                time.sleep(15)
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256'] == receipt['code_sha256']
            r.log(fn + ' exact receipt and live code hash verified: ' + expected)
        started = datetime.now(timezone.utc).replace(microsecond=0)
        invoke('justhodl-tradingview', {'force': False, 'suppress_alerts': True})
        vault = read('data/tradingview.json')
        assert 'v3.31.0' in vault['marker']
        assert datetime.fromisoformat(vault['generated_at']) >= started
        rows = {row['symbol']: row for row in vault['symbols']}
        verified = []
        for symbol, sid in CURATED_YOY.items():
            row = rows[symbol]
            assert row['series_id'] == sid and row['contract_version'] == 'fred-calendar-yoy.v1'
            if row.get('value') is None: continue
            observations = json.loads(read_verified(s3, BUCKET, row['evidence']['observations']))
            definition = json.loads(read_verified(s3, BUCKET, row['evidence']['definition']))['seriess'][0]
            replay = calendar_yoy(observations['observations'], definition, datetime.fromisoformat(row['calculated_at']))
            assert row['value'] == replay['value'] and row['comparison_date'] == replay['comparison_date']
            assert row['frequency'] == replay['frequency'] and row['source_unit'] == replay['source_unit']
            assert all('api_key' not in p['source_url'] and 'apikey' not in p['source_url'] for p in row['evidence'].values())
            verified.append(symbol)
        for symbol in ('USIRYY', 'USGDPYY', 'JPGDPYY'):
            assert valid_yoy_row(rows[symbol], symbol), symbol + ' current archived observation unavailable'
        r.log('Exact source bytes replayed for: ' + ', '.join(verified))
        invoke('justhodl-indicator-bus', {})
        bus = read('data/indicator-bus.json')
        assert bus['schema_version'] == '1.1' and bus['source_generated_at'] == vault['generated_at']
        for symbol in CURATED_YOY:
            value = bus['indicators'].get(symbol)
            if value: assert valid_yoy_row(value, symbol)
            else: assert symbol in bus['gaps']
        invoke('justhodl-market-tape', {})
        tape = read('data/market-tape.json')
        assert tape['schema_version'] == '2.0'
        assert datetime.fromisoformat(tape['generated_at']) >= started
        by_label = {item['label']: item for item in tape['items']}
        assert 'NDX' not in by_label and 'DXY' not in by_label and 'CN GDP' not in by_label
        assert by_label['COMP']['provider_symbol'] == '^IXIC'
        assert by_label['USD BROAD']['series_id'] == 'DTWEXBGS'
        assert by_label['US CPI SA YoY']['value'] == rows['USIRYY']['value']
        proof_count = 0
        for item in tape['items']:
            assert item['observation_date'] and item['unit'] and item['sizing_eligible'] is False
            for proof in item['evidence'].values():
                read_verified(s3, BUCKET, proof)
                proof_count += 1
        r.kv(commit=expected, vault_generated_at=vault['generated_at'], bus_generated_at=bus['generated_at'],
             tape_generated_at=tape['generated_at'], macro_series_replayed=len(verified), tape_evidence_verified=proof_count,
             cpi_yoy=rows['USIRYY']['value'], cpi_observation=rows['USIRYY']['observation_date'],
             us_real_gdp_yoy=rows['USGDPYY']['value'], gdp_comparison=rows['USGDPYY']['comparison_date'],
             china_status=rows['CNGDPYY']['status'], china_observation=rows['CNGDPYY'].get('observation_date'))
        r.log('Unavailable ticker inputs: ' + json.dumps(tape['gaps'], sort_keys=True))
        r.ok('Exact releases, source replay, calendar comparisons, bus preservation and ticker identities verified')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
