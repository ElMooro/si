"""Verify exact performance releases and refresh research permissions; preserve the ledger."""
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report

FUNCTIONS = ('justhodl-signal-scorecard', 'justhodl-engine-trust', 'justhodl-signal-harvester', 'justhodl-outcome-checker')
BUCKET = 'justhodl-dashboard-live'


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=900, retries={'max_attempts': 0}))
    expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--',
        'aws/lambdas/justhodl-signal-scorecard/source/lambda_function.py'], text=True).strip()
    def read(key):
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())
    def invoke(fn, event):
        result = lam.invoke(FunctionName=fn, InvocationType='RequestResponse', Payload=json.dumps(event).encode())
        payload = json.loads(result['Payload'].read())
        assert not result.get('FunctionError'), fn + ' invocation failed'
        body = json.loads(payload['body']) if isinstance(payload.get('body'), str) else payload
        assert body.get('ok') is True, fn + ' did not pass its verification contract'
        return body
    with report('ops_5712_performance_integrity_verify') as r:
        deadline = time.monotonic() + 1200
        for fn in FUNCTIONS:
            while True:
                receipt = read('data/ops/releases/' + fn + '.json')
                if receipt.get('commit') == expected:
                    break
                assert time.monotonic() < deadline, fn + ' exact release receipt missing'
                time.sleep(15)
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256'] == receipt['code_sha256']
            r.log(fn + ' exact receipt and live code hash verified: ' + expected)
        # Read-only production runtime checks; no harvesting/regrading of ledger rows.
        harvester = invoke('justhodl-signal-harvester', {'validation_only': True, 'probe_prices': True})
        checker = invoke('justhodl-outcome-checker', {'validation_only': True})
        assert harvester['ledger_writes'] == checker['ledger_writes'] == 0
        assert harvester['token_identity']['instrument_id'] != harvester['fund_identity']['instrument_id']
        started = datetime.now(timezone.utc).replace(microsecond=0)
        invoke('justhodl-signal-scorecard', {'suppress_alerts': True})
        score = read('data/signal-scorecard.json')
        assert score['schema_version'] == '2.2' and score['integrity']['scan_complete'] is True
        assert datetime.fromisoformat(score['generated_at']) >= started
        assert score['n_outcomes_scanned'] > 100000
        assert score['n_outcomes_quarantined'] > 0 and score['n_promoted'] == score['alpha']['n_alpha_proven'] == 0
        assert score['ssm_ok'] is True
        assert all(row['performance_multiplier'] == 1 and row['sizing_eligible'] is False for row in score['scorecard'])
        affected = {row['signal_type']: row for row in score['scorecard'] if row['signal_type'] in
                    ('eng:crypto-emergence', 'eng:compass-decisive-call', 'eng:dollar-decisive-call')}
        assert len(affected) == 3 and all(row['n_quarantined'] > 0 and row['n_scored'] == 0 for row in affected.values())
        invoke('justhodl-engine-trust', {'suppress_alerts': True})
        trust = read('data/engine-trust.json')
        assert trust['version'] == '1.1.0' and trust['source_generated_at'] == score['generated_at']
        assert trust['integrity']['scorecard_contract_verified'] is True
        assert all(row['effective_trust'] == 1 and row['sizing_eligible'] is False for row in trust['engines'])
        r.kv(commit=expected, generated_at=score['generated_at'], outcomes_scanned=score['n_outcomes_scanned'],
             outcomes_quarantined=score['n_outcomes_quarantined'], outcomes_verified=score['n_outcomes_scored'],
             promoted=score['n_promoted'], sizing_eligible=False, ledger_records_modified=0)
        r.log('Distinct identified quote contexts: ' + json.dumps(harvester['quote_context'], sort_keys=True))
        r.ok('Exact releases, real pricing routes, complete scan, quarantine counts, SSM publication and downstream permissions verified')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
