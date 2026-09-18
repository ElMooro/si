"""Invoke and verify the deployed Treasury comparability contract without paid AI."""
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

FN = 'justhodl-auction-desk'
BUCKET = 'justhodl-dashboard-live'


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=900, retries={'max_attempts': 0}))
    expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--',
        'aws/lambdas/' + FN + '/source/lambda_function.py'], text=True).strip()
    def read(key):
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())
    with report('ops_5711_treasury_contract_verify') as r:
        deadline = time.monotonic() + 900
        while True:
            receipt = read('data/ops/releases/' + FN + '.json')
            if receipt.get('commit') == expected:
                break
            assert time.monotonic() < deadline, 'Exact-commit deployment receipt missing'
            time.sleep(15)
        assert lam.get_function_configuration(FunctionName=FN)['CodeSha256'] == receipt['code_sha256']
        started = datetime.now(timezone.utc)
        result = lam.invoke(FunctionName=FN, InvocationType='RequestResponse',
            Payload=json.dumps({'backfill': True, 'suppress_alerts': True}).encode())
        payload = json.loads(result['Payload'].read())
        assert not result.get('FunctionError') and payload.get('ok') is True, 'Treasury refresh failed'
        doc = read('data/auction-desk.json')
        assert doc['version'] == '1.3.0'
        assert datetime.fromisoformat(doc['generated_at'].replace('Z', '+00:00')) >= started.replace(microsecond=0)
        examples = [a for a in doc['auctions'] if a.get('cusip') == '91282CRE3' and a.get('auction_date') == '2026-09-17']
        assert examples, 'Known TIPS regression fixture absent from live packet'
        assert all(a['instrument_kind'] == 'TIPS' and a['tail_bp'] is None and a['par_prev_close'] is None for a in examples)
        assert all(a.get('wi_tail_bp') is None for a in doc['auctions'])
        assert all(a['score_parts'][-1]['weight'] == 0 for a in doc['auctions'])
        assert all(not b['monetary_easing_inferred'] and b['implication']['tone'] == 'neutral'
                   for b in doc['buybacks']['operations'])
        note = doc['today']['ai_note']
        assert note['paid_api_calls'] == 0 and note['generation_method'] == 'deterministic_treasury_v1'
        assert doc['decision']['sizing_eligible'] is False
        assert 'easing impulse' not in json.dumps(doc).lower()
        r.kv(function=FN, commit=expected, generated_at=doc['generated_at'], version=doc['version'],
             tips_comparison=None, nominal_tail_votes=0, paid_api_calls=0,
             verified_instruments=sum(a.get('instrument_classification_status') == 'verified' for a in doc['auctions']),
             auctions=len(doc['auctions']), sizing_eligible=False)
        r.ok('Exact receipt and live code hash verified; refreshed TIPS comparison, score exclusions, buyback semantics and provider-free note passed')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
