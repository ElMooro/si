"""Accept the native canonical 13F cutover without collecting or invoking consumers."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import io
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zipfile

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/shared'), str(ROOT/'scripts'),
               str(ROOT/'aws/lambdas/justhodl-13f-positions/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
import holdings_canonical as canonical
import holdings_store as store
from replay_holdings_canonical import verify_current
from replay_fred_vintage import read_public


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method='HEAD',
                headers={'User-Agent': 'justhodl-verify-release/1.0'}), timeout=30) as response:
            return response.status in (401, 403, 404)
    except urllib.error.HTTPError as exc:
        return exc.code in (401, 403, 404)


def main():
    fn, bucket = 'justhodl-13f-positions', 'justhodl-dashboard-live'
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1',
                      config=Config(read_timeout=240, tcp_keepalive=True, retries={'max_attempts': 0}))
    read = store.reader(s3, bucket)
    with report('ops_5882_canonical_holdings_acceptance') as r:
        expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/'+fn], text=True).strip()
        deadline = time.monotonic() + 2400
        while True:
            receipt = json.loads(read('data/ops/releases/'+fn+'.json'))
            if receipt.get('commit') == expected: break
            assert time.monotonic() < deadline, 'Exact runtime receipt wait expired'
            time.sleep(15)
        conf = lam.get_function_configuration(FunctionName=fn)
        assert conf['CodeSha256'] == receipt['code_sha256'] and conf['LastUpdateStatus'] == 'Successful'
        location = lam.get_function(FunctionName=fn)['Code']['Location']
        with urllib.request.urlopen(location, timeout=45) as response: archive = response.read(32*1024*1024+1)
        assert len(archive) <= 32*1024*1024
        with zipfile.ZipFile(io.BytesIO(archive)) as z:
            for path in (ROOT/'aws/lambdas'/fn/'source').glob('*.py'):
                assert z.read(path.name) == path.read_bytes(), 'Actual runtime source differs: '+path.name
        policy = json.loads(s3.get_bucket_policy(Bucket=bucket)['Policy'])
        deny = next(v for v in policy['Statement'] if v.get('Sid') == 'Audit20260909ImmutableOriginalBackups')
        assert deny['Effect'] == 'Deny' and deny['Principal'] == '*'
        assert deny['Condition'] == {'StringNotEquals': {'aws:PrincipalAccount': '857687956942'}}
        r.kv(commit=expected, code_sha256=conf['CodeSha256'], action='holdings_canonical_refresh',
             source_collection_requested=False, consumers_invoked=False, notifications_requested=False)
        preserved_before = {}
        for key in canonical.KEYS:
            body = read(key); sha = hashlib.sha256(body).hexdigest()
            store.immutable(s3, bucket, store.PRIVATE+sha+'.bin', body, 'application/octet-stream')
            assert denied('https://'+bucket+'.s3.amazonaws.com/'+store.PRIVATE+sha+'.bin')
            assert denied('https://justhodl.ai/'+store.PRIVATE+sha+'.bin')
            preserved_before[key] = {'sha256': sha, 'bytes': len(body)}
        source_before = hashlib.sha256(read(canonical.model.CURRENT)).hexdigest()
        response, rejected = invoke_when_available(lam, dict(FunctionName=fn, InvocationType='RequestResponse',
            Payload=b'{"action":"holdings_canonical_refresh","notify":false}'), wait_seconds=180)
        result = json.loads(response['Payload'].read())
        assert not response.get('FunctionError'), 'Canonical runtime failed; inspect execution before retry'
        assert result.get('statusCode') == 200
        result = json.loads(result['body'])
        assert result['published'] and result['source_collection'] is None
        assert all(result[k] == 0 for k in ('paid_ai_calls', 'notifications_sent', 'private_account_reads', 'portfolio_writes'))
        assert hashlib.sha256(read(canonical.model.CURRENT)).hexdigest() == source_before, 'Research changed during controlled acceptance'
        r.kv(invocation=result, throttle_rejections_before_execution=rejected)
        # Public, complete replay; immutable cache never substitutes a mutable key.
        cache, inspected = {}, set()
        def public(key):
            inspected.add(key)
            immutable = bool(re.search(r'/[a-f0-9]{64}\.(?:json|py|bin\.gz)$', key))
            if immutable and key in cache: return cache[key]
            raw = read_public(key)
            if immutable: cache[key] = raw
            return raw
        manifest, products = verify_current(public)
        packet = json.loads(public(canonical.CURRENT))
        assert packet['canonical_replay'] == result['canonical_replay']
        for key, product in products.items():
            alias = json.loads(public(key))
            assert alias.pop('canonical_replay') == result['canonical_replay']
            assert alias == product and alias['calls_eligible'] is False and alias['call'] is None
            assert alias['transaction_inference']['net_capital_flow_usd'] is None
        assert set(products) == set(canonical.KEYS)
        assert packet['funds_total'] == 18 and packet['manager_comparison_count'] >= 45000
        detail = store.verified(packet['by_fund']['BERKSHIRE']['positions_ref'], public, 'funds')
        ko = next(v for v in detail['comparison']['rows'] if v['identity']['cusip'] == '191216100')
        assert ko['status'] == 'reported_quantity_unchanged' and ko['reported_quantity_change'] == '0'
        assert ko['reported_value_change_usd'] == '2088000000' and ko['inferred_purchase_usd'] is None
        assert lam.get_function_configuration(FunctionName=fn)['CodeSha256'] == conf['CodeSha256']
        assert json.loads(read('data/ops/releases/'+fn+'.json'))['commit'] == expected
        proof = {'contract': 'holdings-canonical-verification.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
                 'commit': expected, 'code_sha256': conf['CodeSha256'], 'canonical_replay': result['canonical_replay'],
                 'research': manifest['research'], 'research_generated_at': packet['generated_at'],
                 'source_generated_at': packet['source_generated_at'], 'funds': packet['funds_total'],
                 'manager_comparisons': packet['manager_comparison_count'], 'cohort_indexes': len(packet['security_indexes']),
                 'public_artifacts_replayed': len(inspected), 'complete_replay_verified': True,
                 'whole_preceding_products_preserved': preserved_before, 'source_research_unchanged': True,
                 'legacy_collector_invoked': False, 'paid_ai_calls': 0, 'notifications_sent': 0,
                 'private_account_reads': 0, 'portfolio_writes': 0,
                 'remaining': 'Remaining direct/indirect consumer migrations, CapitalFlow replacement and recurring independent auditing are not certified by this source cutover.'}
        key = 'data/holdings-canonical-verification.json'
        s3.put_object(Bucket=bucket, Key=key, Body=canonical.model.encoded(proof), ContentType='application/json', CacheControl='no-store')
        assert json.loads(read_public(key)) == proof
        r.kv(acceptance_complete=True, proof_key=key, public_artifacts_replayed=len(inspected))


if __name__ == '__main__':
    try: main()
    except Exception:
        print('Canonical holdings acceptance failed. Read the committed report before retrying any invocation.')
        sys.exit(1)
