"""Qualify unpublished liquidity arithmetic against the accepted whole originals.

No source acquisition, public write, native invocation or schedule change.
Only content-addressed private evidence and this operation's journal are written.
"""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json, re, subprocess, sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/shared', 'aws/ops', 'aws/ops/checks', 'aws/ops/staged', 'scripts')]
from ops_report import report
from market_runtime_evidence import runtime, bounded
import ops_6115_liquidity_flow_original_baseline as baseline
import canonical_fred_replay as canonical
import liquidity_flow_candidate as candidate
import verify_liquidity_arithmetic as independent
import retained_access_evidence as access

REQUEST = 'chatgpt-liquidity-flow-retained-arithmetic-6116'
STATUS = baseline.PRIVATE+'requests/'+baseline.sha(REQUEST.encode())+'.json'
BASELINE = {'key': baseline.PRIVATE+'1696ff2ee5a02c3c645e86bddd910d20fd8692c8d23f07e96d0111cd56522cca.bin',
            'sha256': '1696ff2ee5a02c3c645e86bddd910d20fd8692c8d23f07e96d0111cd56522cca', 'bytes': 17835}


def read(s3, ref):
    if (not isinstance(ref, dict) or not re.fullmatch('[a-f0-9]{64}', ref.get('sha256', ''))
        or ref.get('key') != baseline.PRIVATE+ref['sha256']+'.bin'
        or type(ref.get('bytes')) is not int or not 0 < ref['bytes'] <= 64*1024*1024):
        raise ValueError('Exact retained original reference required')
    response = s3.get_object(Bucket=baseline.BUCKET, Key=ref['key'])
    try: raw = bounded(response['Body'])
    finally: response['Body'].close()
    if len(raw) != ref['bytes'] or baseline.sha(raw) != ref['sha256']: raise ValueError('Retained original differs')
    return raw


def journal(s3, value, claim=False):
    raw = baseline.encoded(value)
    s3.put_object(Bucket=baseline.BUCKET, Key=STATUS, Body=raw, ContentType='application/json',
                  CacheControl='no-store', **({'IfNoneMatch': '*'} if claim else {}))
    assert bounded(s3.get_object(Bucket=baseline.BUCKET, Key=STATUS)['Body']) == raw


def main():
    for test in ('test_liquidity_flow_candidate.py', 'test_liquidity_arithmetic_verifier.py', 'test_liquidity_retained_acceptance.py'):
        subprocess.run([sys.executable, str(ROOT/'tests'/test)], cwd=ROOT, check=True)
    s3, lam, events, scheduler = (boto3.client(name, region_name='us-east-1') for name in ('s3', 'lambda', 'events', 'scheduler'))
    with report('ops_6116_liquidity_flow_retained_arithmetic') as r:
        assert '**Status:** success' in (ROOT/'aws/ops/reports/latest/ops_6115_liquidity_flow_original_baseline.md').read_text(encoding='utf-8')
        previous = json.loads(read(s3, BASELINE))
        assert previous['status'] == 'retained' and previous['request_id'] == baseline.REQUEST
        before = runtime(lam, s3, events, scheduler, baseline.FUNCTION)
        assert before == previous['native_predecessor']['runtime']
        progress = {'request_id': REQUEST, 'status': 'claimed', 'baseline': BASELINE, 'started_at': baseline.now()}
        journal(s3, progress, True)
        try:
            source = json.loads(read(s3, previous['captures']['data/report-measurements.json']['original']))
            def original(key):
                if key not in previous['canonical_originals']: raise ValueError('Unretained original requested')
                entry = previous['canonical_originals'][key]
                assert entry['source_key'] == key
                return read(s3, entry['original'])
            originals = canonical.restore(source, tuple(candidate.SPECS), original)
            stamp = baseline.now()
            output = candidate.build(source, originals, stamp)
            proof = independent.verify(output, source, originals)
            refs = {name: baseline.retain(s3, Path(module.__file__).read_bytes())
                    for name, module in (('compiler', candidate), ('independent_verifier', independent))}
            refs['candidate'] = baseline.retain(s3, baseline.encoded(output))
            # Re-read retained output before independent verification.
            assert independent.verify(json.loads(read(s3, refs['candidate'])), source, originals) == proof
            refs['proof'] = baseline.retain(s3, baseline.encoded(proof))
            protected = [STATUS] + [ref['key'] for ref in refs.values()]
            outcomes = [access.check(key) for key in protected]
            refs['access_evidence'] = baseline.retain(s3, baseline.encoded({'outcomes': outcomes}))
            outcomes.append(access.check(refs['access_evidence']['key']))
            privacy = access.summarize(outcomes)
            assert privacy['all_denied'], 'Inspect retained HEAD outcomes before another operation'
            assert runtime(lam, s3, events, scheduler, baseline.FUNCTION) == before
            snapshot = output['last_reconstructed_snapshot']
            final = {'baseline': BASELINE, 'artifacts': refs, 'proof': proof, 'privacy': privacy,
                'generated_at': stamp, 'source_generated_at': source['generated_at'], 'source_replay': source['replay'],
                'last_reconstructed_snapshot': snapshot, 'quality': output['quality'],
                'signed_changes': {key: {'change': row['change'], 'baseline_valuation_date': row['baseline_valuation_date'],
                    'contributions': {sid: leg['signed_formula_contribution'] for sid, leg in row['legs'].items()}}
                    for key, row in output['comparisons'].items()},
                'all_original_rows_retained': True, 'originals_reconstructed': True,
                'native_package_unchanged': True, 'native_formula_changed': False, 'provider_requests': 0,
                'producer_invocations': 0, 'consumer_invocations': 0, 'public_writes': 0,
                'private_account_reads': 0, 'paid_ai_calls': 0, 'notifications_sent': 0,
                'signal_writes': 0, 'schedules_changed': 0, 'forecast_qualified': False, 'sizing_qualified': False}
            journal(s3, {**progress, 'status': 'complete', 'result': final}); r.kv(**final)
        except Exception as exc:
            journal(s3, {**progress, 'status': 'failed', 'error_type': type(exc).__name__}); raise


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
