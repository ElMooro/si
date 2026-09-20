"""Complete 5884 with build-aware checks. No engine invocation or account access."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import io
import json
import sys
import urllib.request
import zipfile

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(Path(__file__).resolve().parent), str(ROOT/'aws/ops'),
               str(ROOT/'aws/shared'), str(ROOT/'scripts')]
from ops_5884_holdings_derived_acceptance import TARGETS, BUCKET, PRIVATE, public, denied
from ops_report import report
from holdings_derived_boundary import BASIS, compound_rows, current_basis, flow_rows
from reskin_site import reskin_text

COMMIT = 'a7b3519062e970a1b2ea28aa184157c049322828'
# Complete current packets that passed 5884, before its CSS source/artifact mismatch.
ACCEPTED = {
    'data/compound-signals.json': 'c8d90fac4384130a5ee7fb128653816ac57e30d5f8657059cb00ee598553e705',
    'data/attention-confluence.json': '2f1eab069801de46cd0dd32ab1cac15191c0df86a2c6f75919208b653623826c',
    'data/flow-confluence.json': 'b34e30fc069b59662f67a2c4def013070cd2c871e46c089e139a1a7e67f076f0',
    'data/master-ranker.json': '4d90a09128c2dff2db2f21010c73084a5fd4747e81e14f3340ecca778240cc52',
}
PRECEDING = {
    'data/compound-signals.json': ('8fbbaed8041394eac72741ed5b8fcf9636d0740d79007f2ceb7206461db37185', 158581),
    'data/attention-confluence.json': ('793ec333b819e088f10d93db3c685de3283ea7ff7d437af3afb6ac2fb22c8511', 3749428),
    'data/flow-confluence.json': ('17cb4da666968101274f28c534f00e3c11a54feff53e57f8806d114712485e69', 598896),
    'data/master-ranker.json': ('cb454be3a3be25d9805caeb714e47a04f2546045215b4c04712176acd5c4d118', 102638),
}


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1')
    def raw(key):
        body = s3.get_object(Bucket=BUCKET, Key=key)['Body']
        try: value = body.read(64*1024*1024+1)
        finally: body.close()
        assert len(value) <= 64*1024*1024
        return value
    with report('ops_5885_holdings_derived_finalize') as r:
        build = json.loads(public('build-manifest.json')[0])
        assert build['commit_sha'] == COMMIT, 'Pages build is not the intended source commit'
        artifacts = {}
        for name in [v[1] for v in TARGETS.values()] + ['jh-holdings-boundary.js', 'jh-holdings-boundary.css']:
            body = public(name)[0]; digest = hashlib.sha256(body).hexdigest()
            assert digest == build['files_sha256'][name], 'Exact built artifact differs: '+name
            if name.endswith(('.js', '.css')):
                assert body == reskin_text((ROOT/name).read_text(encoding='utf-8')).encode(), 'Reviewed palette transformation differs'
            artifacts[name] = digest
        preserved = []
        for key, (sha, size) in PRECEDING.items():
            destination = PRIVATE+sha+'.bin'; body = raw(destination)
            assert hashlib.sha256(body).hexdigest() == sha and len(body) == size
            assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+destination) and denied('https://justhodl.ai/'+destination)
            preserved.append({'source': key, 'sha256': sha, 'bytes': size})
        releases, observations = {}, {}
        for fn, (key, page) in TARGETS.items():
            receipt = json.loads(public('data/ops/releases/'+fn+'.json')[0])
            conf = lam.get_function_configuration(FunctionName=fn)
            assert receipt['commit'] == COMMIT and conf['CodeSha256'] == receipt['code_sha256']
            assert conf['State'] == 'Active' and conf['LastUpdateStatus'] == 'Successful'
            url = lam.get_function(FunctionName=fn)['Code']['Location']
            with urllib.request.urlopen(url, timeout=45) as response: archive = response.read(32*1024*1024+1)
            assert len(archive) <= 32*1024*1024
            with zipfile.ZipFile(io.BytesIO(archive)) as z:
                assert z.read('lambda_function.py') == (ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_bytes()
                for name in ('holdings_derived_boundary.py', 'holdings_authority.py'):
                    assert z.read(name) == (ROOT/'aws/shared'/name).read_bytes()
            body, headers = public(key)
            assert hashlib.sha256(body).hexdigest() == ACCEPTED[key], 'Packet superseded; inspect without reinvoking'
            assert body == raw(key)
            assert any(k.lower() == 'cache-control' and 'no-store' in v for k, v in headers.items())
            p = json.loads(body); assert current_basis(p)
            if fn == 'justhodl-compound-aggregator':
                assert p['compound'] == compound_rows(p) and p['feed_stats']['smart_money'] == 0
                assert p['notifications_suppressed'] and not p['new_alerts']
            elif fn == 'justhodl-attention-confluence':
                assert 'funds' not in p['scoring']['smart_families']
                assert all(v['signals']['funds'] is None and 'funds' not in v['families_firing'] for v in p['tickers'].values())
            elif fn == 'justhodl-flow-confluence': assert p['multi_engine_confluence'] == flow_rows(p)
            else:
                assert all(p['holdings_exclusions']['composite_basis_present'].values())
                assert all(not ({'smart_money', 'institutional_13f'} & set(v['systems'])) and 'khalid_note' not in v for v in p['top_tickers'])
            releases[fn] = {'commit': COMMIT, 'code_sha256': conf['CodeSha256']}
            observations[fn] = {'packet_sha256': ACCEPTED[key], 'generated_at': p.get('generated_at') or p.get('as_of'),
                                'public_page': page, 'known_direct_and_cluster_paths_excluded': True}
        proof = {'contract': 'holdings-derived-consumer-verification.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
                 'releases': releases, 'consumers': observations, 'score_basis': BASIS,
                 'pages_commit': COMMIT, 'built_artifact_sha256': artifacts,
                 'whole_preceding_products_preserved': preserved,
                 'controlled_invocation_run': 35481005372,
                 'controlled_invocation_report': 'aws/ops/reports/latest/ops_5884_holdings_derived_acceptance.md',
                 'finalization_engine_invocations': 0, 'paid_ai_calls': 0, 'notifications_sent': 0,
                 'private_account_reads': 0, 'portfolio_writes': 0,
                 'verification_correction': 'Compare CSS with the reviewed deploy-time palette transformation and exact commit-bound build manifest.',
                 'remaining': 'Other indirect holdings paths, native overlap research, CapitalFlow and whole-composite replay are unfinished; these exclusions do not establish forecast or sizing authority.'}
        key = 'data/holdings-derived-consumer-verification.json'
        s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(proof, sort_keys=True).encode(), ContentType='application/json', CacheControl='no-store')
        assert json.loads(public(key)[0]) == proof
        r.kv(acceptance_complete=True, proof_key=key, releases=releases, finalization_engine_invocations=0, built_artifacts=artifacts)


if __name__ == '__main__':
    try: main()
    except Exception:
        print('Final verification failed. No engine was invoked; inspect the committed report.')
        sys.exit(1)
