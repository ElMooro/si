"""Accept four actual consumers, preserve preceding products, and verify public pages."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import io
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zipfile

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/shared'), str(ROOT/'scripts')]
from ops_report import report
from reskin_site import reskin_text
from acceptance_invoke import invoke_when_available
from holdings_derived_boundary import BASIS, compound_rows, current_basis, flow_rows

BUCKET = 'justhodl-dashboard-live'
PRIVATE = 'audit-private/20260909-originals/holdings-consumers/'
TARGETS = {
    'justhodl-compound-aggregator': ('data/compound-signals.json', 'compound-signals.html'),
    'justhodl-attention-confluence': ('data/attention-confluence.json', 'attention.html'),
    'justhodl-flow-confluence': ('data/flow-confluence.json', 'flow-confluence.html'),
    'justhodl-master-ranker': ('data/master-ranker.json', 'master-rank.html'),
}


def public(path, method='GET'):
    url = path if path.startswith('https://') else 'https://justhodl.ai/'+path
    request = urllib.request.Request(url, method=method, headers={'User-Agent': 'justhodl-verify-release/1.0'})
    with urllib.request.urlopen(request, timeout=45) as response:
        raw = response.read(64*1024*1024+1)
        assert len(raw) <= 64*1024*1024
        return raw, dict(response.headers)


def denied(url):
    try:
        public(url, 'HEAD')
        return False
    except urllib.error.HTTPError as exc:
        return exc.code in (401, 403, 404)


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(
        read_timeout=180, tcp_keepalive=True, retries={'max_attempts': 0}))
    def raw(key):
        body = s3.get_object(Bucket=BUCKET, Key=key)['Body']
        try: value = body.read(64*1024*1024+1)
        finally: body.close()
        assert len(value) <= 64*1024*1024, 'Whole artifact bound exceeded'
        return value
    def read(key): return json.loads(raw(key))
    with report('ops_5884_holdings_derived_acceptance') as r:
        releases = {}
        for fn in TARGETS:
            expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/'+fn], text=True).strip()
            receipt = read('data/ops/releases/'+fn+'.json')
            conf = lam.get_function_configuration(FunctionName=fn)
            assert receipt['commit'] == expected and receipt['code_sha256'] == conf['CodeSha256'], fn+' exact receipt differs'
            assert conf['State'] == 'Active' and conf['LastUpdateStatus'] == 'Successful'
            location = lam.get_function(FunctionName=fn)['Code']['Location']
            with urllib.request.urlopen(location, timeout=45) as response: archive = response.read(32*1024*1024+1)
            assert len(archive) <= 32*1024*1024
            with zipfile.ZipFile(io.BytesIO(archive)) as z:
                for path in (ROOT/'aws/lambdas'/fn/'source').glob('*.py'):
                    assert z.read(path.name) == path.read_bytes(), 'Actual source differs: '+str(path)
                for name in ('holdings_authority.py', 'holdings_derived_boundary.py'):
                    assert z.read(name) == (ROOT/'aws/shared'/name).read_bytes()
                if fn == 'justhodl-master-ranker':
                    assert z.read('private_artifact.py') == (ROOT/'aws/shared/private_artifact.py').read_bytes()
            releases[fn] = {'commit': expected, 'code_sha256': conf['CodeSha256']}
        r.kv(releases=releases, notification_suppression=True, private_account_reads=0)
        keys = [v[0] for v in TARGETS.values()] + ['data/compound-signals-state.json',
                'data/compound-firstseen.json', 'data/compound-history.json',
                'data/prime-convergence.json', 'data/master-ranker.json.prev']
        preserved = []
        for key in keys:
            try: body = raw(key)
            except Exception as exc:
                if getattr(exc, 'response', {}).get('Error', {}).get('Code') == 'NoSuchKey': continue
                raise
            sha = hashlib.sha256(body).hexdigest(); dest = PRIVATE+sha+'.bin'
            try: s3.put_object(Bucket=BUCKET, Key=dest, Body=body, IfNoneMatch='*', ContentType='application/octet-stream', CacheControl='no-store')
            except Exception as exc:
                if getattr(exc, 'response', {}).get('Error', {}).get('Code') not in ('PreconditionFailed', 'ConditionalRequestConflict'): raise
            assert raw(dest) == body
            assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+dest) and denied('https://justhodl.ai/'+dest)
            preserved.append({'source': key, 'sha256': sha, 'bytes': len(body)})
        r.kv(whole_preceding_products_preserved=preserved)
        observations = {}
        for fn, (key, page) in TARGETS.items():
            started = datetime.now(timezone.utc)
            alert_before = hashlib.sha256(raw('data/compound-signals-state.json')).hexdigest() if fn == 'justhodl-compound-aggregator' else None
            response, rejected = invoke_when_available(lam, dict(FunctionName=fn, InvocationType='RequestResponse',
                Payload=b'{"suppress_events":true,"suppress_alerts":true,"notify":false}'), wait_seconds=180)
            result = json.loads(response['Payload'].read())
            assert not response.get('FunctionError'), 'Consumer execution failed; inspect before retry'
            assert result.get('statusCode', 200) == 200
            packet = read(key)
            assert current_basis(packet)
            stamp = packet.get('generated_at') or packet.get('as_of')
            assert datetime.fromisoformat(stamp.replace('Z', '+00:00')) >= started.replace(microsecond=0)
            assert all(not v['vote_eligible'] and v['independent_votes'] == 0 for v in packet['holdings_exclusions']['sources'])
            if fn == 'justhodl-compound-aggregator':
                assert compound_rows(packet) == packet['compound']
                assert packet['feed_stats']['smart_money'] == 0 and packet['notifications_suppressed']
                assert packet['new_alerts'] == [] and packet['score_basis'] == BASIS
                assert hashlib.sha256(raw('data/compound-signals-state.json')).hexdigest() == alert_before
                assert current_basis(read('data/prime-convergence.json'))
            elif fn == 'justhodl-attention-confluence':
                assert 'funds' not in packet['scoring']['smart_families'] and packet['panels']['smart_money'] == []
                assert all(v['signals']['funds'] is None and 'funds' not in v['families_firing'] for v in packet['tickers'].values())
            elif fn == 'justhodl-flow-confluence':
                assert flow_rows(packet) == packet['multi_engine_confluence']
            else:
                assert all(packet['holdings_exclusions']['composite_basis_present'].values())
                assert all(not ({'smart_money', 'institutional_13f'} & set(v['systems'])) and 'khalid_note' not in v for v in packet['top_tickers'])
                assert any(v.get('key') == 'data/notes-index.json' and v.get('exclusion') == 'private_source_not_read_by_public_ranker' for v in packet['feed_freshness'])
            live, headers = public(key)
            assert json.loads(live) == packet, 'Public packet does not match accepted output'
            assert any(k.lower() == 'cache-control' and 'no-store' in v for k, v in headers.items()), 'Public cache policy differs'
            html, _ = public(page)
            assert b'jh-holdings-boundary.js' in html and b'id="holdings-boundary"' in html
            assert read('data/ops/releases/'+fn+'.json')['commit'] == releases[fn]['commit']
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256'] == releases[fn]['code_sha256']
            observations[fn] = {'generated_at': stamp, 'packet_sha256': hashlib.sha256(live).hexdigest(),
                                'throttle_rejections_before_execution': rejected, 'public_page': page,
                                'known_direct_and_cluster_paths_excluded': True}
            r.kv(consumer=fn, acceptance=observations[fn])
        for name in ('jh-holdings-boundary.js', 'jh-holdings-boundary.css'):
            assert public(name)[0] == reskin_text((ROOT/name).read_text(encoding='utf-8')).encode(), 'Built public asset differs: '+name
        proof = {'contract': 'holdings-derived-consumer-verification.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
                 'releases': releases, 'consumers': observations, 'whole_preceding_products_preserved': preserved,
                 'score_basis': BASIS, 'paid_ai_calls': 0, 'notifications_sent': 0,
                 'private_account_reads': 0, 'portfolio_writes': 0,
                 'remaining': 'Other indirect holdings paths, disclosure-overlap producer, CapitalFlow and recurring independent replay remain unfinished. These exclusions do not validate forecast performance.'}
        key = 'data/holdings-derived-consumer-verification.json'
        s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(proof, sort_keys=True).encode(), ContentType='application/json', CacheControl='no-store')
        assert json.loads(public(key)[0]) == proof
        r.kv(acceptance_complete=True, proof_key=key)


if __name__ == '__main__':
    try: main()
    except Exception:
        print('Derived holdings acceptance failed. Read the committed report before any retry.')
        sys.exit(1)
