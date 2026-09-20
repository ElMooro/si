"""Accept the deployed disclosure-overlap desk against original public SEC evidence."""
from datetime import datetime, timezone
from pathlib import Path
import base64
import hashlib
import io
import json
import re
import subprocess
import sys
import urllib.error
import urllib.request
import zipfile

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/shared'), str(ROOT/'scripts'),
               str(ROOT/'aws/lambdas/justhodl-smart-money-cluster/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from replay_holdings_overlap import verify_current
from reskin_site import reskin_text
import holdings_overlap as model
import overlap_store as store

FN = 'justhodl-smart-money-cluster'
BUCKET = 'justhodl-dashboard-live'
PRECEDING_SHA = 'a6f60a7b662c4d4f502b4b04d7e2a905cc63824c3bcbb05ceededd59f8d56218'
PRECEDING_BYTES = 5417715


def public(path, method='GET'):
    url = path if path.startswith('https://') else 'https://justhodl.ai/'+path
    request = urllib.request.Request(url, method=method, headers={'User-Agent': 'justhodl-verify-release/1.0'})
    with urllib.request.urlopen(request, timeout=45) as response:
        body = response.read(model.MAX_BYTES+1)
        assert len(body) <= model.MAX_BYTES, 'Complete artifact exceeds bound'
        return body, dict(response.headers)


def denied(url):
    try: public(url, 'HEAD'); return False
    except urllib.error.HTTPError as exc: return exc.code in (401, 403, 404)


def built_bytes(name, body):
    if not name.endswith('.html'): return body
    pattern = rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?'
    clean, count = re.subn(pattern, b'', body)
    assert count <= 1, 'Unexpected edge analytics insertion'
    return clean


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(
        read_timeout=330, tcp_keepalive=True, retries={'max_attempts': 0}))
    scheduler = boto3.client('scheduler', region_name='us-east-1')
    raw = store.reader(s3, BUCKET)
    with report('ops_5887_holdings_overlap_acceptance') as r:
        commit = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/'+FN], text=True).strip()
        receipt = model.decode(public('data/ops/releases/'+FN+'.json')[0])
        config = lam.get_function_configuration(FunctionName=FN)
        assert receipt['commit'] == commit and receipt['code_sha256'] == config['CodeSha256'], 'Exact runtime receipt differs'
        assert config['State'] == 'Active' and config['LastUpdateStatus'] == 'Successful'
        assert config['MemorySize'] == 1024 and config['Timeout'] == 300
        assert config['Handler'] == 'lambda_function.lambda_handler' and config['Runtime'] == 'python3.12'
        env = config.get('Environment', {}).get('Variables', {})
        assert env.get('S3_BUCKET', BUCKET) == BUCKET and env.get('S3_KEY', model.CURRENT) == model.CURRENT
        location = lam.get_function(FunctionName=FN)['Code']['Location']
        with urllib.request.urlopen(location, timeout=45) as response: archive = response.read(32*1024*1024+1)
        assert len(archive) <= 32*1024*1024
        assert base64.b64encode(hashlib.sha256(archive).digest()).decode() == config['CodeSha256']
        sources = {}
        with zipfile.ZipFile(io.BytesIO(archive)) as z:
            for path in (ROOT/'aws/lambdas'/FN/'source').glob('*.py'):
                body = path.read_bytes(); assert z.read(path.name) == body, 'Actual packaged source differs: '+path.name
                sources[path.name] = {'sha256': hashlib.sha256(body).hexdigest(), 'bytes': len(body)}
        r.kv(commit=commit, code_sha256=config['CodeSha256'], actual_packaged_sources=sources)

        rule = scheduler.get_schedule(Name='justhodl-smart-money-cluster-hourly', GroupName='default')
        assert rule['State'] == 'ENABLED' and rule['ScheduleExpression'] == 'cron(55 * * * ? *)'
        assert rule['Target']['Arn'] == config['FunctionArn']
        assert rule['Target']['RoleArn'] == 'arn:aws:iam::857687956942:role/justhodl-scheduler-role'
        assert json.loads(rule['Target']['Input']) == {'action': 'overlap_refresh', 'notify': False}
        assert rule['FlexibleTimeWindow']['Mode'] == 'OFF' and rule['ScheduleExpressionTimezone'] == 'UTC'
        schedule = {'name': rule['Name'], 'expression': rule['ScheduleExpression'], 'state': rule['State'],
                    'configured_target_verified': True, 'recurring_execution_observed_by_this_op': False}
        r.kv(schedule=schedule)

        private = store.PRIVATE+PRECEDING_SHA+'.bin'; preceding = raw(private)
        assert len(preceding) == PRECEDING_BYTES and hashlib.sha256(preceding).hexdigest() == PRECEDING_SHA
        assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+private) and denied('https://justhodl.ai/'+private)
        r.kv(whole_legacy_product_preserved={'sha256': PRECEDING_SHA, 'bytes': PRECEDING_BYTES}, anonymous_denied=True)

        started = datetime.now(timezone.utc).isoformat()
        response, rejected = invoke_when_available(lam, dict(FunctionName=FN, InvocationType='RequestResponse',
            LogType='Tail', Payload=b'{"action":"overlap_refresh","notify":false}'), wait_seconds=180)
        result = json.loads(response['Payload'].read())
        assert not response.get('FunctionError'), 'Execution failed; inspect before another invocation'
        assert result['statusCode'] == 200
        invoked = json.loads(result['body'])
        assert invoked.get('published') or invoked.get('reason') == 'source_and_compiler_unchanged', 'Unexpected invocation outcome'
        tail = base64.b64decode(response.get('LogResult', '')).decode('utf-8', 'replace')
        runtime = {}
        for key, pattern in {'request_id': r'REPORT RequestId:\s*([a-f0-9-]+)',
                'duration_ms': r'\bDuration:\s*([0-9.]+)\s*ms', 'memory_size_mb': r'Memory Size:\s*(\d+)\s*MB',
                'max_memory_used_mb': r'Max Memory Used:\s*(\d+)\s*MB'}.items():
            match = re.search(pattern, tail); assert match, 'Runtime REPORT missing '+key
            runtime[key] = match[1]
        runtime.update(duration_ms=float(runtime['duration_ms']), memory_size_mb=int(runtime['memory_size_mb']),
                       max_memory_used_mb=int(runtime['max_memory_used_mb']))
        assert runtime['duration_ms'] < 300000 and runtime['max_memory_used_mb'] < runtime['memory_size_mb']
        r.kv(invocation_started=started, published=invoked['published'], rejected_before_acceptance=rejected, runtime=runtime)

        body, headers = public(model.CURRENT); packet = model.decode(body)
        assert body == raw(model.CURRENT), 'Public current packet differs from storage'
        assert any(k.lower() == 'cache-control' and 'no-store' in v for k, v in headers.items())
        assert packet['contract'] == model.CONTRACT and packet['replay'] == invoked['replay']
        assert not packet['calls_eligible'] and not packet['sizing_eligible'] and not packet['execution_eligible']
        assert packet['call'] is None and packet['additional_independent_votes'] == 0
        assert packet['clusters'] == [] and packet['compatibility']['legacy_trade_scoring'] == 'retired'
        cache, checked = {}, set()
        def read(key):
            if key not in cache: cache[key] = public(key)[0]; checked.add(key)
            return cache[key]
        manifest, output = verify_current(read, run=packet['replay']['sha256'])
        assert {k: v for k, v in packet.items() if k != 'replay'} == output
        scope_details, disclosures = [], 0
        for period, cohort in output['cohorts'].items():
            for scope, ref in cohort['scopes'].items():
                doc = model.verified(ref, read, model.PREFIX, 'scopes')
                disclosures += doc['manager_disclosure_count']
                scope_details.append({'period': period, 'scope': scope, 'managers': len(doc['funds']),
                    'identities': doc['security_count'], 'pairs': len(doc['pairs']), 'sha256': ref['sha256']})
        assert disclosures == sum(v['reported_identity_count'] or 0 for v in output['funds'].values())
        if manifest.get('whole_preceding_product'):
            prev = manifest['whole_preceding_product']; prior = raw(prev['private_key'])
            assert len(prior) == prev['bytes'] and hashlib.sha256(prior).hexdigest() == prev['sha256']
            assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+prev['private_key']) and denied('https://justhodl.ai/'+prev['private_key'])
        r.kv(original_sec_to_overlap_reproduced=True, public_artifacts_replayed=len(checked),
             manager_disclosures=disclosures, counts=output['counts'], scopes=scope_details)

        build = model.decode(public('build-manifest.json')[0]); assert build['commit_sha'] == commit
        artifacts = {}
        for name in ('smart-money.html', 'jh-holdings-overlap.js', 'jh-holdings-overlap-page.js'):
            live = public(name)[0]
            assert hashlib.sha256(built_bytes(name, live)).hexdigest() == build['files_sha256'][name], 'Built page artifact differs: '+name
            if name.endswith('.js'): assert live == reskin_text((ROOT/name).read_text(encoding='utf-8')).encode()
            else: assert b'jh-holdings-overlap-page.js' in live and b'jh-holdings-overlap.js' in live
            artifacts[name] = {'public_sha256': hashlib.sha256(live).hexdigest(), 'build_sha256': build['files_sha256'][name]}
        assert model.decode(public('data/ops/releases/'+FN+'.json')[0])['commit'] == commit
        assert lam.get_function_configuration(FunctionName=FN)['CodeSha256'] == config['CodeSha256']
        proof = {'contract': 'holdings-overlap-verification.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
            'commit': commit, 'code_sha256': config['CodeSha256'], 'actual_packaged_sources': sources,
            'pages_commit': commit, 'built_artifacts': artifacts, 'replay': packet['replay'],
            'source': output['source'], 'source_generated_at': output['source_generated_at'],
            'calculated_at': output['generated_at'], 'counts': output['counts'], 'manager_disclosures': disclosures,
            'scopes': scope_details, 'original_sec_to_overlap_reproduced': True, 'public_artifacts_replayed': len(checked),
            'whole_legacy_product_preserved': {'sha256': PRECEDING_SHA, 'bytes': PRECEDING_BYTES},
            'anonymous_archive_denied': True, 'invocation_published': invoked['published'], 'runtime': runtime,
            'schedule': schedule, 'paid_ai_calls': 0, 'notifications_sent': 0, 'private_account_reads': 0, 'portfolio_writes': 0,
            'remaining': 'No transaction, conviction, diversification, forecast or sizing authority. Other consumers, CapitalFlow, time-valid security mapping and recurring independent replay remain unfinished.'}
        key = 'data/holdings-overlap-verification.json'
        s3.put_object(Bucket=BUCKET, Key=key, Body=model.encoded(proof), ContentType='application/json', CacheControl='no-store')
        assert model.decode(public(key)[0]) == proof
        r.kv(acceptance_complete=True, proof_key=key, replay=packet['replay'], built_artifacts=artifacts)


if __name__ == '__main__':
    try: main()
    except Exception:
        print('Overlap acceptance failed. Read the committed report before retrying; do not blindly reinvoke.')
        sys.exit(1)
