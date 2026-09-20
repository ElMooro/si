"""Accept the new CapitalFlow runtime, complete source replay and public page."""
from datetime import datetime, timezone
from pathlib import Path
import base64, hashlib, io, json, re, subprocess, sys, tempfile
import urllib.error, urllib.request, zipfile
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/shared'), str(ROOT/'scripts'),
               str(ROOT/'aws/lambdas/justhodl-capital-flow/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from replay_capital_research import verify_current
from replay_fred_vintage import read_public
from reskin_site import reskin_text
import capital_research as model
import capital_store as store

FN, BUCKET = 'justhodl-capital-flow', 'justhodl-dashboard-live'
TARGETS = ('justhodl-capital-flow', 'justhodl-ask', 'justhodl-best-setups', 'justhodl-deep-value-overlap',
    'justhodl-engine-conflicts', 'justhodl-equity-confluence', 'justhodl-flow-confluence',
    'justhodl-industry-rotation', 'justhodl-master-ranker', 'justhodl-narrative-vs-tape',
    'justhodl-compound-aggregator', 'justhodl-attention-confluence')


def public(path, method='GET'):
    url = path if path.startswith('https://') else 'https://justhodl.ai/'+path
    request = urllib.request.Request(url, method=method, headers={'User-Agent': 'justhodl-verify-release/1.0'})
    with urllib.request.urlopen(request, timeout=45) as response:
        body = response.read(model.MAX_BYTES+1)
        assert len(body) <= model.MAX_BYTES, 'Complete public artifact exceeds bound'
        return body, dict(response.headers)


def denied(url):
    try: public(url, 'HEAD'); return False
    except urllib.error.HTTPError as exc: return exc.code in (401, 403, 404)


def verified_runtime(lam, fn, commit):
    receipt = model.decode(public('data/ops/releases/'+fn+'.json')[0])
    config = lam.get_function_configuration(FunctionName=fn)
    assert receipt['commit'] == commit and receipt['code_sha256'] == config['CodeSha256'], 'Exact runtime receipt differs: '+fn
    assert config['State'] == 'Active' and config['LastUpdateStatus'] == 'Successful'
    url = lam.get_function(FunctionName=fn)['Code']['Location']
    with urllib.request.urlopen(url, timeout=45) as response: archive = response.read(32*1024*1024+1)
    assert len(archive) <= 32*1024*1024
    assert base64.b64encode(hashlib.sha256(archive).digest()).decode() == config['CodeSha256']
    sources = {}
    with zipfile.ZipFile(io.BytesIO(archive)) as z:
        for path in [*(ROOT/'aws/lambdas'/fn/'source').glob('*.py'), ROOT/'aws/shared/capital_research_boundary.py',
                     ROOT/'aws/shared/holdings_derived_boundary.py']:
            raw = path.read_bytes(); assert z.read(path.name) == raw, 'Actual packaged source differs: '+fn+'/'+path.name
            sources[path.name] = {'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}
    return config, {'commit': commit, 'code_sha256': config['CodeSha256'], 'actual_packaged_sources': sources}


def verified_pages(commit):
    build = model.decode(public('build-manifest.json')[0]); actual = build['commit_sha']
    assert re.fullmatch('[a-f0-9]{40}', actual)
    subprocess.run(['git', 'merge-base', '--is-ancestor', commit, actual], cwd=ROOT, check=True)
    subprocess.run(['git', 'diff', '--quiet', commit, actual, '--', '.', ':(exclude)aws/ops/**', ':(exclude)docs/**'], cwd=ROOT, check=True)
    artifacts = {}
    for name in ('capital-flow.html', 'jh-capital-research.js', 'jh-capital-research-page.js'):
        assert subprocess.check_output(['git', 'show', commit+':'+name], cwd=ROOT) == (ROOT/name).read_bytes()
        body = public(name)[0]; built = body
        if name.endswith('.html'):
            built, count = re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?', b'', body)
            assert count <= 1
            assert b'jh-capital-research.js' in body and b'jh-capital-research-page.js' in body
        else: assert body == reskin_text((ROOT/name).read_text(encoding='utf-8')).encode()
        assert hashlib.sha256(built).hexdigest() == build['files_sha256'][name], 'Built page differs: '+name
        artifacts[name] = {'public_sha256': hashlib.sha256(body).hexdigest(), 'build_sha256': build['files_sha256'][name]}
    return build, artifacts


def main(*, accepted_run=None, prior_runtime=None, report_name='ops_5891_capital_research_acceptance'):
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=330, tcp_keepalive=True, retries={'max_attempts': 0}))
    events = boto3.client('events', region_name='us-east-1')
    def raw(key): return store.bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body'])
    with report(report_name) as r:
        commit = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/'+FN], text=True).strip()
        releases = {}
        for fn in TARGETS:
            cfg, releases[fn] = verified_runtime(lam, fn, commit)
            if fn == FN: config = cfg
        assert config['MemorySize'] == 1024 and config['Timeout'] == 300
        assert config['Handler'] == 'lambda_function.lambda_handler' and config['Runtime'] == 'python3.12'
        r.kv(commit=commit, exact_runtime_releases=releases)
        rule = events.describe_rule(Name='justhodl-capital-flow-daily')
        targets = events.list_targets_by_rule(Rule=rule['Name'])
        assert not targets.get('NextToken')
        assert rule['State'] == 'ENABLED' and rule['ScheduleExpression'] == 'cron(30 16 * * ? *)'
        assert any(v['Id'] == '1' and v['Arn'] == config['FunctionArn'] for v in targets['Targets'])
        schedule = {'name': rule['Name'], 'expression': rule['ScheduleExpression'], 'state': rule['State'], 'existing_target_verified': True}
        r.kv(schedule=schedule)
        legacy_history = raw(store.HISTORY)
        assert hashlib.sha256(legacy_history).hexdigest() == 'f654110f2d85bef437608c9c3820d2d43335db46a563b9c4bc6e90af65f860c5'
        assert len(model.decode(legacy_history)['entries']) == 66
        if accepted_run is None:
            started = datetime.now(timezone.utc).isoformat()
            response, rejected = invoke_when_available(lam, dict(FunctionName=FN, InvocationType='RequestResponse',
                LogType='Tail', Payload=b'{"action":"capital_refresh","notify":false}'), wait_seconds=180)
            result = json.loads(response['Payload'].read())
            assert not response.get('FunctionError'), 'Capital research execution failed; inspect before retry'
            assert result.get('statusCode') == 200
            result = json.loads(result['body']); assert result.get('published') or result.get('reason') == 'source_and_compiler_unchanged'
            tail = base64.b64decode(response.get('LogResult', '')).decode('utf-8', 'replace')
            runtime = {}
            for key, pattern in {'request_id': r'REPORT RequestId:\s*([a-f0-9-]+)', 'duration_ms': r'\bDuration:\s*([0-9.]+)\s*ms',
                    'memory_size_mb': r'Memory Size:\s*(\d+)\s*MB', 'max_memory_used_mb': r'Max Memory Used:\s*(\d+)\s*MB'}.items():
                match = re.search(pattern, tail); assert match, 'Missing runtime REPORT '+key
                runtime[key] = match[1] if key == 'request_id' else float(match[1])
            accepted_run = result['replay']['sha256']
            r.kv(invocation_started=started, published=result['published'], replay=result['replay'], rejected_before_execution=rejected, runtime=runtime)
        else:
            assert prior_runtime, 'Previous accepted invocation evidence required'
            runtime = prior_runtime
            r.kv(engine_invocations_this_op=0, previously_published_run=accepted_run, prior_runtime=runtime)
        assert runtime['duration_ms'] < 300000 and runtime['max_memory_used_mb'] < runtime['memory_size_mb']
        body, headers = public(model.CURRENT); packet = model.decode(body)
        assert body == raw(model.CURRENT) and packet['contract'] == model.CONTRACT
        assert packet['replay']['sha256'] == accepted_run
        assert any(k.lower() == 'cache-control' and 'no-store' in v for k, v in headers.items())
        assert packet['accumulating'] == [] and packet['distributing'] == [] and packet['call'] is None
        assert not packet['calls_eligible'] and not packet['sizing_eligible'] and not packet['execution_eligible']
        assert raw(store.HISTORY) == legacy_history, 'Legacy history was rewritten'
        with tempfile.TemporaryDirectory(prefix='capital-public-replay-') as directory:
            cache = Path(directory); checked = set(); retained_bytes = 0
            def read(key):
                nonlocal retained_bytes
                path = cache/(hashlib.sha256(key.encode()).hexdigest()+'.bin')
                if not path.exists():
                    value = read_public(key); retained_bytes += len(value)
                    assert retained_bytes <= 12*1024**3, 'Complete replay cache requires partition review'
                    path.write_bytes(value); checked.add(key)
                return path.read_bytes()
            manifest, output = verify_current(read, run=accepted_run)
            assert output == {k: v for k, v in packet.items() if k != 'replay'}
            preserved = []
            for key, item in output['legacy_contexts'].items():
                original = read(item['artifact']['key']); private = store.PRIVATE+item['artifact']['sha256']+'.bin'
                assert raw(private) == original
                assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+private) and denied('https://justhodl.ai/'+private)
                preserved.append({'source': key, 'artifact': item['artifact'], 'private_backup_anonymous_denied': True})
            ko_doc = model.verified(output['managers']['BERKSHIRE']['bridge'], read, model.PREFIX, 'managers')
            ko = next(v for v in ko_doc['rows'] if v['identity'] == {'cusip':'191216100','class':'COM','quantity_type':'SH','put_call':None})
            assert ko['prior']['quantity'] == ko['current']['quantity'] == '400000000'
            assert ko['bridge']['quantity_term_usd']['numerator'] == '0'
            assert ko['bridge']['unit_value_term_usd']['display_decimal'] == '2088000000.00'
            r.kv(complete_original_source_replay=True, public_artifacts_checked=len(checked), replay_bytes=retained_bytes,
                 counts=output['counts'], legacy_publications_preserved=preserved, unchanged_quantity_value_change=ko['bridge'])
        build, artifacts = verified_pages(commit)
        assert model.decode(public('data/ops/releases/'+FN+'.json')[0])['commit'] == commit
        assert lam.get_function_configuration(FunctionName=FN)['CodeSha256'] == config['CodeSha256']
        proof = {'contract': 'capital-evidence-verification.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
            'commit': commit, 'releases': releases, 'replay': packet['replay'], 'source': output['source'],
            'counts': output['counts'], 'calculated_at': output['generated_at'], 'source_clocks': output['source_clocks'],
            'complete_original_source_replay': True, 'public_artifacts_checked': len(checked), 'replay_bytes': retained_bytes,
            'legacy_publications_preserved': preserved, 'legacy_history_unchanged': True,
            'pages_commit': build['commit_sha'], 'page_source_commit': commit, 'built_artifacts': artifacts,
            'schedule': schedule, 'runtime': runtime, 'paid_ai_calls': 0, 'notifications_sent': 0,
            'private_account_reads': 0, 'portfolio_writes': 0,
            'remaining': 'Consumers have exact code receipts; their refreshed public outputs need separate acceptance. No stock cash-flow, forecasting, sizing or execution authority is inferred. Recurring independent replay and wider indirect paths remain unfinished.'}
        key = 'data/capital-research-verification.json'
        s3.put_object(Bucket=BUCKET, Key=key, Body=model.encoded(proof), ContentType='application/json', CacheControl='no-store')
        assert model.decode(public(key)[0]) == proof
        r.kv(producer_and_page_accepted=True, proof_key=key, replay=packet['replay'], consumer_publication_acceptance='pending')


if __name__ == '__main__':
    try: main()
    except Exception:
        print('Capital research acceptance failed. Read the committed report before retrying; do not blindly reinvoke.')
        sys.exit(1)
