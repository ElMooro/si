"""Verify CapitalFlow consumers without acceptance reads of private accounts."""
from datetime import datetime, timezone
from pathlib import Path
import base64, hashlib, io, json, re, subprocess, sys, urllib.error, urllib.request, zipfile
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/shared'), str(ROOT/'scripts')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from capital_research_boundary import current_basis, context
from holdings_derived_boundary import flow_rows, flow_annotations

BUCKET = 'justhodl-dashboard-live'
PRIOR = '5d51b7c6422bc1dd41ea24546fdd2b7456202ba0'
CLOSURE = ('flow-confluence', 'best-setups', 'master-ranker', 'compound-aggregator', 'attention-confluence')
REFRESH = ('deep-value-overlap', 'equity-confluence', 'flow-confluence', 'master-ranker', 'narrative-vs-tape', 'industry-rotation')
NATURAL = ('best-setups', 'engine-conflicts')
PAGES = ('deep-value-overlap.html', 'equity-confluence.html', 'flow-confluence.html', 'industry-rotation.html',
         'cockpit.html', 'jh-capital-boundary.js', 'jh-capital-research-page.js')


def public(key, method='GET'):
    url = key if key.startswith('https://') else 'https://justhodl.ai/'+key
    with urllib.request.urlopen(urllib.request.Request(url, method=method,
            headers={'User-Agent':'justhodl-verify-release/1.0'}), timeout=45) as response:
        body = response.read(64*1024*1024+1)
        assert len(body) <= 64*1024*1024
        return body, dict(response.headers)


def denied(url):
    try: public(url, 'HEAD'); return False
    except urllib.error.HTTPError as exc: return exc.code in (401,403,404)


def runtime(lam, name, expected):
    fn = 'justhodl-'+name
    receipt = json.loads(public('data/ops/releases/'+fn+'.json')[0])
    conf = lam.get_function_configuration(FunctionName=fn)
    assert receipt['commit'] == expected and receipt['code_sha256'] == conf['CodeSha256'], 'Exact receipt differs: '+fn
    assert conf['State'] == 'Active' and conf['LastUpdateStatus'] == 'Successful'
    with urllib.request.urlopen(lam.get_function(FunctionName=fn)['Code']['Location'], timeout=45) as response:
        archive = response.read(32*1024*1024+1)
    assert len(archive) <= 32*1024*1024 and base64.b64encode(hashlib.sha256(archive).digest()).decode() == conf['CodeSha256']
    paths = [*(ROOT/'aws/lambdas'/fn/'source').glob('*.py')]
    if name in CLOSURE: paths.append(ROOT/'aws/shared/holdings_derived_boundary.py')
    paths.append(ROOT/'aws/shared/capital_research_boundary.py')
    with zipfile.ZipFile(io.BytesIO(archive)) as z:
        for p in paths: assert z.read(p.name) == p.read_bytes(), 'Actual packaged source differs: '+fn+'/'+p.name
    return {'commit': expected, 'code_sha256': conf['CodeSha256'], 'timeout': conf['Timeout'], 'deployed_at': receipt['deployed_at']}


def check(name, packet, capital):
    if name == 'master-ranker':
        assert packet['holdings_exclusions']['composite_basis_present']['flow_confluence']
        assert all('khalid_note' not in v for v in packet['top_tickers'])
        assert any(v.get('key') == 'data/notes-index.json' and v.get('exclusion') == 'private_source_not_read_by_public_ranker'
                   for v in packet['feed_freshness'])
        return
    assert current_basis(packet), 'Previous CapitalFlow calculation still published: '+name
    q = packet['capital_flow_exclusion']
    assert q == context(capital), 'Actual CapitalFlow source context differs: '+name
    if name == 'flow-confluence':
        assert flow_rows(packet) == packet['multi_engine_confluence']
        assert flow_annotations(packet) == packet['ticker_map'], 'Missing or forbidden flow components'
    elif name == 'deep-value-overlap':
        assert all('capital flow' not in str(r['catalysts']).lower() for r in packet['board'])
    elif name == 'equity-confluence':
        assert 'flow_macro' not in packet['family_status']
        assert all(v['engine'] != 'capital-flow' for v in packet['sources'])
    elif name == 'narrative-vs-tape':
        assert packet['crowded_fading'] == []
        assert all(v['tape'] == 'Upstream dislocation screen: cheap & inflecting' for v in packet['quiet_accumulation'])
    elif name == 'industry-rotation':
        assert packet['join_hits']['capital_flow'] is None
        assert all('capital_flow' not in v.get('smart_money',{}) for v in packet['ladder'])
    elif name == 'best-setups':
        for v in packet['top_setups']:
            assert 'CAPITAL_FLOW' not in v['signal_keys']
            if v.get('flow_confluence'):
                envelope = {'holdings_exclusions': {'basis':'holdings-direct-and-cluster-excluded.v1'},
                            'capital_flow_exclusion': q, 'ticker_map': {v['ticker']:v['flow_confluence']}}
                assert flow_annotations(envelope) == envelope['ticker_map']
    elif name == 'engine-conflicts':
        assert all(v['type'] != 'FLOW vs PRICE' for v in packet['conflicts'])


def main():
    s3 = boto3.client('s3',region_name='us-east-1')
    lam = boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=730,tcp_keepalive=True,retries={'max_attempts':0}))
    def raw(key):
        stream = s3.get_object(Bucket=BUCKET,Key=key)['Body']
        try: value = stream.read(64*1024*1024+1)
        finally: stream.close()
        assert len(value) <= 64*1024*1024
        return value
    with report('ops_5892_capital_consumer_acceptance') as r:
        commit = subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/holdings_derived_boundary.py'],text=True).strip()
        releases = {name:runtime(lam,name,commit if name in CLOSURE else PRIOR)
                    for name in sorted(set(CLOSURE+REFRESH+NATURAL+('ask',)))}
        r.kv(exact_actual_runtimes=releases, paid_ai_calls=0, private_account_reads=0, notifications_sent=0)
        preserved = []
        for name in REFRESH+NATURAL:
            key = 'data/'+name+'.json'; body = raw(key)
            assert json.loads(public(key)[0]) == json.loads(body), 'Only the existing public product may be retained'
            sha = hashlib.sha256(body).hexdigest(); dest = 'audit-private/20260909-originals/capital-consumers/'+sha+'.bin'
            try: s3.put_object(Bucket=BUCKET,Key=dest,Body=body,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
            except Exception as exc:
                if getattr(exc,'response',{}).get('Error',{}).get('Code') not in ('PreconditionFailed','ConditionalRequestConflict'): raise
            assert raw(dest) == body
            assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+dest) and denied('https://justhodl.ai/'+dest)
            preserved.append({'source':key,'sha256':sha,'bytes':len(body)})
        r.kv(whole_preceding_publications_preserved=preserved)
        capital = json.loads(raw('data/capital-flow.json'))
        assert capital['contract'] == 'capital-evidence-research.v1'
        observations = {}
        for name in REFRESH:
            key = 'data/'+name+'.json'; started = datetime.now(timezone.utc)
            response,rejected = invoke_when_available(lam,dict(FunctionName='justhodl-'+name, InvocationType='RequestResponse',
                Payload=b'{"suppress_events":true,"suppress_alerts":true,"notify":false}'),wait_seconds=180)
            value = json.loads(response['Payload'].read())
            # Record that execution happened before any subsequent assertion.
            r.kv(invoked=name, invocation_started=started.isoformat(), request_id=response.get('ResponseMetadata',{}).get('RequestId'),
                 function_error=response.get('FunctionError'), throttle_rejections=rejected)
            assert not response.get('FunctionError') and value.get('statusCode',200) == 200, 'Read report before any reinvocation'
            packet = json.loads(raw(key)); stamp = packet.get('generated_at') or packet.get('as_of')
            assert datetime.fromisoformat(stamp.replace('Z','+00:00')) >= started.replace(microsecond=0)
            check(name,packet,capital)
            live,headers = public(key); assert json.loads(live) == packet
            assert any(k.lower() == 'cache-control' and 'no-store' in v for k,v in headers.items())
            observations[name] = {'generated_at':stamp,'public_sha256':hashlib.sha256(live).hexdigest(),'acceptance':'direct_path_verified'}
            r.kv(accepted=name,public_output=observations[name])
        for name in NATURAL:
            # Read only the existing public result; do not invoke private-context producers.
            body,_ = public('data/'+name+'.json'); packet = json.loads(body)
            stamp = packet.get('generated_at')
            qualified = (current_basis(packet) and isinstance(stamp,str)
                and datetime.fromisoformat(stamp.replace('Z','+00:00')) >= datetime.fromisoformat(releases[name]['deployed_at'].replace('Z','+00:00')))
            if qualified: check(name,packet,capital)
            observations[name] = {'generated_at':packet.get('generated_at'),'public_sha256':hashlib.sha256(body).hexdigest(),
                'acceptance':'direct_path_verified_from_existing_publication' if qualified else 'awaiting_natural_refresh',
                'engine_invocations_this_op':0}
        build = json.loads(public('build-manifest.json')[0]); actual = build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',commit,actual],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',commit,actual,'--','.',':(exclude)aws/ops/**',':(exclude)docs/**'],cwd=ROOT,check=True)
        for name in PAGES:
            body,_ = public(name)
            built,count = re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',body)
            assert count <= 1 and hashlib.sha256(built).hexdigest() == build['files_sha256'][name], 'Built asset differs: '+name
            if name.endswith('.html'): assert b'jh-capital-boundary.js' in body
        for name,item in releases.items():
            assert json.loads(public('data/ops/releases/justhodl-'+name+'.json')[0])['commit'] == item['commit']
            assert lam.get_function_configuration(FunctionName='justhodl-'+name)['CodeSha256'] == item['code_sha256']
        proof = {'contract':'capital-consumer-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'commit':commit,'releases':releases,'consumers':observations,'capital_source_replay':capital['replay'],
            'pages_commit':actual,'built_assets':{k:build['files_sha256'][k] for k in PAGES},
            'whole_preceding_publications_preserved':preserved,'paid_ai_calls':0,'notifications_sent':0,
            'private_account_reads':0,'portfolio_writes':0,
            'remaining':'Ask runtime and offline tests verified; no live private or LLM request made. Any awaiting natural refresh remains pending. These direct-path exclusions do not qualify indirect families, predictive performance, whole-composite replay or sizing.'}
        key='data/capital-consumer-verification.json'
        s3.put_object(Bucket=BUCKET,Key=key,Body=json.dumps(proof,sort_keys=True).encode(),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key)[0]) == proof
        r.kv(proof_key=key,consumers=observations)


if __name__ == '__main__':
    try: main()
    except Exception:
        print('Read the committed report before retrying; completed invocations must not be repeated blindly.')
        sys.exit(1)
