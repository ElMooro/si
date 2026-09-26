"""Replay the whole retained Signal Board population privately.

No new sidecar/provider request, invocation, private account read, public write,
notification or schedule change. Qualification is for a derived inventory only.
"""
from pathlib import Path
import json
import subprocess
import sys
import tempfile
import time
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/shared', 'aws/ops', 'aws/ops/checks', 'aws/ops/staged')]
from ops_report import report
import ops_6141_signal_board_original_baseline as baseline
import signal_board_candidate as candidate
import verify_signal_board_inventory as independent
import retained_access_evidence as access

REQUEST = 'chatgpt-signal-board-inventory-qualification-6142'
STATUS = baseline.PRIVATE+'requests/'+baseline.sha(REQUEST.encode())+'.json'
BASELINE = {'key': baseline.PRIVATE+'746cfc5ea9abc8dc933c7c1bbefc36c1adebe5db00d78c2f38cfe97031ef35d6.bin',
            'sha256': '746cfc5ea9abc8dc933c7c1bbefc36c1adebe5db00d78c2f38cfe97031ef35d6', 'bytes': 192798}

ISOLATED = '''from pathlib import Path
import hashlib,json,sys
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
import signal_board_candidate as candidate
import verify_signal_board_inventory as independent
inputs=json.loads((root/'inputs.json').read_bytes())
def read(ref):
    raw=(root/'originals'/ref['sha256']).read_bytes()
    if len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:
        raise ValueError('Retained source differs')
    return raw
out=candidate.build(inputs['registry'],inputs['captures'],read,inputs['generated_at'])
proof=independent.verify(out,inputs['registry'],inputs['captures'],read)
(root/'result.json').write_bytes(candidate.encoded(out))
(root/'proof.json').write_bytes(candidate.encoded(proof))
'''


def checked(s3, ref):
    candidate.reference(ref)
    if ref['key'] != baseline.PRIVATE+ref['sha256']+'.bin':
        raise ValueError('Accepted private research original required')
    raw = baseline.bounded(s3.get_object(Bucket=baseline.BUCKET, Key=ref['key'])['Body'])
    if len(raw) != ref['bytes'] or baseline.sha(raw) != ref['sha256']:
        raise ValueError('Retained artifact differs')
    return raw


def journal(s3, value, claim=False):
    raw = candidate.encoded(value)
    s3.put_object(Bucket=baseline.BUCKET, Key=STATUS, Body=raw, ContentType='application/json', CacheControl='no-store',
                  **({'IfNoneMatch': '*'} if claim else {}))
    if baseline.bounded(s3.get_object(Bucket=baseline.BUCKET, Key=STATUS)['Body']) != raw:
        raise ValueError('Journal readback differs')


def compiler_paths():
    return {m.__name__+'.py': Path(m.__file__) for m in (candidate, independent)}


def isolated(s3, inputs, compilers):
    if set(compilers) != set(compiler_paths()):
        raise ValueError('Complete reviewed compiler closure required')
    with tempfile.TemporaryDirectory(prefix='signal-board-replay-') as directory:
        root = Path(directory); (root/'originals').mkdir()
        for name, path in compiler_paths().items():
            body = checked(s3, compilers[name])
            if body != path.read_bytes():
                raise ValueError('Reviewed compiler differs')
            (root/name).write_bytes(body)
        for entry in inputs['captures'].values():
            if entry.get('original'):
                ref = entry['original']; (root/'originals'/ref['sha256']).write_bytes(checked(s3, ref))
        (root/'inputs.json').write_bytes(candidate.encoded(inputs))
        subprocess.run([sys.executable, '-I', '-c', ISOLATED, str(root)], cwd=root, check=True, timeout=180)
        return (root/'result.json').read_bytes(), (root/'proof.json').read_bytes()


def main():
    for name in ('test_signal_board_candidate.py', 'test_signal_board_inventory_qualification.py'):
        subprocess.run([sys.executable, str(ROOT/'tests'/name)], cwd=ROOT, check=True)
    s3, lam, events, scheduler = (boto3.client(n, region_name='us-east-1') for n in ('s3', 'lambda', 'events', 'scheduler'))
    with report('ops_6142_signal_board_inventory_qualification') as r:
        progress = {'status': 'claimed', 'request_id': REQUEST, 'started_at': baseline.now(), 'baseline': BASELINE}
        journal(s3, progress, True)
        try:
            old = json.loads(checked(s3, BASELINE))
            accepted = json.loads(baseline.bounded(s3.get_object(Bucket=baseline.BUCKET, Key=baseline.STATUS)['Body']))
            if old['status'] != 'retained' or accepted['status'] != 'complete' or accepted['manifest'] != BASELINE or not accepted['privacy']['all_denied']:
                raise ValueError('Complete accepted baseline required')
            source = ROOT/'aws/lambdas'/baseline.FUNCTION/'source/lambda_function.py'
            if checked(s3, old['repo_predecessors'][source.relative_to(ROOT).as_posix()]) != source.read_bytes():
                raise ValueError('Native predecessor changed')
            rows = baseline.feeds(source.read_text(encoding='utf-8'))
            if rows != old['registry']:
                raise ValueError('Whole registry differs')
            before = baseline.runtime(lam, s3, events, scheduler, baseline.FUNCTION)
            arn = lam.get_function_configuration(FunctionName=baseline.FUNCTION)['FunctionArn']
            bindings = baseline.triggers.collect(lam, scheduler, s3, arn, baseline.BUCKET)
            if before != old['native_predecessor']['runtime'] or bindings != old['trigger_inventory']:
                raise ValueError('Actual native package or schedule changed')
            captures = {key: old['captures'][key] for key in {row['source_key'] for row in rows}}
            inputs = {'registry': rows, 'captures': captures, 'generated_at': baseline.now()}
            started = time.monotonic()
            output = candidate.build(rows, captures, lambda ref: checked(s3, ref), inputs['generated_at'])
            proof = independent.verify(output, rows, captures, lambda ref: checked(s3, ref))
            out_raw, proof_raw = candidate.encoded(output), candidate.encoded(proof)
            compilers = {name: baseline.retain(s3, path.read_bytes()) for name, path in compiler_paths().items()}
            rebuilt, rebuilt_proof = isolated(s3, inputs, compilers)
            if (rebuilt, rebuilt_proof) != (out_raw, proof_raw):
                raise ValueError('Fresh-process complete replay differs')
            artifacts = {'inputs': baseline.retain(s3, candidate.encoded(inputs)),
                         'candidate': baseline.retain(s3, out_raw), 'proof': baseline.retain(s3, proof_raw)}
            population = {'contract': 'signal-board-inventory-qualified-population.v1', 'baseline': BASELINE,
                          'compilers': compilers, **artifacts, 'generated_at': inputs['generated_at'],
                          'source_scope': 'whole derived public sidecars, not original-provider evidence',
                          **dict.fromkeys(candidate.PERMISSIONS, False)}
            manifest = baseline.retain(s3, candidate.encoded(population))
            protected = {STATUS, manifest['key'], *(ref['key'] for ref in artifacts.values()), *(ref['key'] for ref in compilers.values())}
            outcomes = [access.check(key) for key in sorted(protected)]
            evidence = baseline.retain(s3, candidate.encoded(outcomes)); outcomes.append(access.check(evidence['key']))
            privacy = access.summarize(outcomes)
            if not privacy['all_denied']:
                raise ValueError('Private qualification artifacts exposed')
            if baseline.runtime(lam, s3, events, scheduler, baseline.FUNCTION) != before or baseline.triggers.collect(lam, scheduler, s3, arn, baseline.BUCKET) != bindings:
                raise ValueError('Native package or triggers changed during qualification')
            for qualifier, entry in old['scheduled_qualified_packages'].items():
                if lam.get_function_configuration(FunctionName=baseline.FUNCTION, Qualifier=qualifier)['CodeSha256'] != entry['code_sha256']:
                    raise ValueError('Qualified scheduled package changed')
            result = {'manifest': manifest, 'compilers': compilers, 'artifacts': artifacts, 'proof': proof,
                      'privacy': privacy, 'access_evidence': evidence, 'elapsed_s': round(time.monotonic()-started, 3),
                      'fresh_process_replay_verified': True, 'native_package_unchanged': True,
                      'original_provider_verified': False, 'public_packet_requests': 0, 'provider_requests': 0,
                      'producer_invocations': 0, 'consumer_invocations': 0, 'public_writes': 0,
                      'private_account_reads': 0, 'notifications_sent': 0, 'paid_api_calls': 0, 'schedules_changed': 0,
                      **dict.fromkeys(candidate.PERMISSIONS, False)}
            journal(s3, {**progress, 'status': 'complete', 'result': result}); r.kv(**result)
        except Exception as exc:
            journal(s3, {**progress, 'status': 'failed', 'error_type': type(exc).__name__}); raise


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
