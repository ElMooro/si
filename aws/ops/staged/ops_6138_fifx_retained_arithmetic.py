"""Qualify all retained FI/FX histories privately, with no new provider request.

No native or consumer invocation, current publication, schedule change, paid
API, notification or account access. Complete per-source shards bound memory.
"""
from pathlib import Path
import json, subprocess, sys, tempfile
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/checks','aws/ops/staged','scripts')]
from ops_report import report
import ops_6136_fifx_original_baseline as baseline
import ops_6137_fifx_full_source_capture as capture
import fifx_candidate as candidate
import fifx_catalog as catalog
import fifx_originals as originals
import fifx_timezones as timezones
import verify_fifx_arithmetic as independent
import canonical_fred_replay as canonical
import retained_access_evidence as access

REQUEST = 'chatgpt-fifx-retained-arithmetic-6138'
STATUS = baseline.PRIVATE+'requests/'+baseline.sha(REQUEST.encode())+'.json'
CAPTURE = {'key':baseline.PRIVATE+'f1d0601f35550f50fb2c5f9c082ef77d8eef4fc9a641bd7561b0c78b53db6fa7.bin',
    'sha256':'f1d0601f35550f50fb2c5f9c082ef77d8eef4fc9a641bd7561b0c78b53db6fa7','bytes':31248}
BASELINE = capture.BASELINE
read = capture.checked

ISOLATED = '''from pathlib import Path
import hashlib,json,sys
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
import fifx_candidate as candidate
import verify_fifx_arithmetic as independent
inputs=json.loads((root/'inputs.json').read_bytes())
proofs={}
for sid,entry in inputs['sources'].items():
    raw=(root/'originals'/entry['original']['sha256']).read_bytes() if entry.get('original') else None
    if raw is not None:
        assert hashlib.sha256(raw).hexdigest()==entry['original']['sha256'] and len(raw)==entry['original']['bytes']
    definition=inputs['definitions'].get(sid)
    output=candidate.build_source(sid,raw,entry.get('receipt'),inputs['generated_at'],definition)
    proof=independent.verify(output,raw,entry.get('receipt'),definition)
    result=candidate.encoded(output);expected=inputs['expected'][sid]
    if hashlib.sha256(result).hexdigest()!=expected['sha256'] or len(result)!=expected['bytes']:
        raise ValueError('Isolated complete source differs: '+sid)
    proofs[sid]=proof
(root/'proofs.json').write_bytes(candidate.encoded(proofs))
'''


def journal(s3, progress, claim=False):
    raw = baseline.encoded(progress)
    s3.put_object(Bucket=baseline.BUCKET, Key=STATUS, Body=raw, ContentType='application/json', CacheControl='no-store',
                  **({'IfNoneMatch':'*'} if claim else {}))
    if baseline.bounded(s3.get_object(Bucket=baseline.BUCKET, Key=STATUS)['Body']) != raw:
        raise ValueError('Qualification journal differs')


def compiler_paths():
    return {m.__name__+'.py':Path(m.__file__) for m in (candidate,catalog,originals,timezones,independent)}


def isolated(s3, sources, definitions, compilers, stamp, expected):
    paths = compiler_paths()
    if set(paths) != set(compilers):
        raise ValueError('Complete reviewed compiler closure required')
    with tempfile.TemporaryDirectory(prefix='fifx-replay-') as directory:
        root = Path(directory);(root/'originals').mkdir()
        for name, path in paths.items():
            raw = read(s3,compilers[name])
            if raw != path.read_bytes():
                raise ValueError('Reviewed compiler differs')
            (root/name).write_bytes(raw)
        for entry in sources.values():
            if entry.get('original'):
                (root/'originals'/entry['original']['sha256']).write_bytes(read(s3,entry['original']))
        (root/'inputs.json').write_bytes(candidate.encoded({'sources':sources,'definitions':definitions,
            'generated_at':stamp,'expected':expected}))
        subprocess.run([sys.executable,'-I','-c',ISOLATED,str(root)],cwd=root,check=True,timeout=300)
        return (root/'proofs.json').read_bytes()


def main():
    import resource, time
    for name in ('test_fifx_candidate.py','test_fifx_retained_arithmetic.py'):
        subprocess.run([sys.executable,str(ROOT/'tests'/name)],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('s3','lambda','events','scheduler'))
    with report('ops_6138_fifx_retained_arithmetic') as r:
        progress={'status':'claimed','request_id':REQUEST,'started_at':baseline.now(),'baseline':BASELINE,'capture':CAPTURE}
        journal(s3,progress,True)
        try:
            old=json.loads(read(s3,BASELINE));captured=json.loads(read(s3,CAPTURE))
            if old['status']!='retained' or captured['status']!='captured' or captured['baseline']!=BASELINE:
                raise ValueError('Accepted complete predecessor and source capture required')
            if set(captured['sources'])!=set(catalog.SOURCES):raise ValueError('Every source identity must be retained')
            native='aws/lambdas/'+baseline.FUNCTION+'/source/lambda_function.py'
            if read(s3,old['repo_predecessors'][native])!=(ROOT/native).read_bytes():raise ValueError('Predecessor changed since baseline')
            before=baseline.runtime(lam,s3,events,scheduler,baseline.FUNCTION)
            arn=lam.get_function_configuration(FunctionName=baseline.FUNCTION)['FunctionArn']
            bindings=baseline.triggers.collect(lam,scheduler,s3,arn,baseline.BUCKET)
            if before!=old['native_predecessor']['runtime'] or bindings!=old['trigger_inventory']:
                raise ValueError('Actual native package or triggers changed')
            source=json.loads(read(s3,old['captures']['data/report-measurements.json']['original']))
            restored=canonical.restore(source,catalog.FRED,lambda key:read(s3,old['canonical_originals'][key]['original']))
            if any(restored[sid] is None for sid in catalog.FRED):raise ValueError('Every original FRED definition required')
            definitions={sid:restored[sid]['definition'] for sid in catalog.FRED}
            definitions_ref=baseline.retain(s3,candidate.encoded(definitions))
            stamp=baseline.now();started=time.monotonic();outputs={};proofs={};overlap={}
            for sid in catalog.SOURCES:
                entry=captured['sources'][sid]
                raw=read(s3,entry['original']) if entry.get('original') else None
                receipt=entry.get('receipt')
                if receipt is not None and read(s3,entry['whole_receipt'])!=candidate.encoded(receipt):
                    raise ValueError('Complete acquisition receipt differs')
                output=candidate.build_source(sid,raw,receipt,stamp,definitions.get(sid))
                proof=independent.verify(output,raw,receipt,definitions.get(sid))
                if sid not in ('^MOVE','^VHSI') and not proof['current_available']:
                    raise ValueError('Reviewed measurement failed qualification: '+sid+' '+proof['status'])
                if sid=='^MOVE' and proof['status']!='identity_mismatch':raise ValueError('Conflicting retained MOVE must stay quarantined')
                if sid=='^VHSI' and proof['status']!='http_error':raise ValueError('Failed VHSI response must stay explicit')
                if sid in catalog.FRED:
                    prior={r['date']:r.get('value') for r in restored[sid]['observations']['observations']}
                    common=[r for r in output['original_rows'] if r['date'] in prior]
                    revised=[{'date':r['date'],'prior_value':prior[r['date']],'captured_value':r['value']} for r in common
                        if originals.decimal(r['value'])!=originals.decimal(prior[r['date']])]
                    overlap[sid]={'overlap_rows':len(common),'revisions':revised,'point_in_time_qualified':False}
                outputs[sid]=baseline.retain(s3,candidate.encoded(output));proofs[sid]=proof
                progress['completed_sources']=list(outputs);journal(s3,progress)
                del output,raw
            profile={'compile_and_verify_seconds':round(time.monotonic()-started,3),
                'total_candidate_bytes':sum(ref['bytes'] for ref in outputs.values()),'largest_source_bytes':max(ref['bytes'] for ref in outputs.values()),
                'max_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
            compilers={name:baseline.retain(s3,path.read_bytes()) for name,path in compiler_paths().items()}
            proof_ref=baseline.retain(s3,candidate.encoded(proofs));overlap_ref=baseline.retain(s3,candidate.encoded(overlap))
            started=time.monotonic()
            rebuilt=isolated(s3,captured['sources'],definitions,compilers,stamp,outputs)
            if rebuilt!=read(s3,proof_ref):raise ValueError('Independent fresh-process proof differs')
            profile.update(isolated_replay_seconds=round(time.monotonic()-started,3),
                max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,replay_child_max_rss_kib=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
            population={'contract':'fifx-qualified-population.v1','baseline':BASELINE,'capture':CAPTURE,'definitions':definitions_ref,
                'sources':captured['sources'],'source_candidates':outputs,'compilers':compilers,'proofs':proof_ref,'vintage_comparison':overlap_ref,
                'generated_at':stamp,'bond_vol_context':{'originals':captured['bond_vol_source'],'independent_votes':0},
                'calls_eligible':False,'sizing_eligible':False,'forecast_qualified':False}
            manifest=baseline.retain(s3,candidate.encoded(population))
            protected={STATUS,manifest['key'],definitions_ref['key'],proof_ref['key'],overlap_ref['key'],
                *(ref['key'] for ref in outputs.values()),*(ref['key'] for ref in compilers.values())}
            outcomes=[access.check(key) for key in sorted(protected)]
            access_ref=baseline.retain(s3,candidate.encoded(outcomes));outcomes.append(access.check(access_ref['key']))
            privacy=access.summarize(outcomes)
            if not privacy['all_denied']:raise ValueError('Protected qualification artifacts exposed')
            if baseline.runtime(lam,s3,events,scheduler,baseline.FUNCTION)!=before:raise ValueError('Native package changed during qualification')
            if baseline.triggers.collect(lam,scheduler,s3,arn,baseline.BUCKET)!=bindings:raise ValueError('Native triggers changed during qualification')
            result={'manifest':manifest,'proofs':proofs,'compilers':compilers,'profile':profile,'privacy':privacy,'access_evidence':access_ref,
                'sources':len(proofs),'current_sources':sum(p['current_available'] for p in proofs.values()),
                'original_rows':sum(p['original_rows'] for p in proofs.values()),'history_rows':sum(p['history_rows'] for p in proofs.values()),
                'independent_scalar_checks':sum(p['independent_scalar_checks'] for p in proofs.values()),
                'fresh_process_replay_verified':True,'native_package_unchanged':True,'provider_requests':0,'producer_invocations':0,
                'consumer_invocations':0,'public_writes':0,'schedules_changed':0,'notifications_sent':0,'private_account_reads':0,
                'paid_api_calls':0,'forecast_qualified':False,'sizing_qualified':False}
            journal(s3,{**progress,'status':'complete','result':result});r.kv(**result)
        except Exception as exc:
            journal(s3,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
