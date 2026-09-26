"""Qualify all original ACM workbook cells without replacing the native engine.

One official public workbook request, durably claimed before acquisition. Private
whole evidence only; no invocations, account reads, public writes or schedule work.
"""
from pathlib import Path
import json,re,subprocess,sys,tempfile,urllib.request
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/checks','aws/ops/staged','scripts',
    'aws/lambdas/justhodl-term-premium/source')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
import ops_6130_term_premium_original_baseline as baseline
import term_premium_candidate as candidate
import verify_term_premium_arithmetic as independent
import retained_access_evidence as access

REQUEST='chatgpt-term-premium-workbook-qualification-6131'
STATUS=baseline.PRIVATE+'requests/'+baseline.sha(REQUEST.encode())+'.json'
BASELINE={'key':baseline.PRIVATE+'5e9843aa6ea1af5934da8c1622de7e332552648d094bc5d42b5ad43c7fe2c810.bin',
    'sha256':'5e9843aa6ea1af5934da8c1622de7e332552648d094bc5d42b5ad43c7fe2c810','bytes':11376}
ISOLATED_REPLAY='''from pathlib import Path
import sys,json
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
import term_premium_candidate as candidate
import verify_term_premium_arithmetic as independent
raw=(root/'original.xls').read_bytes();source=json.loads((root/'source.json').read_bytes())
out=candidate.build(raw,source,sys.argv[2]);proof=independent.verify(out,raw)
(root/'candidate.json').write_bytes(candidate.encoded(out))
(root/'proof.json').write_bytes(candidate.encoded(proof))
'''


def read(s3,ref):
    if (not isinstance(ref,dict) or not isinstance(ref.get('sha256'),str)
        or not re.fullmatch('[a-f0-9]{64}',ref['sha256']) or type(ref.get('bytes')) is not int
        or not 0<ref['bytes']<=candidate.MAX or ref.get('key')!=baseline.PRIVATE+ref['sha256']+'.bin'):
        raise ValueError('Exact protected whole reference required')
    raw=bounded(s3.get_object(Bucket=baseline.BUCKET,Key=ref['key'])['Body'])
    if len(raw)!=ref['bytes'] or baseline.sha(raw)!=ref['sha256']:raise ValueError('Complete retained bytes differ')
    return raw


def journal(s3,value,claim=False):
    raw=baseline.encoded(value)
    s3.put_object(Bucket=baseline.BUCKET,Key=STATUS,Body=raw,ContentType='application/json',
        CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    if bounded(s3.get_object(Bucket=baseline.BUCKET,Key=STATUS)['Body'])!=raw:raise ValueError('Journal readback differs')


def acquire():
    request=urllib.request.Request(candidate.URL,headers={'User-Agent':'Mozilla/5.0 JustHodl-source-research/1.0'})
    with urllib.request.urlopen(request,timeout=45) as response:
        if response.geturl()!=candidate.URL:raise ValueError('Unreviewed source redirect')
        headers={name:response.headers.get(name) for name in ('Content-Type','ETag','Last-Modified')}
        raw=bounded(response,candidate.MAX)
    if not raw:raise ValueError('Empty original workbook')
    return raw,{'source_url':candidate.URL,'sha256':baseline.sha(raw),'bytes':len(raw),
        'acquired_at':baseline.now(),'response_headers':headers}


def compiler_paths():
    paths={'term_premium_candidate.py':Path(candidate.__file__),'verify_term_premium_arithmetic.py':Path(independent.__file__)}
    native=ROOT/'aws/lambdas/justhodl-term-premium/source'
    for path in subprocess.check_output(['git','ls-files','--',str((native/'xlrd').relative_to(ROOT))],cwd=ROOT,text=True).splitlines():
        actual=ROOT/path
        if actual.suffix=='.py':paths[actual.relative_to(native).as_posix()]=actual
    if 'xlrd/__init__.py' not in paths or len(paths)!=12:raise ValueError('Reviewed complete parser closure required')
    return paths


def isolated_replay(s3,source_ref,workbook_ref,compilers,stamp):
    paths=compiler_paths()
    if set(compilers)!=set(paths):raise ValueError('Complete reviewed compiler closure required')
    with tempfile.TemporaryDirectory(prefix='term-premium-replay-') as directory:
        root=Path(directory)
        for name,path in paths.items():
            body=read(s3,compilers[name])
            if body!=path.read_bytes():raise ValueError('Retained compiler differs from reviewed source')
            target=root/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(body)
        (root/'original.xls').write_bytes(read(s3,workbook_ref))
        (root/'source.json').write_bytes(read(s3,source_ref))
        subprocess.run([sys.executable,'-I','-c',ISOLATED_REPLAY,str(root),stamp],cwd=root,check=True,timeout=120)
        return (root/'candidate.json').read_bytes(),(root/'proof.json').read_bytes()


def main():
    import resource,time
    for test in ('test_term_premium_candidate.py','test_term_premium_workbook_qualification.py'):
        subprocess.run([sys.executable,str(ROOT/'tests'/test)],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    with report('ops_6131_term_premium_workbook_qualification') as r:
        if '**Status:** success' not in (ROOT/'aws/ops/reports/latest/ops_6130_term_premium_original_baseline.md').read_text(encoding='utf8'):
            raise ValueError('Accepted original baseline required')
        original=json.loads(read(s3,BASELINE));native='aws/lambdas/'+baseline.FUNCTION+'/source/lambda_function.py'
        if original['status']!='retained':raise ValueError('Complete original baseline required')
        if read(s3,original['repo_predecessors'][native])!=(ROOT/native).read_bytes():raise ValueError('Native source changed since baseline')
        before=runtime(lam,s3,events,scheduler,baseline.FUNCTION)
        if before!=original['native_predecessor']['runtime']:raise ValueError('Native runtime changed since baseline')
        progress={'status':'claimed','request_id':REQUEST,'started_at':baseline.now(),'baseline':BASELINE,'provider_request_attempts':0}
        journal(s3,progress,True)
        try:
            progress.update(status='acquisition_attempted',provider_request_attempts=1);journal(s3,progress)
            raw,source=acquire()
            workbook_ref=baseline.retain(s3,raw);source_ref=baseline.retain(s3,baseline.encoded(source))
            progress.update(status='original_retained',workbook=workbook_ref,source=source_ref);journal(s3,progress)
            stamp=baseline.now();started=time.monotonic()
            output=candidate.build(raw,source,stamp);proof=independent.verify(output,raw)
            if proof['current_series']!=60 or proof['requested_series']!=60 or proof['observation_comparisons']!=240:
                raise ValueError('All reviewed model series and observation comparisons must qualify')
            profile={'compile_and_verify_seconds':round(time.monotonic()-started,3),
                'candidate_bytes':len(baseline.encoded(output)),'max_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
            compilers={}
            for name,path in compiler_paths().items():
                body=path.read_bytes()
                if name.startswith('xlrd/') and body!=read(s3,original['repo_predecessors'][path.relative_to(ROOT).as_posix()]):
                    raise ValueError('Vendored original parser changed since baseline')
                compilers[name]=baseline.retain(s3,body)
            output_ref=baseline.retain(s3,baseline.encoded(output));proof_ref=baseline.retain(s3,baseline.encoded(proof))
            summaries={name:{'data_rows':len(table['rows']),'original_columns':table['original_columns'],
                'first_observation':min(row['observation_date'] for row in table['rows']),
                'last_observation':max(row['observation_date'] for row in table['rows'])} for name,table in output['tables'].items()}
            del raw,output
            started=time.monotonic();rebuilt,rechecked=isolated_replay(s3,source_ref,workbook_ref,compilers,stamp)
            if rebuilt!=read(s3,output_ref) or rechecked!=read(s3,proof_ref):raise ValueError('Isolated whole replay differs')
            profile.update(isolated_replay_seconds=round(time.monotonic()-started,3),
                max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                replay_child_max_rss_kib=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
            population={'contract':'term-premium-qualified-population.v1','baseline':BASELINE,'source':source_ref,
                'workbook':workbook_ref,'compilers':compilers,'candidate':output_ref,'proof':proof_ref,'generated_at':stamp}
            manifest=baseline.retain(s3,baseline.encoded(population))
            protected={STATUS,manifest['key'],source_ref['key'],workbook_ref['key'],output_ref['key'],proof_ref['key'],
                *[ref['key'] for ref in compilers.values()]}
            outcomes=[access.check(key) for key in sorted(protected)]
            audit=baseline.retain(s3,baseline.encoded({'outcomes':outcomes}));outcomes.append(access.check(audit['key']))
            privacy=access.summarize(outcomes)
            if not privacy['all_denied']:raise ValueError('Qualification evidence must remain private')
            if runtime(lam,s3,events,scheduler,baseline.FUNCTION)!=before:raise ValueError('Native runtime changed during qualification')
            result={'manifest':manifest,'baseline':BASELINE,'workbook':workbook_ref,'source_reference':source_ref,'source':source,
                'candidate':output_ref,'proof_reference':proof_ref,'proof':proof,'compilers':compilers,'profile':profile,
                'privacy':privacy,'access_evidence':audit,'generated_at':stamp,'sheets':summaries,
                'all_original_cells_retained':True,'fresh_process_replay_verified':True,'native_package_unchanged':True,
                'native_formula_changed':False,'provider_requests':1,'producer_invocations':0,'consumer_invocations':0,
                'public_writes':0,'private_account_reads':0,'schedules_changed':0,'notifications_sent':0,
                'paid_ai_calls':0,'signal_writes':0,'forecast_qualified':False,'sizing_qualified':False}
            journal(s3,{**progress,'status':'complete','result':result});r.kv(**result)
        except Exception as exc:
            journal(s3,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
