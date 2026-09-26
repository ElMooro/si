"""Qualify full Bond Vol arithmetic from originals, without changing native execution.

All new artifacts remain private. One durably claimed public quote acquisition;
existing FRED originals only. No invocation, schedules, paid AI or notifications.
"""
from pathlib import Path
import gzip,io,json,re,subprocess,sys,tempfile,urllib.request
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/checks','aws/ops/staged','scripts')]
from ops_report import report
from market_runtime_evidence import bounded
import ops_6134_bond_vol_original_baseline as baseline
import bond_vol_candidate as candidate
import bond_vol_catalog as catalog
import bond_vol_timezone as timezone_data
import verify_bond_vol_arithmetic as independent
import canonical_fred_replay as canonical
import report_observations,research_brief_model,evidence_store
import retained_access_evidence as access

REQUEST='chatgpt-bond-vol-retained-arithmetic-6135'
STATUS=baseline.PRIVATE+'requests/'+baseline.sha(REQUEST.encode())+'.json'
BASELINE={'key':baseline.PRIVATE+'dc68c9336817c13e610c7edda771aef3e47f5f4cb9bd14196f07328998842ab9.bin',
    'sha256':'dc68c9336817c13e610c7edda771aef3e47f5f4cb9bd14196f07328998842ab9','bytes':42690}

ISOLATED_REPLAY='''from pathlib import Path
import json,sys
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
import bond_vol_candidate as candidate
import canonical_fred_replay as canonical
import verify_bond_vol_arithmetic as independent
import report_observations
inputs=json.loads((root/'inputs.json').read_bytes())
source=json.loads((root/'source.json').read_bytes())
entries=inputs['entries']
def read(key):
    ref=entries[key]['original'];raw=(root/'originals'/ref['sha256']).read_bytes()
    if candidate.hashlib.sha256(raw).hexdigest()!=ref['sha256'] or len(raw)!=ref['bytes']:raise ValueError('Original bytes differ')
    return raw
originals=canonical.restore(source,candidate.SERIES,read)
quote=(root/'quote.bin').read_bytes() if inputs['quote'] else None
output=candidate.build(source,originals,inputs['generated_at'],inputs['context'],inputs['predecessor'],quote,inputs['quote_receipt'])
proof=independent.verify(output,source,originals,quote,inputs['quote_receipt'])
(root/'candidate.json').write_bytes(report_observations.encoded(output))
(root/'proof.json').write_bytes(report_observations.encoded(proof))
'''

def read(s3,ref):
    if not candidate.original_ref(ref):raise ValueError('Exact protected reference required')
    raw=bounded(s3.get_object(Bucket=baseline.BUCKET,Key=ref['key'])['Body'])
    if len(raw)!=ref['bytes'] or baseline.sha(raw)!=ref['sha256']:raise ValueError('Whole retained bytes differ')
    return raw

def journal(s3,value,claim=False):
    raw=baseline.encoded(value)
    s3.put_object(Bucket=baseline.BUCKET,Key=STATUS,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    if bounded(s3.get_object(Bucket=baseline.BUCKET,Key=STATUS)['Body'])!=raw:raise ValueError('Journal readback differs')

def capture_originals(s3,packet,entries):
    cache={}
    def fetch(key):
        if not re.fullmatch(r'data/(report-research|evidence/fred)/[A-Za-z0-9_./-]+',key) or '..' in key:raise ValueError('Canonical original path required')
        if key not in cache:
            container=bounded(s3.get_object(Bucket=baseline.BUCKET,Key=key)['Body'],canonical.MAX);raw=container
            if key.endswith('.gz'):
                with gzip.GzipFile(fileobj=io.BytesIO(container)) as stream:raw=stream.read(canonical.MAX+1)
                if len(raw)>canonical.MAX:raise ValueError('Whole original exceeds bound')
            entries[key]={'source_key':key,'stored_container':baseline.retain(s3,container),'original':baseline.retain(s3,raw)};cache[key]=raw
        return cache[key]
    return canonical.restore(packet,catalog.SERIES,fetch)

def acquire_quote():
    request=urllib.request.Request(catalog.MOVE_URL,headers={'User-Agent':'Mozilla/5.0 (JustHodl research source review)','Accept':'application/json'})
    with urllib.request.urlopen(request,timeout=35) as response:
        if response.geturl()!=catalog.MOVE_URL:raise ValueError('Unreviewed quote redirect')
        headers={k:v for k,v in response.headers.items() if k.lower() in ('date','etag','last-modified','content-type')}
        status=response.status;raw=bounded(response,8*1024*1024)
    if not raw:raise ValueError('Empty quote response')
    return raw,{'source_url':catalog.MOVE_URL,'http_status':status,'headers':headers,'acquired_at':baseline.now(),'sha256':baseline.sha(raw),'bytes':len(raw)}

def compiler_paths():
    modules=(candidate,catalog,timezone_data,independent,canonical,report_observations,research_brief_model,evidence_store)
    return {m.__name__+'.py':Path(m.__file__) for m in modules}

def isolated_replay(s3,source_ref,entries,quote_ref,quote_receipt,compilers,stamp,context,predecessor):
    paths=compiler_paths()
    if set(compilers)!=set(paths):raise ValueError('Complete reviewed compiler closure required')
    with tempfile.TemporaryDirectory(prefix='bond-vol-replay-') as directory:
        root=Path(directory);(root/'originals').mkdir()
        for name,path in paths.items():
            raw=read(s3,compilers[name])
            if raw!=path.read_bytes():raise ValueError('Reviewed compiler differs')
            (root/name).write_bytes(raw)
        for row in entries.values():(root/'originals'/row['original']['sha256']).write_bytes(read(s3,row['original']))
        (root/'source.json').write_bytes(read(s3,source_ref))
        if quote_ref:(root/'quote.bin').write_bytes(read(s3,quote_ref))
        (root/'inputs.json').write_bytes(baseline.encoded({'entries':entries,'quote':quote_ref,'quote_receipt':quote_receipt,
            'generated_at':stamp,'context':context,'predecessor':predecessor}))
        subprocess.run([sys.executable,'-I','-c',ISOLATED_REPLAY,str(root)],cwd=root,check=True,timeout=180)
        return (root/'candidate.json').read_bytes(),(root/'proof.json').read_bytes()

def main():
    import resource,time
    for test in ('test_bond_vol_candidate.py','test_bond_vol_retained_arithmetic.py'):
        subprocess.run([sys.executable,str(ROOT/'tests'/test)],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    with report('ops_6135_bond_vol_retained_arithmetic') as r:
        if '**Status:** success' not in (ROOT/'aws/ops/reports/latest/ops_6134_bond_vol_original_baseline.md').read_text(encoding='utf8'):
            raise ValueError('Accepted whole predecessor baseline required')
        original=json.loads(read(s3,BASELINE));native='aws/lambdas/'+baseline.FUNCTION+'/source/lambda_function.py'
        if original['status']!='retained' or read(s3,original['repo_predecessors'][native])!=(ROOT/native).read_bytes():
            raise ValueError('Complete unchanged original baseline required')
        before=baseline.runtime(lam,s3,events,scheduler,baseline.FUNCTION)
        if before!=original['native_predecessor']['runtime']:raise ValueError('Native runtime changed since baseline')
        progress={'status':'claimed','request_id':REQUEST,'started_at':baseline.now(),'baseline':BASELINE,'canonical_originals':{},'provider_request_attempts':0}
        journal(s3,progress,True)
        try:
            raw=bounded(s3.get_object(Bucket=baseline.BUCKET,Key='data/report-measurements.json')['Body'])
            source_ref=baseline.retain(s3,raw);source=json.loads(raw)
            if any(s not in source.get('measurements',{}) for s in catalog.SERIES):raise ValueError('All ten canonical series required')
            originals=capture_originals(s3,source,progress['canonical_originals'])
            context={'source_key':'data/funding-plumbing.json','independent_votes':0}
            try:
                raw=bounded(s3.get_object(Bucket=baseline.BUCKET,Key=context['source_key'])['Body'])
                context.update(status='retained_unqualified_context',original=baseline.retain(s3,raw),captured_at=baseline.now())
            except Exception as exc:
                if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('404','NoSuchKey'):raise
                context.update(status='missing',original=None)
            progress.update(status='quote_acquisition_attempted',provider_request_attempts=1);journal(s3,progress)
            try:quote_raw,receipt=acquire_quote()
            except Exception as exc:
                quote_raw=receipt=None
                progress['quote_acquisition']={'status':'unavailable','error_type':type(exc).__name__,'http_status':getattr(exc,'code',None)}
            else:progress['quote_acquisition']={'status':'received'}
            quote_ref=baseline.retain(s3,quote_raw) if quote_raw else None
            receipt_ref=baseline.retain(s3,baseline.encoded(receipt)) if receipt else None
            progress.update(quote=quote_ref,quote_receipt=receipt_ref);journal(s3,progress)
            stamp=baseline.now();started=time.monotonic();predecessor=original['captures']['data/bond-vol.json']['original']
            output=candidate.build(source,originals,stamp,context,predecessor,quote_raw,receipt)
            proof=independent.verify(output,source,originals,quote_raw,receipt)
            if proof['reconstructed_series']!=10 or proof['current_series']!=10:raise ValueError('All ten rate/spread sources must qualify')
            profile={'compile_and_verify_seconds':round(time.monotonic()-started,3),'candidate_bytes':len(report_observations.encoded(output)),
                'max_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
            compilers={name:baseline.retain(s3,path.read_bytes()) for name,path in compiler_paths().items()}
            output_ref=baseline.retain(s3,report_observations.encoded(output));proof_ref=baseline.retain(s3,report_observations.encoded(proof))
            del output,originals,source,quote_raw
            started=time.monotonic()
            rebuilt,rechecked=isolated_replay(s3,source_ref,progress['canonical_originals'],quote_ref,receipt,compilers,stamp,context,predecessor)
            if rebuilt!=read(s3,output_ref) or rechecked!=read(s3,proof_ref):raise ValueError('Isolated whole candidate replay differs')
            profile.update(isolated_replay_seconds=round(time.monotonic()-started,3),
                max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,replay_child_max_rss_kib=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
            population={'contract':'bond-vol-qualified-population.v1','baseline':BASELINE,'source':source_ref,
                'canonical_originals':progress['canonical_originals'],'quote':quote_ref,'quote_receipt':receipt_ref,
                'context':context,'predecessor':predecessor,'compilers':compilers,'candidate':output_ref,'proof':proof_ref,'generated_at':stamp}
            manifest=baseline.retain(s3,baseline.encoded(population))
            protected={STATUS,manifest['key'],source_ref['key'],output_ref['key'],proof_ref['key'],*[v['key'] for v in compilers.values()]}
            for ref in (quote_ref,receipt_ref,context.get('original')):
                if ref:protected.add(ref['key'])
            for row in progress['canonical_originals'].values():protected.update((row['stored_container']['key'],row['original']['key']))
            outcomes=[access.check(key) for key in sorted(protected)]
            audit=baseline.retain(s3,baseline.encoded({'outcomes':outcomes}));outcomes.append(access.check(audit['key']))
            privacy=access.summarize(outcomes)
            if not privacy['all_denied']:raise ValueError('Qualification evidence must remain private')
            if baseline.runtime(lam,s3,events,scheduler,baseline.FUNCTION)!=before:raise ValueError('Native runtime changed during qualification')
            result={'manifest':manifest,'baseline':BASELINE,'candidate':output_ref,'proof_reference':proof_ref,'proof':proof,
                'compilers':compilers,'profile':profile,'privacy':privacy,'access_evidence':audit,'quote':quote_ref,
                'quote_receipt':receipt_ref,'quote_acquisition':progress['quote_acquisition'],'generated_at':stamp,
                'all_original_rows_retained':True,'fresh_process_replay_verified':True,'native_package_unchanged':True,
                'native_formula_changed':False,'provider_requests':1,'producer_invocations':0,'consumer_invocations':0,
                'public_writes':0,'private_account_reads':0,'schedules_changed':0,'notifications_sent':0,
                'paid_ai_calls':0,'signal_writes':0,'forecast_qualified':False,'sizing_qualified':False}
            journal(s3,{**progress,'status':'complete','result':result});r.kv(**result)
        except Exception as exc:
            journal(s3,{**progress,'status':'failed','error_type':type(exc).__name__});raise

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
