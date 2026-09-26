"""Qualify the complete 23-series curve against retained canonical originals.

Private artifacts only. No new provider request, producer/consumer invocation,
native mutation, account, notification, paid AI, signal or schedule operation.
"""
from pathlib import Path
import gzip,io,json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/checks','aws/ops/staged','scripts')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
import ops_6127_yield_curve_original_baseline as baseline
import yield_curve_candidate as candidate
import yield_curve_catalog as catalog
import verify_yield_curve_arithmetic as independent
import canonical_fred_replay as canonical
import report_observations,research_brief_model,evidence_store
import retained_access_evidence as access

REQUEST='chatgpt-yield-curve-retained-arithmetic-6128'
STATUS=baseline.PRIVATE+'requests/'+baseline.sha(REQUEST.encode())+'.json'
BASELINE={'key':baseline.PRIVATE+'c6538de44a68d0b17a21f2cd7385ad00f706ab3a063bc8401d9668b41e2675c5.bin',
    'sha256':'c6538de44a68d0b17a21f2cd7385ad00f706ab3a063bc8401d9668b41e2675c5','bytes':58311}


def read(s3,ref):
    if not candidate.original_ref(ref):raise ValueError('Exact protected reference required')
    raw=bounded(s3.get_object(Bucket=baseline.BUCKET,Key=ref['key'])['Body'])
    if len(raw)!=ref['bytes'] or baseline.sha(raw)!=ref['sha256']:raise ValueError('Complete retained bytes differ')
    return raw


def journal(s3,value,claim=False):
    raw=baseline.encoded(value)
    s3.put_object(Bucket=baseline.BUCKET,Key=STATUS,Body=raw,ContentType='application/json',
        CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    if bounded(s3.get_object(Bucket=baseline.BUCKET,Key=STATUS)['Body'])!=raw:raise ValueError('Journal readback differs')


def capture_originals(s3,packet,entries):
    cache={}
    def fetch(key):
        if not re.fullmatch(r'data/(report-research|evidence/fred)/[A-Za-z0-9_./-]+',key) or '..' in key:
            raise ValueError('Canonical original path required')
        if key not in cache:
            container=bounded(s3.get_object(Bucket=baseline.BUCKET,Key=key)['Body'],canonical.MAX)
            raw=container
            if key.endswith('.gz'):
                with gzip.GzipFile(fileobj=io.BytesIO(container)) as stream:raw=stream.read(canonical.MAX+1)
                if len(raw)>canonical.MAX:raise ValueError('Whole original exceeds bound')
            entries[key]={'source_key':key,'stored_container':baseline.retain(s3,container),'original':baseline.retain(s3,raw)}
            cache[key]=raw
        return cache[key]
    return canonical.restore(packet,catalog.SERIES,fetch)


def replay(s3,source_ref,entries):
    packet=json.loads(read(s3,source_ref))
    def fetch(key):
        if key not in entries or entries[key]['source_key']!=key:raise ValueError('Only pinned whole originals allowed')
        return read(s3,entries[key]['original'])
    return packet,canonical.restore(packet,catalog.SERIES,fetch)


def main():
    import resource,time
    for test in ('test_yield_curve_candidate.py','test_yield_curve_retained_arithmetic.py'):
        subprocess.run([sys.executable,str(ROOT/'tests'/test)],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    with report('ops_6128_yield_curve_retained_arithmetic') as r:
        if '**Status:** success' not in (ROOT/'aws/ops/reports/latest/ops_6127_yield_curve_original_baseline.md').read_text(encoding='utf8'):
            raise ValueError('Accepted baseline required')
        original=json.loads(read(s3,BASELINE));native='aws/lambdas/'+baseline.FUNCTION+'/source/lambda_function.py'
        if original['status']!='retained' or tuple(original['source_summary'])==():raise ValueError('Complete original baseline required')
        if read(s3,original['repo_predecessors'][native])!=(ROOT/native).read_bytes():raise ValueError('Native source changed since baseline')
        before=runtime(lam,s3,events,scheduler,baseline.FUNCTION)
        if before!=original['native_predecessor']['runtime']:raise ValueError('Native runtime changed since baseline')
        progress={'status':'claimed','request_id':REQUEST,'started_at':baseline.now(),'baseline':BASELINE,'canonical_originals':{}}
        journal(s3,progress,True)
        try:
            body=bounded(s3.get_object(Bucket=baseline.BUCKET,Key='data/report-measurements.json')['Body'])
            source_ref=baseline.retain(s3,body);source=json.loads(body)
            if any(sid not in source.get('measurements',{}) for sid in catalog.SERIES):
                raise ValueError('All 23 roots, including DFII7 and DFII20, must have arrived on the normal collector')
            started=time.monotonic();originals=capture_originals(s3,source,progress['canonical_originals'])
            restore_seconds=time.monotonic()-started
            context={'source_key':'data/term-premium.json','independent_votes':0}
            try:
                body=bounded(s3.get_object(Bucket=baseline.BUCKET,Key=context['source_key'])['Body'])
                context.update(status='retained_unqualified_context',original=baseline.retain(s3,body),captured_at=baseline.now())
            except Exception as exc:
                if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('404','NoSuchKey'):raise
                context.update(status='missing',original=None)
            stamp=baseline.now();started=time.monotonic()
            predecessor=original['captures']['data/yield-curve.json']['original']
            output=candidate.build(source,originals,stamp,context,predecessor)
            proof=independent.verify(output,source,originals)
            if proof['reconstructed_series']!=23 or proof['current_series']!=23 or proof['missing_series']:
                raise ValueError('Every requested original must be reconstructed and within the reviewed age ceiling')
            if proof['matched_observation_comparisons']!=140:raise ValueError('Complete dated comparison inventory required')
            profile={'restore_seconds':round(restore_seconds,3),'compile_and_verify_seconds':round(time.monotonic()-started,3),
                'candidate_bytes':len(baseline.encoded(output)),'max_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
            modules=(candidate,catalog,independent,canonical,report_observations,research_brief_model,evidence_store)
            compilers={module.__name__:baseline.retain(s3,Path(module.__file__).read_bytes()) for module in modules}
            output_ref=baseline.retain(s3,baseline.encoded(output));proof_ref=baseline.retain(s3,baseline.encoded(proof))
            del source,originals,output
            started=time.monotonic();source,originals=replay(s3,source_ref,progress['canonical_originals'])
            rebuilt=candidate.build(source,originals,stamp,context,predecessor)
            if baseline.sha(baseline.encoded(rebuilt))!=output_ref['sha256']:
                raise ValueError('Fresh-reader whole candidate replay differs')
            if independent.verify(json.loads(read(s3,output_ref)),source,originals)!=proof:
                raise ValueError('Retained output independent checks differ')
            profile.update(fresh_reader_replay_seconds=round(time.monotonic()-started,3),max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            population={'contract':'yield-curve-qualified-population.v1','baseline':BASELINE,'source':source_ref,
                'canonical_originals':progress['canonical_originals'],'context':context,'predecessor':predecessor,
                'compilers':compilers,'candidate':output_ref,'proof':proof_ref,'generated_at':stamp}
            manifest=baseline.retain(s3,baseline.encoded(population))
            protected={STATUS,manifest['key'],source_ref['key'],output_ref['key'],proof_ref['key'],*[v['key'] for v in compilers.values()]}
            if context['original']:protected.add(context['original']['key'])
            for value in progress['canonical_originals'].values():protected.update((value['stored_container']['key'],value['original']['key']))
            outcomes=[access.check(key) for key in sorted(protected)]
            audit=baseline.retain(s3,baseline.encoded({'outcomes':outcomes}));outcomes.append(access.check(audit['key']))
            privacy=access.summarize(outcomes)
            if not privacy['all_denied']:raise ValueError('New evidence must remain private')
            if runtime(lam,s3,events,scheduler,baseline.FUNCTION)!=before:raise ValueError('Native runtime changed during qualification')
            result={'manifest':manifest,'baseline':BASELINE,'candidate':output_ref,'proof_reference':proof_ref,
                'proof':proof,'compilers':compilers,'profile':profile,'privacy':privacy,'access_evidence':audit,
                'source_generated_at':source['generated_at'],'source_replay':source['replay'],'generated_at':stamp,
                'series_evidence':{s:{'definition':row['source_definition'],'original_rows':len(row['history']),
                    'latest_date':row['latest_date'],'acquired_at':row['acquired_at'],'status':row['quality']['status']}
                    for s,row in rebuilt['series'].items()},
                'all_original_rows_retained':True,'fresh_reader_replay_verified':True,'native_package_unchanged':True,
                'native_formula_changed':False,'provider_requests':0,'producer_invocations':0,'consumer_invocations':0,
                'public_writes':0,'private_account_reads':0,'schedules_changed':0,'notifications_sent':0,
                'paid_ai_calls':0,'signal_writes':0,'forecast_qualified':False,'sizing_qualified':False}
            journal(s3,{**progress,'status':'complete','result':result});r.kv(**result)
        except Exception as exc:
            journal(s3,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
