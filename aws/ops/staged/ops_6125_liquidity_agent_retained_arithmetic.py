"""Qualify all 73 requested identities against accepted whole original responses.

Private evidence only. No provider request, native invocation, current head,
account, schedule, paid AI, notification or order is read or changed.
"""
from pathlib import Path
import json, re, subprocess, sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/checks','aws/ops/staged','scripts',
    'aws/lambdas/justhodl-liquidity-flow/source')]
from ops_report import report
from market_runtime_evidence import runtime, bounded
import ops_6121_liquidity_agent_original_baseline as baseline
import ops_6124_liquidity_agent_scope_completion as scope_operation
import liquidity_agent_catalog as catalog
import liquidity_agent_candidate as candidate
import verify_liquidity_agent_arithmetic as independent
import canonical_fred_replay as canonical
import report_observations, research_brief_model, evidence_store
import retained_access_evidence as access

REQUEST = 'chatgpt-liquidity-agent-retained-arithmetic-6125'
STATUS = baseline.PRIVATE+'requests/'+baseline.sha(REQUEST.encode())+'.json'
SCOPE = {'key':baseline.PRIVATE+'3ccd3cd4810a9f533a47868a3b3cae2c0ff515da95ab9257be82d035ee155f87.bin',
    'sha256':'3ccd3cd4810a9f533a47868a3b3cae2c0ff515da95ab9257be82d035ee155f87','bytes':15916}
BASELINE = scope_operation.BASELINE


def read(s3, ref):
    if not candidate.original_ref(ref):
        raise ValueError('Exact protected original reference required')
    raw = bounded(s3.get_object(Bucket=baseline.BUCKET, Key=ref['key'])['Body'])
    if len(raw) != ref['bytes'] or baseline.sha(raw) != ref['sha256']:
        raise ValueError('Complete retained bytes differ')
    return raw


def journal(s3, value, claim=False):
    raw = baseline.encoded(value)
    s3.put_object(Bucket=baseline.BUCKET, Key=STATUS, Body=raw, ContentType='application/json',
        CacheControl='no-store', **({'IfNoneMatch':'*'} if claim else {}))
    if bounded(s3.get_object(Bucket=baseline.BUCKET, Key=STATUS)['Body']) != raw:
        raise ValueError('Operation journal readback differs')


def reconstruction(s3, original, extension):
    source = json.loads(read(s3, original['captures']['data/report-measurements.json']['original']))
    entries = {**original['canonical_originals'], **extension['retained_sources']}
    def fetch(key):
        if key not in entries or entries[key]['source_key'] != key:
            raise ValueError('Only accepted whole original sources are allowed')
        return read(s3, entries[key]['original'])
    return source, canonical.restore(source, catalog.SERIES, fetch)


def main():
    import resource, time
    for test in ('test_liquidity_agent_candidate.py','test_liquidity_agent_retained_arithmetic.py'):
        subprocess.run([sys.executable,str(ROOT/'tests'/test)],cwd=ROOT,check=True)
    s3, lam, events, scheduler = (boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    with report('ops_6125_liquidity_agent_retained_arithmetic') as r:
        for name in ('ops_6121_liquidity_agent_original_baseline','ops_6124_liquidity_agent_scope_completion'):
            if '**Status:** success' not in (ROOT/'aws/ops/reports/latest'/f'{name}.md').read_text(encoding='utf-8'):
                raise ValueError('Accepted predecessor evidence required')
        original = json.loads(read(s3,BASELINE)); extension = json.loads(read(s3,SCOPE))
        if original['status'] != 'retained' or extension['status'] != 'retained' or extension['baseline'] != BASELINE:
            raise ValueError('Matching complete retained populations required')
        if tuple(extension['inventory']['all_series']) != catalog.SERIES or extension['inventory']['unresolved_requests']:
            raise ValueError('Complete reviewed predecessor scope required')
        native = 'aws/lambdas/'+baseline.FUNCTION+'/source/lambda_function.py'
        if read(s3,original['repo_predecessors'][native]) != (ROOT/native).read_bytes():
            raise ValueError('Native producer changed since baseline')
        before = runtime(lam,s3,events,scheduler,baseline.FUNCTION)
        if before != original['native_predecessor']['runtime']:
            raise ValueError('Native runtime changed since baseline')
        progress = {'status':'claimed','request_id':REQUEST,'started_at':baseline.now(),'baseline':BASELINE,'scope':SCOPE}
        journal(s3,progress,True)
        try:
            started = time.monotonic(); source, originals = reconstruction(s3,original,extension)
            restore_seconds = time.monotonic()-started
            contexts = {}
            for key in catalog.CONTEXT_KEYS:
                item = extension['legacy_cache'] if key == 'data/fred-cache.json' else original['captures'][key]
                if item.get('status') == 'missing':
                    contexts[key] = {'status':'missing','original':None,'independent_votes':0}
                else:
                    # Retention must still be whole and hash-bound even when not used as arithmetic input.
                    read(s3,item['original'])
                    contexts[key] = {'status':'retained_unqualified_context','source_key':key,'original':item['original'],
                        'captured_at':item['captured_at'],'independent_votes':0,'same_snapshot_as_baseline':key != 'data/fred-cache.json'}
            stamp = baseline.now(); started = time.monotonic()
            output = candidate.build(source,originals,stamp,contexts,original['captures']['liquidity-data.json']['original'])
            proof = independent.verify(output,source,originals)
            if (proof['requested_series'],proof['reconstructed_series'],proof['missing_series'],proof['original_rows']) != (73,61,12,145589):
                raise ValueError('Complete accepted original population differs')
            if proof['calendar_comparisons'] != 244:
                raise ValueError('Every available calendar comparison must be checked')
            profile = {'restore_seconds':round(restore_seconds,3),'compile_and_verify_seconds':round(time.monotonic()-started,3),
                'candidate_bytes':len(baseline.encoded(output)),'max_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
            modules = (candidate,catalog,independent,independent.flow_verifier,candidate.flow,canonical,
                report_observations,research_brief_model,evidence_store)
            compilers = {module.__name__:baseline.retain(s3,Path(module.__file__).read_bytes()) for module in modules}
            output_ref = baseline.retain(s3,baseline.encoded(output))
            # Reconstruct from a fresh original reader and compare the exact full output.
            del source, originals, output
            started = time.monotonic(); source, originals = reconstruction(s3,original,extension)
            retained = json.loads(read(s3,output_ref))
            replay = candidate.build(source,originals,stamp,contexts,original['captures']['liquidity-data.json']['original'])
            if baseline.sha(baseline.encoded(replay)) != output_ref['sha256'] or independent.verify(retained,source,originals) != proof:
                raise ValueError('Fresh-reader complete candidate replay differs')
            profile.update(fresh_reader_replay_seconds=round(time.monotonic()-started,3),
                max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            proof_ref = baseline.retain(s3,baseline.encoded(proof))
            artifacts = {'candidate':output_ref,'proof':proof_ref}
            protected = {STATUS,*[v['key'] for v in compilers.values()],output_ref['key'],proof_ref['key']}
            outcomes = [access.check(key) for key in sorted(protected)]
            artifacts['access_evidence'] = baseline.retain(s3,baseline.encoded({'outcomes':outcomes}))
            outcomes.append(access.check(artifacts['access_evidence']['key']))
            privacy = access.summarize(outcomes)
            if not privacy['all_denied']:
                raise ValueError('All new evidence must remain private')
            if runtime(lam,s3,events,scheduler,baseline.FUNCTION) != before:
                raise ValueError('Native runtime changed during qualification')
            final = {'baseline':BASELINE,'scope':SCOPE,'artifacts':artifacts,'compilers':compilers,'proof':proof,
                'profile':profile,'privacy':privacy,'quality':retained['quality'],'generated_at':stamp,
                'source_generated_at':source['generated_at'],'source_replay':source['replay'],
                'all_original_rows_retained':True,'fresh_reader_replay_verified':True,
                'series_evidence':{sid:{'status':row['quality']['status'],'original_rows':len(row['history']),
                    'unit':row['unit'],'frequency':row['frequency'],'observation_date':row['latest_date'],
                    'acquired_at':row['acquired_at']} for sid,row in retained['series'].items()},
                'native_package_unchanged':True,'native_formula_changed':False,'provider_requests':0,'producer_invocations':0,
                'consumer_invocations':0,'public_writes':0,'private_account_reads':0,'schedules_changed':0,
                'notifications_sent':0,'paid_ai_calls':0,'signal_writes':0,'forecast_qualified':False,'sizing_qualified':False}
            journal(s3,{**progress,'status':'complete','result':final});r.kv(**final)
        except Exception as exc:
            journal(s3,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__ == '__main__':
    try:main()
    except Exception:sys.exit(1)
