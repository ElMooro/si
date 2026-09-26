"""Retain the omitted SP500 original and operational cache before migration.

Adds to the accepted 6121 baseline. No provider acquisition, native invocation,
public publication, schedule/package mutation, account access or paid AI.
"""
from pathlib import Path
import gzip,io,json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/checks','aws/ops/staged')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from liquidity_agent_input_inventory import inventory
import canonical_fred_replay as canonical
import retained_access_evidence as access
import ops_6121_liquidity_agent_original_baseline as baseline

BUCKET=baseline.BUCKET
BASELINE={'key':baseline.PRIVATE+'f828450ee02c450e848b172755f3dc1735121fb3a684f4a52dfcae32f13b2c3b.bin',
          'sha256':'f828450ee02c450e848b172755f3dc1735121fb3a684f4a52dfcae32f13b2c3b','bytes':143374}
REQUEST='chatgpt-liquidity-agent-scope-completion-6124'
STATUS=baseline.PRIVATE+'requests/'+baseline.sha(REQUEST.encode())+'.json'


def read_original(s3,ref):
    if (not isinstance(ref,dict) or ref.get('key')!=baseline.PRIVATE+str(ref.get('sha256'))+'.bin'
            or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256')))):
        raise ValueError('Exact protected baseline reference required')
    raw=bounded(s3.get_object(Bucket=BUCKET,Key=ref['key'])['Body'])
    if len(raw)!=ref.get('bytes') or baseline.sha(raw)!=ref['sha256']:
        raise ValueError('Complete original baseline bytes differ')
    return raw


def journal(s3,value,claim=False):
    raw=baseline.encoded(value)
    s3.put_object(Bucket=BUCKET,Key=STATUS,Body=raw,ContentType='application/json',CacheControl='no-store',
                  **({'IfNoneMatch':'*'} if claim else {}))
    if bounded(s3.get_object(Bucket=BUCKET,Key=STATUS)['Body'])!=raw:
        raise ValueError('Scope completion journal differs')


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_liquidity_agent_input_inventory.py')],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    with report('ops_6124_liquidity_agent_scope_completion') as r:
        original=json.loads(read_original(s3,BASELINE))
        native='aws/lambdas/'+baseline.FUNCTION+'/source/lambda_function.py'
        raw=read_original(s3,original['repo_predecessors'][native])
        if raw!=(ROOT/native).read_bytes():raise ValueError('Native source changed since accepted baseline')
        scope=inventory(raw)
        if scope['unresolved_requests'] or tuple(scope['catalog_series'])!=baseline.SERIES or scope['additional_literal_series']!=['SP500']:
            raise ValueError('Review complete input inventory before continuing')
        before=runtime(lam,s3,events,scheduler,baseline.FUNCTION)
        if before!=original['native_predecessor']['runtime']:raise ValueError('Native runtime changed since baseline')
        progress={'request_id':REQUEST,'contract':'liquidity-agent-scope-completion.v1','status':'claimed',
                  'started_at':baseline.now(),'baseline':BASELINE,'inventory':scope,'retained_sources':{}}
        journal(s3,progress,True)
        try:
            packet=json.loads(read_original(s3,original['captures']['data/report-measurements.json']['original']))
            def read(key):
                if key in original['canonical_originals']:
                    return read_original(s3,original['canonical_originals'][key]['original'])
                if not re.fullmatch(r'data/evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz',key):
                    raise ValueError('Reviewed additional canonical original required')
                body=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'],canonical.MAX)
                container=baseline.retain(s3,body)
                with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:raw=stream.read(canonical.MAX+1)
                if len(raw)>canonical.MAX:raise ValueError('Whole source exceeds bound')
                progress['retained_sources'][key]={'source_key':key,'stored_container':container,'original':baseline.retain(s3,raw)}
                return raw
            restored=canonical.restore(packet,scope['additional_literal_series'],read)
            summary={}
            for sid,item in restored.items():
                if item is None:
                    summary[sid]={'status':'missing_canonical_series'};continue
                meta=item['definition']['seriess'][0];rows=item['observations']['observations']
                summary[sid]={'status':'reconstructed_from_whole_originals','original_row_count':len(rows),
                    'native_unit':meta['units'],'frequency':meta['frequency'],'frequency_short':meta['frequency_short'],
                    'seasonal_adjustment':meta['seasonal_adjustment'],'acquired_at':item['acquired_at'],
                    'definition':meta,'evidence':item['evidence'],'source_quality':packet['measurements'][sid]['quality']}
            cache_key='data/fred-cache.json'
            try:
                response=s3.get_object(Bucket=BUCKET,Key=cache_key);body=bounded(response['Body'])
                progress['legacy_cache']={'status':'retained_unqualified_context','source_key':cache_key,
                    'original':baseline.retain(s3,body),'captured_at':baseline.now(),
                    'same_snapshot_as_baseline':False,'used_as_canonical_original':False}
            except Exception as exc:
                if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('404','NoSuchKey'):raise
                progress['legacy_cache']={'status':'missing','source_key':cache_key}
            progress.update(status='retained',source_summary=summary,source_replay=packet['replay'])
            journal(s3,progress);manifest=baseline.retain(s3,baseline.encoded(progress))
            protected={STATUS,manifest['key']}
            if progress['legacy_cache'].get('original'):protected.add(progress['legacy_cache']['original']['key'])
            for row in progress['retained_sources'].values():protected.update((row['stored_container']['key'],row['original']['key']))
            outcomes=[access.check(key) for key in sorted(protected)]
            audit=baseline.retain(s3,baseline.encoded({'outcomes':outcomes}));outcomes.append(access.check(audit['key']))
            privacy=access.summarize(outcomes)
            if not privacy['all_denied']:raise ValueError('Additional originals must remain private')
            if runtime(lam,s3,events,scheduler,baseline.FUNCTION)!=before:raise ValueError('Native runtime changed during scope completion')
            journal(s3,{**progress,'status':'complete','completed_at':baseline.now(),'manifest':manifest,'privacy':privacy})
            r.kv(manifest=manifest,baseline=BASELINE,inventory=scope,additional_source_summary=summary,
                legacy_cache=progress['legacy_cache'],access_evidence=audit,privacy=privacy,
                native_package_unchanged=True,native_formula_changed=False,provider_requests=0,
                producer_invocations=0,consumer_invocations=0,public_writes=0,private_account_reads=0,
                notifications_sent=0,paid_ai_calls=0,schedules_changed=0,forecast_qualified=False,sizing_qualified=False)
        except Exception as exc:
            journal(s3,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
