"""Accept bounded flow parsing from retained originals; refresh only public radar.

No provider collection, account reads, paid AI, decisions or notifications.
The canonical publication and all fourteen aliases must remain byte-identical.
"""
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import json, re, subprocess, sys
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared'),str(ROOT/'scripts')]
from ops_report import report
import ops_5974_provider_fund_flow_native_acceptance as acceptance
import provider_flow_store as store
import provider_flow_model as model

COMMIT='c8d821e8ca9f8a2b876a6bb6f23d0d00e136d5c6'
BUCKET='justhodl-dashboard-live'
PROOF_KEY='data/provider-flow-research-verification.json'
CANONICAL_SHA='c3513b011de432947158488d1e2606fa91ee252ed2b1d064a42274fd12f661d9'
acceptance.COMMIT=COMMIT
acceptance.FUNCTIONS=acceptance.PRODUCERS
# Remove collection authority from the reused durable dispatch helper.
acceptance.KINDS={'justhodl-capital-flow-radar':'radar'}


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=60,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5983_provider_flow_parser_acceptance') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_provider_flow_acceptance.py')],cwd=ROOT,check=True)
        for fn in acceptance.PRODUCERS:subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/fn/'tests/run_tests.py')],cwd=ROOT,check=True)
        runtimes={fn:acceptance.runtime(lam,fn) for fn in acceptance.FUNCTIONS};r.kv(runtimes=runtimes)
        old_raw=acceptance.public(PROOF_KEY);previous=json.loads(old_raw)
        assert previous['commit']=='f93fac34e83a2296f969f67694befe1e7b92d771'
        retained_previous=store.protect(s3,BUCKET,old_raw);protected={retained_previous['key']}
        raw=acceptance.public(model.CURRENT);assert model.sha(raw)==CANONICAL_SHA
        canonical=json.loads(raw);read=store.reader(s3,BUCKET);assert read(model.CURRENT)==raw
        canonical_replay=acceptance.replay_verify(canonical,read)
        assert canonical_replay['no_allocation_authority'] and canonical_replay['configured_funds']==300
        audit,audit_replays=acceptance.preflight_replay(read,protected)
        independent=acceptance.independent_arithmetic(canonical,read)
        assert independent['counts']==previous['independent_source_arithmetic']['counts']
        run=json.loads(read(canonical['replay']['manifest_key']));inputs=store.checked(run['input'],model.PREFIX,'inputs',read)
        refs=[*inputs['contexts'].values(),inputs['previous']]
        for collection in inputs['collections'].values():
            refs.extend(page['original'] for page in collection['pages'])
            if collection.get('rejected_original'):refs.append(collection['rejected_original'])
        for ref in refs:
            if ref:store.protected(ref,read);protected.add(ref['key'])
        migration=json.loads(read(store.MIGRATION));store.protected(migration,read);protected.add(migration['key'])
        before={}
        for key in store.ALIASES:
            saved=read(key);served=acceptance.public(key)
            assert json.loads(saved)==model.compatibility(canonical,key)
            assert json.loads(served)==model.compatibility(canonical,acceptance.public_alias_target(key))
            before[key]=(saved,served)
        schedules=[]
        for fn in acceptance.PRODUCERS:
            cfg=lam.get_function_configuration(FunctionName=fn);names=[]
            for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
            expected={v['name']:v for v in previous['schedules'] if v['function']==fn}
            assert set(names)==set(expected)
            for name in names:
                rule=events.describe_rule(Name=name)
                assert rule['State']==expected[name]['state'] and rule['ScheduleExpression']==expected[name]['expression']
                schedules.append(expected[name])
        build=json.loads(acceptance.public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor','0e8bcac26c2d1bf61916a72235b923dfca108c70',pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet','HEAD',pages_commit,'--',*acceptance.ASSETS],cwd=ROOT,check=True)
        for name in acceptance.ASSETS:
            body=acceptance.public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',body)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name]
            if name.endswith('.html'):assert b'jh-provider-flows.js?v=20260921-native2' in body
        r.kv(canonical_originals_replayed=True,independent_counts=independent['counts'],protected_evidence_retained=len(protected))
        fn='justhodl-capital-flow-radar';request=acceptance.invoke_public(lam,s3,store,fn)
        assert request['status']['provider_requests']==0
        profile=acceptance.execution_profile(boto3.client('logs',region_name='us-east-1'),fn,request['status'])
        radar_raw=acceptance.public(model.RADAR_CURRENT);radar=json.loads(radar_raw)
        assert read(model.RADAR_CURRENT)==radar_raw and radar['generated_at']==request['status']['generated_at']
        assert model.clock(radar['generated_at'])>model.clock(previous['source_replays']['radar']['generated_at'])
        replay=acceptance.replay_verify(radar,read)
        assert radar['canonical_replay']==canonical['replay'] and radar['source_generated_at']==canonical['generated_at']
        for field in ('funds','complexes','comparisons','unique_leveraged','unique_configured_universe','reference','source_valid_until'):
            assert radar[field]==canonical[field]
        assert acceptance.public(model.CURRENT)==raw and read(model.CURRENT)==raw
        aliases={}
        for key,(saved,served) in before.items():
            assert read(key)==saved and acceptance.public(key)==served
            aliases[key]={**previous['compatibility_aliases'][key],'unchanged':True}
        run=json.loads(read(radar['replay']['manifest_key']));inputs=store.checked(run['input'],model.RADAR_PREFIX,'inputs',read)
        for ref in (inputs['canonical_source'],inputs['previous']):
            if ref:store.protected(ref,read);protected.add(ref['key'])
        def denied(key):assert acceptance.denied('https://justhodl.ai/'+key) and acceptance.denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=8) as pool:
            for _ in pool.map(denied,sorted(protected)):pass
        publications={'flow':previous['publications']['flow'],'radar':{'key':model.RADAR_CURRENT,'sha256':model.sha(radar_raw),'bytes':len(radar_raw),'replay':radar['replay']}}
        proof={'contract':'provider-flow-parser-acceptance.v1','generated_at':datetime.now(timezone.utc).isoformat(),'commit':COMMIT,
            'runtime_packages':runtimes,'publications':publications,'source_replays':{'flow':canonical_replay,'radar':replay},
            'requests':{fn:request},'execution_profiles':{fn:profile},'independent_source_arithmetic':independent,
            'canonical_body_unchanged':True,'canonical_originals_verified_under_current_parser':True,'retained_previous_acceptance':retained_previous,
            'audit_originals_replayed':audit_replays,'whole_predecessors_preserved':True,'schedules':schedules,'compatibility_aliases':aliases,
            'protected_artifacts_checked':len(protected),'protected_artifacts_anonymously_denied':True,'radar_is_same_canonical_evidence':True,
            'producer_invocations_this_acceptance':int(request['invoke_sent']),'provider_collection_invocations':0,'provider_requests_this_acceptance':0,
            'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'signals_emitted':0,'portfolio_writes':0,'unreviewed_consumer_invocations':0,
            'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in acceptance.ASSETS}}
        body=model.encoded(proof);s3.put_object(Bucket=BUCKET,Key=PROOF_KEY,Body=body,ContentType='application/json',CacheControl='no-store')
        assert acceptance.public(PROOF_KEY)==body
        r.kv(proof_key=PROOF_KEY,publications=publications,execution_profile=profile,independent_counts=independent['counts'],canonical_body_unchanged=True,
            protected_artifacts_checked=len(protected),compatibility_aliases_unchanged=len(aliases),provider_requests_this_acceptance=0,
            provider_collection_invocations=0,producer_invocations_this_acceptance=int(request['invoke_sent']),private_account_reads=0,
            paid_ai_calls=0,notifications_sent=0,signals_emitted=0,portfolio_writes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
