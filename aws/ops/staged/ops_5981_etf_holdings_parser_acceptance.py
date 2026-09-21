"""Verify parser hardening against retained originals; invoke only public lookthrough.

No holdings recollection, private account, paid AI, decisions or notifications.
The existing canonical publication and all compatibility aliases remain exact.
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
import ops_5980_etf_holdings_recovery_acceptance as acceptance
import etf_holdings_store as store
import etf_holdings_model as model

COMMIT='28a7956a24e7af09bd65e553f3f7566c0592ea82'
PAGE_COMMIT='9e7758266dbfb098770d1ea637885f302537b4da'
BUCKET='justhodl-dashboard-live'
CANONICAL_SHA='0210e1967b0a7a7635c39b65d5b8f4e7f631958eb21b37db3be506da36012f4c'
PROOF_KEY='data/etf-holdings-research-verification.json'
acceptance.COMMIT=COMMIT


def warm_refs(refs, read):
    """Bounded concurrent immutable reads; futures retain no response bodies."""
    unique={}
    for ref in refs:
        if ref:
            assert ref['key'] not in unique or unique[ref['key']]==ref
            unique[ref['key']]=ref
    assert len(unique)<=10000
    def one(ref):
        raw=read(ref['key'])
        assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256']
    with ThreadPoolExecutor(max_workers=8) as pool:
        for _ in pool.map(one, unique.values()): pass
    return len(unique)


def warm_canonical(packet, read, protected):
    run=json.loads(read(packet['replay']['manifest_key']))
    inputs=store.checked(run['input'],model.PREFIX,'inputs',read)
    originals=[ref for ref in [*inputs['contexts'].values(),inputs['previous']] if ref]
    for pair in inputs['collections'].values():
        for collection in pair.values():
            for page in [collection.get('selection'),*collection.get('pages',[])]:
                if page and page.get('original'): originals.append(page['original'])
            if collection.get('rejected_original'): originals.append(collection['rejected_original'])
    protected.update(ref['key'] for ref in originals)
    original_count=warm_refs(originals,read)
    metadata=[packet['security_directory']]
    for fund in packet['funds'].values(): metadata += [fund['current']['snapshot'],fund['prior']['snapshot'],fund['comparison']]
    metadata_count=warm_refs(metadata,read); parts=[]
    for ref in metadata:
        doc=json.loads(read(ref['key']))
        parts += doc.get('parts',[])+doc.get('index_parts',[])+list(doc.get('buckets',{}).values())
    return {'originals':original_count,'metadata':metadata_count,'row_index_comparison_membership_parts':warm_refs(parts,read)}


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=60,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5981_etf_holdings_parser_acceptance') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_etf_holdings_acceptance.py')],cwd=ROOT,check=True)
        for fn in acceptance.PRODUCERS: subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/fn/'tests/run_tests.py')],cwd=ROOT,check=True)
        runtimes={fn:acceptance.runtime(lam,fn) for fn in acceptance.FUNCTIONS};r.kv(runtimes=runtimes)
        proof_raw=acceptance.public(PROOF_KEY);previous=json.loads(proof_raw)
        assert previous['commit']=='bfa8c58127b88b0d5d2760539ca9f10cb0c01111'
        retained_previous=store.protect(s3,BUCKET,proof_raw);protected={retained_previous['key']}
        canonical_raw=acceptance.public(model.CURRENT);assert model.sha(canonical_raw)==CANONICAL_SHA
        canonical=json.loads(canonical_raw);read=store.reader(s3,BUCKET)
        assert read(model.CURRENT)==canonical_raw
        inventory=warm_canonical(canonical,read,protected);r.kv(warmed_immutable_evidence=inventory)
        canonical_replay=acceptance.replay_verify(canonical,read)
        assert canonical_replay['configured_funds']==300 and canonical_replay['no_allocation_authority']
        audit,audit_replays=acceptance.preflight_replay(read,protected)
        independent=acceptance.independent_arithmetic(canonical,read)
        assert independent['counts']==previous['independent_source_arithmetic']['counts']
        r.kv(canonical_originals_replayed_under_current_parser=True,independent_source_arithmetic=independent)
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
        subprocess.run(['git','merge-base','--is-ancestor',PAGE_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet','HEAD',pages_commit,'--',*acceptance.ASSETS],cwd=ROOT,check=True)
        for name in acceptance.ASSETS:
            raw=acceptance.public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',raw)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name]
            if name in ('etf-holdings.html','flow-lookthrough.html'):assert b'jh-etf-holdings.js?v=20260921-native2' in raw
        # Only this projection is invoked. It replays the current canonical originals without collecting them.
        fn='justhodl-flow-lookthrough';request=acceptance.invoke_public(lam,s3,store,fn)
        profile=acceptance.execution_profile(boto3.client('logs',region_name='us-east-1'),fn,request['status'])
        assert request['status']['provider_requests']==request['status']['provider_requests_this_execution']==0
        look_raw=acceptance.public(model.LOOK_CURRENT);look=json.loads(look_raw)
        assert read(model.LOOK_CURRENT)==look_raw and look['generated_at']==request['status']['generated_at']
        assert model.clock(look['generated_at'])>model.clock(previous['source_replays']['lookthrough']['generated_at'])
        look_replay=acceptance.replay_verify(look,read)
        assert look['canonical_replay']==canonical['replay'] and look['source_generated_at']==canonical['generated_at']
        for field in ('funds','quality','security_directory','source_valid_until','methodology'):assert look[field]==canonical[field]
        assert acceptance.public(model.CURRENT)==canonical_raw and read(model.CURRENT)==canonical_raw
        aliases={}
        for key in store.ALIASES:
            expected=store.compatibility(canonical,key)
            assert json.loads(acceptance.public(key))==expected and json.loads(read(key))==expected
            aliases[key]={'contract':expected['contract'],'canonical':expected['canonical'],'matches':True,'unchanged':True}
        run=json.loads(read(look['replay']['manifest_key']));inputs=store.checked(run['input'],model.LOOK_PREFIX,'inputs',read)
        for ref in [inputs['canonical_source'],inputs['previous']]:
            if ref:store.protected(ref,read);protected.add(ref['key'])
        migration=json.loads(read(store.MIGRATION));store.protected(migration,read);protected.add(migration['key'])
        def denied(key):assert acceptance.denied('https://justhodl.ai/'+key) and acceptance.denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=8) as pool:
            for _ in pool.map(denied,sorted(protected)):pass
        publications={'holdings':previous['publications']['holdings'],'lookthrough':{'key':model.LOOK_CURRENT,'sha256':model.sha(look_raw),'bytes':len(look_raw),'replay':look['replay']}}
        proof={'contract':'etf-holdings-parser-acceptance.v1','generated_at':datetime.now(timezone.utc).isoformat(),'commit':COMMIT,
            'runtime_packages':runtimes,'publications':publications,'source_replays':{'holdings':canonical_replay,'lookthrough':look_replay},
            'requests':{fn:request},'execution_profiles':{fn:profile},'independent_source_arithmetic':independent,
            'canonical_body_unchanged':True,'canonical_originals_verified_under_current_parser':True,'retained_previous_acceptance':retained_previous,
            'audit_originals_replayed':audit_replays,'whole_predecessors_preserved':True,'schedules':schedules,'compatibility_aliases':aliases,
            'protected_artifacts_checked':len(protected),'protected_artifacts_anonymously_denied':True,'producer_invocations_this_acceptance':int(request['invoke_sent']),
            'holdings_collection_invocations':0,'provider_requests_this_acceptance':0,'private_account_reads':0,'paid_ai_calls':0,
            'notifications_sent':0,'signals_emitted':0,'portfolio_writes':0,'unreviewed_consumer_invocations':0,
            'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in acceptance.ASSETS}}
        raw=model.encoded(proof);s3.put_object(Bucket=BUCKET,Key=PROOF_KEY,Body=raw,ContentType='application/json',CacheControl='no-store')
        assert acceptance.public(PROOF_KEY)==raw
        r.kv(proof_key=PROOF_KEY,publications=publications,execution_profile=profile,canonical_body_unchanged=True,
            protected_artifacts_checked=len(protected),originals_anonymously_denied=True,compatibility_aliases_verified=len(aliases),
            provider_requests_this_acceptance=0,holdings_collection_invocations=0,producer_invocations_this_acceptance=int(request['invoke_sent']),
            private_account_reads=0,paid_ai_calls=0,notifications_sent=0,signals_emitted=0,portfolio_writes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
