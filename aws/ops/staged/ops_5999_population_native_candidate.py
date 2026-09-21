"""Qualify the complete native population publisher without changing a head."""
from pathlib import Path
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import json,subprocess,sys,time
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5996_option_population_qualification import independent,BUCKET,FUNCTION
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_5987_options_dependency_preflight as baseline
import option_population_store as store
import option_population_research as desk
import option_population_model as model
import option_research_rows as codec
upstream=store.upstream
QUALIFIED={'key':'audit-private/20260909-originals/options-research/d4eb3da76e1b7c9e55ae39ac8a579bda83fb30442df075296a1a981f56f32892.bin',
    'sha256':'d4eb3da76e1b7c9e55ae39ac8a579bda83fb30442df075296a1a981f56f32892','bytes':3258}
REQUEST='chatgpt-ops-5999-population-candidate'


def verify_memberships(item,source,read):
    member=model.checked(item['source_membership'],'groups',read)
    assert member['contract']=='option-population-membership.v1' and member['source_run']==source['replay']
    assert member['source_chain']==item['source_chain']
    summary=upstream.checked(item['source_chain'],read,'chains');positions={}
    assert member['record_blocks']==summary['record_blocks'] and member['underlying']==summary['underlying']
    for part in summary['record_blocks']:
        for row in codec.unpack(upstream.checked(part['artifact'],read,'rows')):
            if row['identity_eligible']:positions[row['evidence']['page'],row['evidence']['row_index']]=row
    seen=set()
    for group in member['groups']:
        for entry in group['source_rows']:
            key=entry['source_page'],entry['row_index'];assert key not in seen;seen.add(key)
            row=positions[key];assert row['contract_id']==entry['contract_id'] and row['contract_type']==entry['contract_type']
            assert row['expiration_date']==group['expiration_date']
            assert Decimal(row['metrics']['strike']['value'])==Decimal(group['strike_usd_per_share'])
            fields=[];metrics=row['metrics']
            if metrics['open_interest']['value'] is not None:
                fields.append('reported_open_interest')
                for name in ('gamma','delta'):
                    if metrics['vendor_'+name]['value'] is not None:fields.append(name+'_oi_shares')
            assert fields==entry['eligible_fields']
    assert seen==set(positions)
    return len(seen)


def main():
    import resource
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=24))
    lam=boto3.client('lambda',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    scheduler=boto3.client('scheduler',region_name='us-east-1');read=store.reader(s3,BUCKET)
    with report('ops_5999_population_native_candidate') as r:
        for name in ('test_option_population_model.py','test_option_population_research.py','test_option_population_store.py'):
            subprocess.run([sys.executable,str(ROOT/'tests'/name)],cwd=ROOT,check=True)
        qualification=json.loads(upstream.protected(QUALIFIED,read));assert qualification['privacy_verified'] is True
        previous=json.loads(upstream.protected(qualification['retained_candidate'],read))
        assert upstream.protected(previous['compiler'],read)==Path(model.__file__).read_bytes()
        actual=runtime(lam,s3,events,scheduler,FUNCTION);assert actual==qualification['predecessor_runtime']
        try:s3.head_object(Bucket=BUCKET,Key=desk.CURRENT)
        except Exception as exc:
            if not store.missing(exc):raise
        else:raise RuntimeError('Native population head already exists; inspect before migration')
        key=store.source_store.request_key('population:'+REQUEST)
        try:s3.head_object(Bucket=BUCKET,Key=key)
        except Exception as exc:
            if not store.missing(exc):raise
        else:raise RuntimeError('Retained candidate attempt exists; inspect without repeating')
        legacy_before=read(desk.LEGACY);history_before=read(desk.HISTORY)
        started=time.monotonic()
        status=store.run(s3,BUCKET,REQUEST,'runner-ops-5999',publish_current=False)
        elapsed=time.monotonic()-started
        r.kv(candidate_status_key=key,candidate_replay=status.get('replay'),native_candidate_seconds=round(elapsed,3))
        assert status['status']=='complete' and status['published'] is False and status['compatibility_published'] is False
        assert status['provider_requests']==0
        run=store.verified_run(status['replay'],read);inputs=store.checked(run['input'],'inputs',read)
        output=store.checked(run['output'],'outputs',read)
        assert store.replay(status['replay'],read)==output
        source=model.verified_packet(upstream.protected(inputs['source_publication'],read),read)
        source_run=store.source_store.verified_run(source['replay'],read)
        source_inputs=upstream.checked(source_run['input'],read,'inputs')
        protected={QUALIFIED['key'],qualification['retained_candidate']['key'],key,inputs['source_publication']['key']}
        protected.update(v['key'] for v in inputs['predecessors'].values() if v is not None)
        counts=Counter();checks={}
        for symbol in model.UNDERLYINGS:
            item=output['underlyings'][symbol];derived=model.restore(item['population'],read)
            assert derived['totals']==item['totals'] and derived['source_run']==source['replay']
            summary=upstream.checked(source['chains'][symbol]['chain'],read,'chains')
            result=independent(derived,summary,source_inputs['chains'][symbol],read)
            result['verified_membership_rows']=verify_memberships(item,source,read)
            checks[symbol]=result;counts.update({k:v for k,v in result.items() if type(v) is int})
            protected.update(p['original']['key'] for p in source_inputs['chains'][symbol]['pages'] if p.get('original'))
        assert upstream.protected(inputs['predecessors'][desk.LEGACY],read)==legacy_before
        assert upstream.protected(inputs['predecessors'][desk.HISTORY],read)==history_before
        history=json.loads(history_before);assert len(history['history'])>=720
        assert read(desk.LEGACY)==legacy_before and read(desk.HISTORY)==history_before
        assert runtime(lam,s3,events,scheduler,FUNCTION)==actual
        try:s3.head_object(Bucket=BUCKET,Key=desk.CURRENT)
        except Exception as exc:
            if not store.missing(exc):raise
        else:raise AssertionError('Candidate unexpectedly changed a public head')
        peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        candidate={'contract':'option-population-native-candidate.v1','generated_at':store.now(),
            'candidate_replay':status['replay'],'candidate_status_key':key,'source_run':source['replay'],
            'source_capture_completed_at':source['generated_at'],'compiled_at':output['generated_at'],
            'quality':output['quality'],'independent_checks':checks,'counts':dict(counts),
            'predecessor_runtime':actual,'whole_predecessor_history_rows':len(history['history']),
            'native_candidate_seconds':round(elapsed,3),'peak_runner_rss_kib':peak,
            'runtime_budget_pass':elapsed<480,'memory_budget_pass':peak<1536*1024,
            'provider_requests':0,'engine_invocations':0,'public_head_writes':0,'schedules_changed':0,
            'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'signals_emitted':0,'portfolio_writes':0}
        retained=baseline.retain(s3,upstream.encoded(candidate));protected.add(retained['key'])
        r.kv(retained_candidate=retained,counts=dict(counts),runtime_budget_pass=candidate['runtime_budget_pass'],
            memory_budget_pass=candidate['memory_budget_pass'],privacy_checks_pending=len(protected))
        def check(k):
            assert denied_with_retry('https://justhodl.ai/'+k)
            assert denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+k)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(check,sorted(protected)):pass
        assert candidate['runtime_budget_pass'] and candidate['memory_budget_pass'],'Inspect retained candidate resources before native deployment'
        candidate.update(privacy_verified=True,protected_artifacts_checked=len(protected),candidate_evidence=retained)
        accepted=baseline.retain(s3,upstream.encoded(candidate));check(accepted['key'])
        r.kv(accepted_candidate=accepted,candidate_replay=status['replay'],source_run=source['replay'],counts=dict(counts),
            native_candidate_seconds=candidate['native_candidate_seconds'],peak_runner_rss_kib=peak,
            whole_predecessor_history_rows=len(history['history']),protected_artifacts_checked=len(protected)+1,
            originals_anonymously_denied=True,provider_requests=0,engine_invocations=0,public_head_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
