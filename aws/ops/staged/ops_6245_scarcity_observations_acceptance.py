"""Read-only exact packages, original schedules and whole Scarcity donor replay.
No native/provider invocation, consumer-output/private account/learning reads,
publication writes, schedule changes, messages or notifications.
"""
from pathlib import Path
import hashlib,importlib.util,json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6232_sec_search_research_acceptance import check_runtime
import retained_access_evidence as access
FN='justhodl-scarcity-radar';KEY='data/scarcity-radar.json';BUCKET='justhodl-dashboard-live'


def compiler():
    p=ROOT/'aws/lambdas'/FN/'source/scarcity_observations.py'
    spec=importlib.util.spec_from_file_location('isolated_scarcity_observations',p);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def public_ref(raw):
    return {'key':'data/scarcity-radar/history/'+hashlib.sha256(raw).hexdigest()+'.json','sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}


def retained(s3,ref,private=False):
    m=compiler();pattern=re.escape(m.PRIVATE)+r'[a-f0-9]{64}\.bin' if private else r'data/scarcity-radar/history/[a-f0-9]{64}\.json'
    if not isinstance(ref,dict) or not re.fullmatch(pattern,str(ref.get('key',''))):raise ValueError('Only declared immutable donor/publication original permitted')
    obj=s3.get_object(Bucket=BUCKET,Key=ref['key']);raw=bounded(obj['Body'])
    expected=m.reference(raw) if private else public_ref(raw)
    if obj.get('ContentLength')!=len(raw) or expected!=ref:raise ValueError('Whole immutable original differs')
    return raw


def publication(s3,raw):
    m=compiler();p=m.strict(raw)
    if not isinstance(p,dict):raise ValueError('Whole current publication required')
    out={'status':'pending_original_schedule_publication','bytes':len(raw),'sha256':m.sha(raw),'generated_at':p.get('generated_at'),'version':p.get('version')}
    if p.get('measurement_contract')!=m.CONTRACT:return out,[]
    if retained(s3,public_ref(raw))!=raw:raise ValueError('Current immutable publication differs')
    start=m.clock(p.get('acquisition_started_at'));finish=m.clock(p.get('generated_at'))
    if start is None or finish is None or start>finish:raise ValueError('Publication clocks invalid')
    rows=p.get('source_inventory')
    if not isinstance(rows,list) or [r.get('key') for r in rows]!=list(m.SOURCES):raise ValueError('Complete ordered donor inventory required')
    captures={r['key']:r['capture'] for r in rows};originals={};protected=[]
    for key,capture in captures.items():
        if capture.get('status') in ('retained','invalid_original'):
            ref=capture['original'];originals[key]=retained(s3,ref,True);protected.append(ref['key'])
            stamp=m.clock(capture.get('received_at'))
            if stamp is None or not start<=stamp<=finish:raise ValueError('Acquisition outside publication interval')
    expected=m.compile_packet(captures,originals,p['generated_at'])
    if set(p)!=set(expected)|{'acquisition_started_at','previous_publication','duration_s'} or any(p.get(k)!=v for k,v in expected.items()):raise ValueError('Whole donor projection replay differs')
    prior=p['previous_publication']
    if prior is not None:
        previous=m.strict(retained(s3,prior))
        if not isinstance(previous,dict):raise ValueError('Whole previous publication required')
        if previous.get('measurement_contract')==m.CONTRACT and (m.clock(previous.get('generated_at')) is None or m.clock(previous['generated_at'])>=start):raise ValueError('Prior publication order invalid')
    out.update(status='complete_native_donor_originals_replayed',source_count=p['source_count'],declared_source_count=p['declared_source_count'],occurrences=p['occurrence_count'],independence_verified=False,first_release_history_verified=False,investment_authority=False)
    return out,protected


def main():
    for path in ('aws/lambdas/'+FN+'/tests/run_tests.py','aws/lambdas/justhodl-inventory-drawdown/tests/run_tests.py','tests/ops/test_scarcity_observations_acceptance.py'):
        subprocess.run([sys.executable,str(ROOT/path)],cwd=ROOT,check=True)
    baselines={FN:('scarcity-radar-original-baseline.json',2),'justhodl-inventory-drawdown':('inventory-original-baseline.json',3)}
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')};args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    with report('ops_6245_scarcity_observations_acceptance') as r:
        before={};expected={}
        for fn,(file,count) in baselines.items():
            original=json.loads((ROOT/'docs/audit/2026-09-27'/file).read_bytes())['actual_producers'][fn]
            expected[fn]=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+fn],cwd=ROOT,text=True).strip()
            before[fn]=runtime(*args,fn);check_runtime(before[fn],original,expected[fn],count)
        obj=clients['s3'].get_object(Bucket=BUCKET,Key=KEY);raw=bounded(obj['Body'])
        if obj.get('ContentLength')!=len(raw):raise ValueError('Whole current publication required')
        result,protected=publication(clients['s3'],raw)
        privacy=access.summarize([access.check(key) for key in set(protected)]) if protected else {'all_denied':None,'status':'awaiting_native_donor_archives'}
        if protected and not privacy['all_denied']:raise ValueError('Retained donor originals must remain private')
        if any(runtime(*args,fn)!=value for fn,value in before.items()):raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commits=expected,actual_runtimes=before,native_publication=result,privacy=privacy,
             native_invocations=0,provider_requests=0,consumer_output_reads=0,account_reads=0,learning_log_reads=0,
             public_writes=0,history_writes=0,schedule_changes=0,notifications_sent=0,
             scope='Complete exact packages and unchanged schedules; one public Scarcity packet and its declared retained originals only. Upstream provider vintages, timing, units, independent evidence and predictive qualification remain unverified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
