"""Verify the expanded canonical source route and retain its original observations.

May dispatch the existing public FRED research route once behind a durable claim.
No Dollar/consumer invocation, account data, AI, notifications or cadence changes.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
from threading import Lock
import gzip,io,json,re,subprocess,sys,time,urllib.request
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from release_package_evidence import shared_imports
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6020_dollar_source_preflight as base
import canonical_fred_replay,dollar_source_catalog
BUCKET=base.BUCKET;PREFIX=base.PREFIX;FN='justhodl-daily-report-v3';CURRENT='data/report-measurements.json'
BASELINE={'key':PREFIX+'c035e557b437dddf94aa796c50d85286d7e9ddcfa9b1c20933326a6ed1aee167.bin',
    'sha256':'c035e557b437dddf94aa796c50d85286d7e9ddcfa9b1c20933326a6ed1aee167','bytes':89076}
EXPECTED={'DEXUSAL':('U.S. Dollars to One Australian Dollar','D'),
    'DEXSIUS':('Singapore Dollars to One U.S. Dollar','D'),'DEXTAUS':('Taiwan Dollars to One U.S. Dollar','D'),
    'DEXSDUS':('Swedish Kronor to One U.S. Dollar','D'),'IRLTLT01DEM156N':('Percent','M')}


def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:return base.bounded(response)
def source_commit():
    source='aws/lambdas/'+FN+'/source'
    files=[ROOT/p for p in subprocess.check_output(['git','ls-files',source],cwd=ROOT,text=True).splitlines()]
    paths=[source,'aws/lambdas/'+FN+'/config.json',*[p.relative_to(ROOT).as_posix() for p in shared_imports(ROOT,files)]]
    value=subprocess.check_output(['git','log','-1','--format=%H','--',*paths],cwd=ROOT,text=True).strip()
    assert re.fullmatch('[a-f0-9]{40}',value);return value
def save_status(s3,key,doc,claim=False):
    assert re.fullmatch(re.escape(PREFIX)+r'requests/[a-f0-9]{64}\.json',key)
    raw=base.encoded(doc)
    s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    assert base.get(s3,key)==raw,'Canonical dispatch claim readback differs'
def checked(ref,s3):
    assert ref['key']==PREFIX+ref['sha256']+'.bin'
    raw=base.get(s3,ref['key']);assert base.sha(raw)==ref['sha256'] and len(raw)==ref['bytes'];return raw
def ready(packet,read,after=None):
    manifest=canonical_fred_replay.pinned_report(packet,read)
    if not set(EXPECTED)<=set(manifest['catalog']):return False
    at=datetime.fromisoformat(packet['generated_at'].replace('Z','+00:00'))
    if after and at<=datetime.fromisoformat(after.replace('Z','+00:00')):return False
    assert 0<=(datetime.now(timezone.utc)-at).total_seconds()<=26*3600,'Canonical source acquisition review required'
    for sid,(unit,freq) in EXPECTED.items():
        row=packet.get('measurements',{}).get(sid)
        assert isinstance(row,dict) and row['unit']==unit and row['frequency']==freq,'Expanded source absent or definition differs: '+sid
        assert row.get('current_decimal') is not None,'Expanded source lacks a dated observation: '+sid
    assert packet.get('calls_eligible') is False and packet.get('sizing_eligible') is False
    return True
def refresh(lam,s3,commit,read,monotonic=time.monotonic,sleep=time.sleep):
    def current(after=None):
        raw=base.get(s3,CURRENT);packet=json.loads(raw)
        return (raw,packet) if ready(packet,read,after) else None
    available=current()
    if available:return available,{'invoke_sent':False,'basis':'Existing pinned canonical publication already contains all five reviewed definitions'}
    request='chatgpt-dollar-canonical-'+commit[:12]+'-6021';key=PREFIX+'requests/'+base.sha(request.encode())+'.json';sent=False
    try:claim=json.loads(base.get(s3,key))
    except Exception as exc:
        if not base.missing(exc):raise
        claim={'contract':'dollar-canonical-dispatch.v1','request_id':request,'source_commit':commit,'started_at':base.now(),'status':'claimed'}
        save_status(s3,key,claim,claim=True)
        response,rejected=invoke_when_available(lam,{'FunctionName':FN,'InvocationType':'Event','Payload':base.encoded({'action':'research_measurements'})})
        assert response['StatusCode']==202
        sent=True;claim.update(status='accepted_async',throttle_rejections_before_acceptance=rejected)
        save_status(s3,key,claim)
    assert claim['request_id']==request and claim['source_commit']==commit
    deadline=monotonic()+900
    while True:
        available=current(claim['started_at'])
        if available:break
        assert monotonic()<deadline,'No qualified source publication observed; inspect the claimed request without re-invoking'
        sleep(10)
    claim.update(status='publication_observed',verified_at=base.now(),replay=available[1]['replay'],
        basis='Pinned publication observed after the claim; this does not assert a specific asynchronous execution identity')
    save_status(s3,key,claim)
    return available,{'invoke_sent':sent,'status_key':key,'status':claim}


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=30,connect_timeout=10,retries={'max_attempts':0}))
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_6021_dollar_canonical_sources') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_dollar_canonical_acceptance.py')],cwd=ROOT,check=True)
        commit=source_commit();actual=runtime(lam,s3,events,scheduler,FN);assert actual['receipt']['commit']==commit
        assert actual['timeout']==900 and actual['memory_mb']==1024
        baseline=json.loads(checked(BASELINE,s3));assert baseline['contract']=='dollar-source-preflight.v1'
        assert not [x for x in baseline['consumer_packages'] if not x['pass']]
        original_before=base.retained(s3,base.get(s3,CURRENT));originals={};lock=Lock();total=0
        def read(key):
            nonlocal total
            if not base.canonical_key(key):raise ValueError('Reviewed canonical artifact required')
            raw=base.get(s3,key)
            if key.endswith('.gz'):raw=base.bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)))
            with lock:
                if key not in originals:total+=len(raw)
                assert total<=128*1024*1024,'Original source byte budget'
            ref=base.retained(s3,raw)
            with lock:originals[key]=ref
            return raw
        (raw,packet),dispatch=refresh(lam,s3,commit,read)
        r.kv(commit=commit,canonical_refresh=dispatch)
        restored=canonical_fred_replay.restore(packet,base.SERIES,read)
        assert all(restored[sid] is not None for sid in EXPECTED)
        retained_packet=base.retained(s3,raw);available=base.inventory(packet,restored)
        public_raw=public(CURRENT);public_packet=json.loads(public_raw)
        assert ready(public_packet,read),'Public canonical publication lacks expanded source definitions'
        canonical_fred_replay.restore(public_packet,tuple(EXPECTED),read)
        assert datetime.fromisoformat(public_packet['generated_at'])>=datetime.fromisoformat(packet['generated_at'])
        manifest={'contract':'dollar-canonical-source-qualification.v1','generated_at':base.now(),'runtime':actual,
            'source_commit':commit,'baseline':BASELINE,'previous_canonical':original_before,'canonical_packet':retained_packet,
            'canonical_originals':originals,'canonical_inventory':available,'canonical_replay':packet['replay'],
            'canonical_refresh':dispatch,'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False}
        ref=base.retained(s3,base.encoded(manifest))
        protected={BASELINE['key'],original_before['key'],retained_packet['key'],ref['key'],*(x['key'] for x in originals.values())}
        if dispatch.get('status_key'):protected.add(dispatch['status_key'])
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        after=runtime(lam,s3,events,scheduler,FN);assert after==actual,'Producer package or runtime changed during qualification'
        proof={'contract':'dollar-canonical-acceptance.v1','verified_at':base.now(),'source_commit':commit,'runtime':actual,
            'qualified_manifest':ref,'qualification_publication':{'sha256':base.sha(raw),'bytes':len(raw),'generated_at':packet['generated_at'],'replay':packet['replay']},
            'public_publication':{'sha256':base.sha(public_raw),'bytes':len(public_raw),'generated_at':public_packet['generated_at'],'replay':public_packet['replay']},
            'canonical_inventory':available,'canonical_originals_replayed':sum(x is not None for x in restored.values()),
            'canonical_refresh':dispatch,'originals_anonymously_denied':True,'protected_artifacts_checked':len(protected),
            'dollar_engine_invocations':0,'consumer_invocations':0,'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,'schedules_changed':0}
        key='data/dollar-source-verification.json';body=base.encoded(proof)
        s3.put_object(Bucket=BUCKET,Key=key,Body=body,ContentType='application/json',CacheControl='no-store')
        assert public(key)==body,'Public source proof differs'
        r.kv(proof_key=key,qualified_manifest=ref,runtime=actual,canonical_inventory=available,
            canonical_originals_replayed=proof['canonical_originals_replayed'],qualification_publication=proof['qualification_publication'],
            public_publication=proof['public_publication'],protected_artifacts_checked=len(protected),
            source_dispatches_this_acceptance=int(dispatch['invoke_sent']),dollar_engine_invocations=0,consumer_invocations=0,
            private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
