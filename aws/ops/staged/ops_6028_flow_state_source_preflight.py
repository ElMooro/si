"""Retain the complete Cross-Asset Flow baseline and bind published parent artifacts.

No provider call, legacy invocation, public-head replacement or private account
read. Hash binding proves publication identity, not original-provider replay or
investment validity. Those distinctions are retained in the resulting inventory.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib, json, re, subprocess, sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks')]
from ops_report import report
from release_package_evidence import check_packages
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
BUCKET='justhodl-dashboard-live'
FUNCTION='justhodl-cross-asset-flow-state'
PRIVATE='audit-private/20260909-originals/flow-state-research/'
REQUEST='chatgpt-flow-state-source-preflight-6028'
STATUS=PRIVATE+'requests/'+hashlib.sha256(REQUEST.encode()).hexdigest()+'.json'
MAX=32*1024*1024
PACKETS=tuple('data/'+name+'.json' for name in ('cross-asset-flow-state','risk-regime',
    'polygon-fx-regime','capital-inflows','gold-equity-rotation','dollar-radar',
    'etf-true-flows','dark-pool','fx-quote-research'))
PARENTS={
    'data/risk-regime.json':('risk-regime-research.v1','risk-regime-research','risk-regime-replay.v1'),
    'data/capital-inflows.json':('tic-original-research.v1','tic-research','tic-original-replay.v1'),
    'data/etf-true-flows.json':('etf-original-research.v1','etf-research','etf-original-replay.v1'),
    'data/dollar-radar.json':('dollar-original-research.v1','dollar-research','dollar-original-replay.v1'),
    'data/fx-quote-research.json':('fx-original-quote-research.v1','fx-quote-research','fx-original-replay.v1'),
}

def now():return datetime.now(timezone.utc).isoformat()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def missing(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code','')) in ('404','NoSuchKey')
def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if not 0<len(raw)<=MAX:raise ValueError('Whole original byte bound')
    return raw
def get(s3,key):return bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
def retain(s3,raw):
    assert isinstance(raw,bytes) and 0<len(raw)<=MAX
    key=PRIVATE+sha(raw)+'.bin'
    try:s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code','')) not in ('409','412','PreconditionFailed','ConditionalRequestConflict'):raise
    assert get(s3,key)==raw,'Retained original differs'
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}
def status(s3,value,claim=False):
    raw=encoded(value)
    s3.put_object(Bucket=BUCKET,Key=STATUS,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    assert get(s3,STATUS)==raw
def original(s3,ref):
    assert ref['key']==PRIVATE+ref['sha256']+'.bin' and re.fullmatch('[a-f0-9]{64}',ref['sha256'])
    raw=get(s3,ref['key']);assert len(raw)==ref['bytes'] and sha(raw)==ref['sha256'];return raw
def capture(s3,key):
    assert key in PACKETS,'Explicit public research packet required'
    raw=get(s3,key);doc=json.loads(raw);assert isinstance(doc,dict)
    return {'source_key':key,'acquired_at':now(),'original':retain(s3,raw)},doc
def describe(doc):
    return {'contract':doc.get('contract'),'generated_at':doc.get('generated_at'),
        'as_of':doc.get('as_of'),'data_asof':doc.get('data_asof'),'replay':doc.get('replay'),
        'quality':doc.get('quality'),'root_fields':sorted(doc),
        'source_original_replay_performed_here':False,
        'reported_authority':{k:doc.get(k) for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')}}

def bind_parent(key,packet,read):
    contract,prefix,run_contract=PARENTS[key];prefix='data/'+prefix+'/'
    assert packet.get('contract')==contract,'Unexpected parent contract: '+key
    assert all(packet.get(k) is False for k in ('calls_eligible','sizing_eligible','execution_eligible')),'Parent authority differs'
    identity=packet['replay'];run_key=identity['manifest_key']
    assert re.fullmatch(re.escape(prefix)+r'runs/[a-f0-9]{64}\.json',run_key),'Unreviewed run path'
    raw=read(run_key);assert run_key==prefix+'runs/'+sha(raw)+'.json','Run bytes differ'
    run=json.loads(raw);assert run['contract']==run_contract and run['output_sha256']==identity['output_sha256']
    def artifact(ref,kind,ext):
        assert re.fullmatch('[a-f0-9]{64}',ref['sha256'])
        assert ref['key']==prefix+kind+'/'+ref['sha256']+'.'+ext,'Unreviewed artifact path'
        body=read(ref['key']);assert sha(body)==ref['sha256'],'Artifact bytes differ'
        if 'bytes' in ref:assert len(body)==ref['bytes'],'Artifact length differs'
        return body
    inputs=json.loads(artifact(run['input'],'inputs','json'));assert isinstance(inputs,dict)
    output=artifact(run['output'],'outputs','json')
    assert sha(output)==identity['output_sha256'] and json.loads(output)=={k:v for k,v in packet.items() if k!='replay'},'Published output differs'
    assert run['generated_at']==packet['generated_at'],'Compilation clock differs'
    assert isinstance(run['compilers'],dict) and 0<len(run['compilers'])<=16
    for ref in run['compilers'].values():artifact(ref,'compilers','py')
    return {'contract':contract,'replay':identity,'generated_at':packet['generated_at'],
        'run_input_output_compiler_hashes_match':True,'compilers_checked':len(run['compilers']),
        'source_original_replay_performed_here':False,'forecast_qualification_performed':False}

def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_6028_flow_state_source_preflight') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_flow_state_source_preflight.py')],cwd=ROOT,check=True)
        try:prior=json.loads(get(s3,STATUS))
        except Exception as exc:
            if not missing(exc):raise
            prior=None
        if prior:
            assert prior['status']=='complete','Inspect incomplete request; no silent recapture'
            ref=prior['manifest'];manifest=json.loads(original(s3,ref));r.kv(adopted_completed_request=True)
        else:
            status(s3,{'status':'claimed','request_id':REQUEST,'generated_at':now()},claim=True)
            actual=runtime(lam,s3,events,scheduler,FUNCTION)
            packages=check_packages(lam,ROOT,[FUNCTION]);assert all(row['pass'] for row in packages)
            captures={};docs={};total=0
            for key in PACKETS:
                captures[key],docs[key]=capture(s3,key);total+=captures[key]['original']['bytes']
                assert total<=128*1024*1024,'Aggregate capture bound'
                status(s3,{'status':'capturing','request_id':REQUEST,'captures':captures})
            artifacts={};artifact_bytes=0
            def read(key):
                nonlocal artifact_bytes
                assert any(re.fullmatch(r'data/'+re.escape(p)+r'/(?:runs|inputs|outputs|compilers)/[a-f0-9]{64}\.(?:json|py)',key) for _,p,_ in PARENTS.values()),'Unreviewed parent artifact'
                if key in artifacts:return original(s3,artifacts[key])
                raw=get(s3,key);artifact_bytes+=len(raw);assert artifact_bytes<=128*1024*1024
                artifacts[key]=retain(s3,raw);return raw
            parents={key:bind_parent(key,docs[key],read) for key in PARENTS}
            alias=docs['data/polygon-fx-regime.json'].get('canonical',{})
            manifest={'contract':'flow-state-source-preflight.v1','generated_at':now(),'runtime':actual,
                'producer_packages':packages,'captures':captures,'parent_artifacts':artifacts,'parents':parents,
                'inventory':{key:describe(doc) for key,doc in docs.items()},'retained_packet_bytes':total,
                'retained_parent_artifact_bytes':artifact_bytes,'fx_alias_matches_captured_native':alias=={'key':'data/fx-quote-research.json','replay':docs['data/fx-quote-research.json']['replay']},
                'source_original_replay_performed_here':False,'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False}
            ref=retain(s3,encoded(manifest));status(s3,{'status':'complete','request_id':REQUEST,'manifest':ref})
        protected={STATUS,ref['key'],*(v['original']['key'] for v in manifest['captures'].values()),*(v['key'] for v in manifest['parent_artifacts'].values())}
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        r.kv(retained_manifest=ref,runtime=manifest['runtime'],packet_inventory=manifest['inventory'],
            parents=manifest['parents'],retained_packet_bytes=manifest['retained_packet_bytes'],
            retained_parent_artifact_bytes=manifest['retained_parent_artifact_bytes'],protected_artifacts_checked=len(protected),
            source_original_replay_performed_here=False,originals_anonymously_denied=True,provider_requests=0,
            engine_invocations=0,public_head_writes=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,
            portfolio_writes=0,schedules_changed=0)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
