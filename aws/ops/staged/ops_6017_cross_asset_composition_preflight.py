"""Retain the existing composite plus native FX/futures for versioned integration.

Read approved public research only. No provider/engine/AI invocation or head write.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from release_package_evidence import check_packages
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import massive_research_model as model
import massive_research_store as store
BUCKET='justhodl-dashboard-live'
PRODUCERS=('justhodl-massive-signals','justhodl-polygon-fx-regime','justhodl-polygon-futures-curves')
CONSUMERS=tuple('justhodl-'+n for n in ('alpha-score','best-setups','convergence-radar','master-ranker','pump-mechanics','theme-rotation'))
NATIVE={'fx':('data/fx-quote-research.json','fx-original-quote-research.v1','data/fx-quote-research-verification.json'),
    'futures':('data/futures-research.json','futures-original-research.v1','data/futures-research-verification.json')}
PACKETS=tuple(dict.fromkeys((model.CURRENT,*model.CAPTURE_KEYS,*model.PREDECESSORS,*(x for row in NATIVE.values() for x in (row[0],row[2])))))
REQUEST='chatgpt-cross-asset-composition-preflight-6017'
STATUS=store.request_key(REQUEST)


def get(s3,key):return store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
def capture(s3,key,read):
    assert key in PACKETS,'Explicit public research capture only'
    raw=get(s3,key);doc=model.strict(raw);assert isinstance(doc,dict)
    return {'source_key':key,'original':store.protect(s3,BUCKET,raw,read),'acquired_at':store.now()}
def describe(key,doc):
    assert key in PACKETS and isinstance(doc,dict)
    return {'contract':doc.get('contract'),'root_fields':sorted(doc),'generated_at':doc.get('generated_at'),
        'source_capture_completed_at':doc.get('source_capture_completed_at'),'definition_date':doc.get('definition_date'),
        'replay':doc.get('replay'),'original_provider_replay_performed_by_this_inventory':False,
        'authority':{k:doc.get(k) for k in model.FLAGS},
        'pair_names':sorted(doc.get('pairs',{})) if isinstance(doc.get('pairs',{}),dict) else None,
        'product_names':sorted(doc.get('products',{})) if isinstance(doc.get('products',{}),dict) else None,
        'native_sources':sorted(doc.get('sources',{})) if isinstance(doc.get('sources',{}),dict) else None}
def proof_matches(packet,proof):
    model.permissions(packet)
    assert (proof.get('publication',{}).get('replay')==packet.get('replay')
        and proof.get('original_source_replay_matches') is True and proof.get('originals_anonymously_denied') is True
        and proof['publication']['sha256']==model.sha(model.encoded(packet))),'Retained native acceptance must match the exact publication'


def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1');read=store.reader(s3,BUCKET)
    with report('ops_6017_cross_asset_composition_preflight') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_cross_asset_composition_preflight.py')],cwd=ROOT,check=True)
        try:prior=model.strict(get(s3,STATUS))
        except Exception as exc:
            if not store.missing(exc):raise
            prior=None
        if prior:
            assert prior['status']=='complete','Inspect incomplete retained graph; do not recapture silently'
            manifest=model.strict(model.original(prior['manifest'],read));ref=prior['manifest'];r.kv(adopted_completed_request=True)
        else:
            store.status_write(s3,BUCKET,STATUS,{'status':'claimed','request_id':REQUEST,'generated_at':store.now()},IfNoneMatch='*')
            runtimes={fn:runtime(lam,s3,events,scheduler,fn) for fn in PRODUCERS}
            producer_packages=check_packages(lam,ROOT,PRODUCERS);assert all(x['pass'] for x in producer_packages),'Inspect producer package mismatch'
            packages=check_packages(lam,ROOT,CONSUMERS);assert all(x['pass'] for x in packages),'Inspect consumer package mismatch'
            refs={};total=0
            for key in PACKETS:
                refs[key]=capture(s3,key,read);total+=refs[key]['original']['bytes'];assert total<=128*1024*1024
                store.status_write(s3,BUCKET,STATUS,{'status':'capturing','request_id':REQUEST,'captures':refs})
            docs={k:model.strict(model.original(v['original'],read)) for k,v in refs.items()}
            current=docs[model.CURRENT];assert store.replay(current['replay'],read)=={k:v for k,v in current.items() if k!='replay'}
            for kind,(key,contract,proof) in NATIVE.items():
                assert docs[key]['contract']==contract;proof_matches(docs[key],docs[proof])
            manifest={'contract':'cross-asset-composition-preflight.v1','generated_at':store.now(),'producer_runtimes':runtimes,
                'producer_packages':producer_packages,'consumer_packages':packages,'captures':refs,'inventory':{k:describe(k,v) for k,v in docs.items()},
                'legacy_composite_replay':current['replay'],'legacy_composite_replayed':True,
                'fx_futures_exact_native_acceptance_bound':True,'provider_original_replay_performed_by_this_preflight':False,
                'retained_bytes':total,'compilers':{m.__name__:store.protect(s3,BUCKET,Path(m.__file__).read_bytes(),read) for m in store.COMPILERS}}
            ref=store.protect(s3,BUCKET,model.encoded(manifest),read)
            store.status_write(s3,BUCKET,STATUS,{'status':'complete','request_id':REQUEST,'manifest':ref})
        protected={STATUS,ref['key'],*(v['original']['key'] for v in manifest['captures'].values()),*(v['key'] for v in manifest['compilers'].values())}
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        r.kv(retained_manifest=ref,retained_packets=len(manifest['captures']),retained_bytes=manifest['retained_bytes'],
            producer_runtimes=manifest['producer_runtimes'],producer_packages_checked=len(manifest['producer_packages']),consumer_packages_checked=len(manifest['consumer_packages']),
            inventory=manifest['inventory'],legacy_composite_replay=manifest['legacy_composite_replay'],
            legacy_composite_replayed=True,fx_futures_native_acceptance_bound=True,protected_artifacts_checked=len(protected),
            originals_anonymously_denied=True,provider_requests=0,engine_invocations=0,public_head_writes=0,private_account_reads=0,
            paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
