"""Retained-input publication candidate; no provider requests or native-head writes."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import sys,time,subprocess,urllib.request
import boto3
ROOT=Path(__file__).resolve().parents[3];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged')]
from ops_report import report
import offexchange_research_model as model
import offexchange_research_store as store
import ops_6042_offexchange_reported_issue_capture as source
from ops_5998_option_population_retained_acceptance import denied_with_retry
prior=source.prior;BUCKET=source.BUCKET;REQUEST='chatgpt-offexchange-replay-candidate-6043'
STATUS=model.PRIVATE+'requests/'+model.sha(REQUEST.encode())+'.json'
REF={'key':model.PRIVATE+'3fc29d3bbc5faf38a5cba6e88e1e0f6f0d78d3a60e76b77459567624095b061b.bin','sha256':'3fc29d3bbc5faf38a5cba6e88e1e0f6f0d78d3a60e76b77459567624095b061b','bytes':163100}

def main():
    import resource
    s3=boto3.client('s3',region_name='us-east-1');read=store.reader(s3,BUCKET)
    with report('ops_6043_offexchange_replay_candidate') as r:
        for test in ('test_offexchange_measurements.py','test_offexchange_research_model.py','test_offexchange_research_store.py'):
            subprocess.run([sys.executable,str(ROOT/'tests'/test)],cwd=ROOT,check=True)
        try:existing=prior.strict(prior.get(s3,STATUS))
        except Exception as exc:
            if not prior.missing(exc):raise
            existing=None
        if existing:
            assert existing['status']=='complete','Never repeat incomplete candidate request'
            ref=existing['replay'];compiled=store.replay(ref,read);profile=existing['profile']
        else:
            manifest=model.strict(model.original(REF,read));inventory=model.strict(model.original(manifest['inventory'],read))
            assert manifest['qualification']['parser_sha256']==model.sha(Path(store.measurements.__file__).read_bytes())
            source.source.write_status(s3,STATUS,{'request_id':REQUEST,'status':'claimed','source_manifest':REF},True)
            inputs={'contract':'offexchange-original-inputs.v1','generated_at':prior.now(),'source_manifest':REF,'inventory':manifest['inventory'],
                'metadata':manifest.get('metadata',{}) or {k:v for k,v in inventory['captures'].items() if k.startswith('metadata:')},
                'partitions':manifest['partitions'],'daily':inventory['captures']['daily:CNMS'],'daily_date':inventory['parents']['data/finra-short.json']['data_date']}
            started=time.monotonic();compiled=model.compile_output(inputs,read);compile_seconds=time.monotonic()-started
            qualification=source.qualify(s3,inputs['partitions'])
            assert qualification==manifest['qualification'],'Independent retained arithmetic differs'
            ref=store.retain(s3,BUCKET,inputs,compiled);source.source.write_status(s3,STATUS,{'request_id':REQUEST,'status':'retained','replay':ref})
            started=time.monotonic();replayed=store.replay(ref,store.reader(s3,BUCKET));replay_seconds=time.monotonic()-started
            assert replayed==compiled
            profile={'compile_seconds':round(compile_seconds,3),'replay_seconds':round(replay_seconds,3),'max_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                'index_bytes':len(model.encoded(compiled['packet'])),'shard_count':len(compiled['shards']),
                'shard_bytes':sum(len(model.encoded(v)) for v in compiled['shards'].values()),'max_shard_bytes':max(len(model.encoded(v)) for v in compiled['shards'].values())}
            assert profile['max_rss_kib']<900*1024,'Candidate exceeds conservative native memory qualification bound'
            source.source.write_status(s3,STATUS,{'request_id':REQUEST,'status':'complete','replay':ref,'profile':profile})
        packet=compiled['packet'];run=store.verified_run(ref,read)
        public={ref['manifest_key'],run['input']['key'],run['output']['key'],*(v['key'] for v in run['compilers'].values()),*(v['key'] for v in packet['record_shards'].values())}
        def check(key):
            response=urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'JustHodl-research-acceptance/1.0','Cache-Control':'no-cache'}),timeout=25)
            if store.bounded(response)!=read(key):raise ValueError('Public immutable artifact differs')
        # The cached reader is intentionally serial; its bounded LRU is not shared across threads.
        for key in sorted(public):check(key)
        inputs=store.checked(run['input'],'inputs',read);protected={STATUS,REF['key'],inputs['inventory']['key'],inputs['daily']['original']['key']}
        for part in inputs['partitions'].values():
            for page in part['pages']:protected.add(page['original']['key'])
        for meta in inputs['metadata'].values():protected.add(meta['original']['key'])
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        r.kv(replay=ref,profile=profile,counts=packet['counts'],public_artifacts_checked=len(public),protected_artifacts_checked=len(protected),
            compiler_sha256={k:v['sha256'] for k,v in run['compilers'].items()},provider_requests=0,engine_invocations=0,
            native_head_writes=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)
if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
