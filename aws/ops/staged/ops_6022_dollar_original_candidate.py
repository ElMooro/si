"""Qualify Dollar original calculations from the exact 6020/6021 captures.

Retained inputs only. No recollection, engine invocation, notification, account
read, schedule change or public head publication. A failed claim is not retried.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json,subprocess,sys,time
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
from dollar_original_audit import independent
import ops_6020_dollar_source_preflight as baseline
import dollar_research_store as store
model=store.model;catalog=store.catalog;BUCKET=baseline.BUCKET
BASELINE={'key':model.PRIVATE+'c035e557b437dddf94aa796c50d85286d7e9ddcfa9b1c20933326a6ed1aee167.bin',
    'sha256':'c035e557b437dddf94aa796c50d85286d7e9ddcfa9b1c20933326a6ed1aee167','bytes':89076}
QUALIFIED={'key':model.PRIVATE+'078928318448295e05b8353d001bde97ceee5529b8bfdf5ffdb74782e755b277.bin',
    'sha256':'078928318448295e05b8353d001bde97ceee5529b8bfdf5ffdb74782e755b277','bytes':50232}
REQUEST='chatgpt-dollar-original-candidate-6022';STATUS=store.request_key(REQUEST)


def recorded_inputs(before,source,stamp):
    assert before['contract']=='dollar-source-preflight.v1' and source['contract']=='dollar-canonical-source-qualification.v1'
    assert source['baseline']==BASELINE and source['source_commit']=='5b74be08d6b5f9e05e9385b0a8b50572f77f6bd6'
    captures=deepcopy(before['captures']);key='data/report-measurements.json'
    assert set(captures)==set(store.SOURCES)
    captures[key]={'source_key':key,'status':'retained','acquired_at':source['generated_at'],'original':source['canonical_packet']}
    assert all(model.clock(row['acquired_at'])<=model.clock(stamp) for row in captures.values())
    return {'contract':'dollar-original-inputs.v1','generated_at':stamp,'captures':captures}
def retained_reader(read,source):
    refs=source['canonical_originals']
    def bound(key):
        if store.canonical(key):
            if key not in refs:raise ValueError('Canonical artifact absent from exact qualified original inventory')
            raw=store.original(refs[key],read)
            if model.sha(raw)!=key.rsplit('/',1)[-1].split('.')[0]:raise ValueError('Canonical source and protected copy differ')
            return raw
        return read(key)
    bound.remember=read.remember
    return bound


def main():
    import resource
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    read=store.reader(s3,BUCKET)
    with report('ops_6022_dollar_original_candidate') as r:
        for test in ('tests/test_dollar_research_model.py','tests/test_dollar_research_store.py','tests/test_dollar_original_audit.py'):
            subprocess.run([sys.executable,str(ROOT/test)],cwd=ROOT,check=True)
        before=json.loads(store.original(BASELINE,read));source=json.loads(store.original(QUALIFIED,read))
        assert not [x for x in before['consumer_packages'] if not x['pass']]
        actual=baseline.runtime(lam,s3,events,scheduler,baseline.FN)
        assert actual==before['runtime'],'Legacy producer changed since retained baseline'
        bound=retained_reader(read,source)
        try:prior=json.loads(baseline.get(s3,STATUS))
        except Exception as exc:
            if not store.missing(exc):raise
            prior=None
        if prior:
            assert prior['status']=='complete','Inspect the retained incomplete candidate; never recapture silently'
            accepted=prior['candidate'];candidate=json.loads(store.original(accepted,read))
            output=store.replay(candidate['candidate_replay'],bound)
        else:
            status={'status':'claimed','request_id':REQUEST,'baseline':BASELINE,'source_qualification':QUALIFIED,'generated_at':store.now()}
            store.status_write(s3,BUCKET,STATUS,status,IfNoneMatch='*')
            def checkpoint(**fields):status.update(fields);store.status_write(s3,BUCKET,STATUS,status)
            inputs=recorded_inputs(before,source,store.now())
            heads={key:baseline.get(s3,key) for key in (model.CURRENT,model.HISTORY)}
            assert all(heads[key]==store.original(inputs['captures'][key]['original'],read) for key in heads),'Dollar predecessor advanced; inspect before qualification'
            started=time.monotonic();output=store.compile_output(inputs,bound)
            replay=store.retain(s3,BUCKET,inputs,output,bound,checkpoint);elapsed=time.monotonic()-started
            canonical=json.loads(store.original(source['canonical_packet'],read));originals=store.canonical_fred_replay.restore(canonical,catalog.SERIES,bound)
            counts=independent(output,originals,canonical,catalog);peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            assert elapsed<120 and peak<224*1024,'Inspect resource budget against the existing 180-second/256-MB runtime'
            assert {key:baseline.get(s3,key) for key in heads}==heads,'Legacy head changed during qualification'
            assert baseline.runtime(lam,s3,events,scheduler,baseline.FN)==actual
            candidate={'contract':'dollar-original-candidate.v1','generated_at':store.now(),'baseline':BASELINE,'source_qualification':QUALIFIED,
                'candidate_replay':replay,'counts':counts,'quality':output['quality'],'predecessor_runtime':actual,
                'candidate_seconds':round(elapsed,3),'peak_runner_rss_kib':peak,'original_arithmetic_independently_checked':True,
                'recorded_replay_verified':True,'compiler_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()}
            accepted=store.protect(s3,BUCKET,model.encoded(candidate),read);checkpoint(status='complete',candidate=accepted)
        protected={STATUS,BASELINE['key'],QUALIFIED['key'],accepted['key'],source['canonical_packet']['key'],
            *(row['original']['key'] for row in before['captures'].values() if row['original']),*(ref['key'] for ref in source['canonical_originals'].values())}
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        r.kv(accepted_candidate=accepted,candidate_replay=candidate['candidate_replay'],generated_at=output['generated_at'],counts=candidate['counts'],
            quality=candidate['quality'],candidate_seconds=candidate['candidate_seconds'],peak_runner_rss_kib=candidate['peak_runner_rss_kib'],
            compiler_commit=candidate['compiler_commit'],original_arithmetic_independently_checked=True,recorded_replay_verified=True,
            protected_artifacts_checked=len(protected),originals_anonymously_denied=True,provider_requests=0,engine_invocations=0,public_head_writes=0,
            private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
