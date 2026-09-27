"""Exact deployed sources and complete retained recession research verification.

No producer invocation, real provider request, public/history/archive write,
schedule change, account read or notification is permitted.
"""
from pathlib import Path
import importlib.util
import subprocess
import sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops', 'aws/ops/checks', 'aws/lambdas/justhodl-global-recession/source')]
from ops_report import report
from market_runtime_evidence import runtime
import recession_research as store
FN = 'justhodl-global-recession'
BUCKET = 'justhodl-dashboard-live'
BASELINE_SHA = '863712562cb23f3ac62fb46aaea39d14a98f1ce48afe1ae1f01ab5e37520016a'


def main():
    lam, s3, events, scheduler = (boto3.client(n, region_name='us-east-1') for n in ('lambda', 's3', 'events', 'scheduler'))
    with report('ops_6192_recession_research_acceptance') as r:
        expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/'+FN], cwd=ROOT, text=True).strip()
        before = runtime(lam, s3, events, scheduler, FN)
        if before['receipt'] != {'status':'matched', 'commit':expected} or (before['memory_mb'], before['timeout']) != (512,300):
            raise ValueError('Exact recession package and original runtime required')
        schedule = {'kind':'EventBridge Scheduler','name':'global-recession-sched','state':'ENABLED',
                    'expression':'cron(40 12 * * ? *)','timezone':'UTC','native_targets':1,'group':'default'}
        if before['schedules'] != [schedule]:
            raise ValueError('Original native cadence differs')
        def fetch(key):
            if not key.startswith(store.PRIVATE) or not key.endswith('.bin') or len(key) != len(store.PRIVATE)+68:
                raise ValueError('Protected complete evidence path required')
            obj=s3.get_object(Bucket=BUCKET,Key=key);raw=store.bounded(obj['Body'])
            if obj['ContentLength'] != len(raw) or store.sha(raw) != key[len(store.PRIVATE):-4]:
                raise ValueError('Retained complete original differs')
            return raw
        baseline=store.strict(fetch(store.PRIVATE+BASELINE_SHA+'.bin'))
        if baseline['status'] != 'complete':
            raise ValueError('Whole baseline required')
        tests=ROOT/'aws/lambdas'/FN/'tests/run_tests.py'
        subprocess.run([sys.executable,str(tests)],cwd=ROOT,check=True)
        spec=importlib.util.spec_from_file_location('recession_tests',tests)
        test=importlib.util.module_from_spec(spec);spec.loader.exec_module(test)
        memory=test.Memory();whole={}
        for key in (*store.INPUTS,store.HEAD):
            capture=baseline['captures'][key]
            if capture['status'] != 'whole_object_retained':
                raise ValueError('Complete fixture input missing')
            raw=fetch(capture['original']['key'])
            if len(raw) != capture['original']['bytes']:
                raise ValueError('Baseline length differs')
            whole[key]=raw;memory.seed(key,raw)
        compiler=test.module(memory);compiler.FRED_KEY=''  # no provider request or historical FRED reconstruction
        _,_,fixture,manifest=test.run(memory,compiler,opener=lambda *a,**k:(_ for _ in ()).throw(AssertionError('Provider forbidden')))
        replay=store.replay_native(compiler,manifest,lambda key:memory.rows[key])
        if memory.writes.count(store.HEAD) != 1 or fixture['unqualified_legacy_calculation'] != store.strict(memory.rows[manifest['complete_native_stages'][-1]['key']]):
            raise ValueError('Whole calculation preservation or single publication differs')
        prior=store.strict(whole[store.HEAD]);projected=store.projection(prior)
        if projected['unqualified_legacy_calculation'] != prior or len(projected['countries']) != len(prior['countries']):
            raise ValueError('Complete predecessor country diagnostic lost')
        if any(projected[k] is not False for k in store.PERMISSIONS):
            raise ValueError('Unsupported authority remains')
        live=store.read(s3,BUCKET,store.HEAD)
        if live is None:raise ValueError('Current public packet missing')
        doc=live['packet'];native={'status':'pending_original_1240_schedule','generated_at':doc['generated_at'],'version':doc.get('version')}
        if doc.get('contract')=='global-recession-research.v1':
            ctx=doc['publication_context']
            if ctx['compiler_sha256'] != store.compiler_hashes():raise ValueError('Native compiler differs')
            raw_manifest=fetch(ctx['acquisition_manifest']['key'])
            if len(raw_manifest) != ctx['acquisition_manifest']['bytes']:raise ValueError('Manifest length differs')
            native_manifest=store.strict(raw_manifest)
            replay_native=store.replay_native(compiler,native_manifest,fetch)
            final=store.strict(fetch(native_manifest['complete_native_stages'][-1]['key']))
            if doc != {**store.projection(final),'publication_context':ctx}:raise ValueError('Complete public projection differs')
            if native_manifest['predecessor']:fetch(native_manifest['predecessor']['key'])
            native.update(status='complete_native_acquisition_and_calculation_replayed',replay=replay_native,
                          acquisition_manifest_sha256=store.sha(raw_manifest))
        if runtime(lam,s3,events,scheduler,FN) != before:raise ValueError('Runtime changed during verification')
        r.kv(expected_commit=expected,actual_runtime=before,baseline_sha256=BASELINE_SHA,
             fixture_scope='Complete retained derived inputs, both native calculations and offline clock replay in memory. FRED disabled for this fixture; not original-provider replay of the historical baseline.',
             complete_derived_input_bytes={k:len(v) for k,v in whole.items()},
             fixture_countries=len(fixture['countries']),fixture_excluded=len(fixture['excluded']),
             complete_predecessor_countries=len(prior['countries']),complete_predecessor_preserved=True,
             fixture_replay=replay,fixture_public_head_writes=memory.writes.count(store.HEAD),
             native_publication=native,current_head_sha256=store.sha(live['raw']),
             provider_requests=0,native_invocations=0,archive_writes=0,public_writes=0,history_writes=0,
             account_reads=0,notifications_sent=0,schedules_changed=0,
             scope='Retained evidence, exact calculation replay and research authority boundary. Definition validation, original historical vintages, calibration and portfolio qualification remain open.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
