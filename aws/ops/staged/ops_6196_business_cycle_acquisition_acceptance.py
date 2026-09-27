"""Read-only code and retained native acquisition replay; never invoke producer."""
from pathlib import Path
import importlib.util,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-global-business-cycle/source','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
import business_cycle_store as store
import business_cycle_acquisition as acq
FN='justhodl-global-business-cycle';BUCKET='justhodl-dashboard-live'
BASELINE='486237160e60f960ebf12a1db05064a5983cef706413608e6b0c6e94dacdaa35'


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6196_business_cycle_acquisition_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
        before=runtime(lam,s3,events,scheduler,FN)
        if before['receipt']!={'status':'matched','commit':expected}:raise ValueError('Exact source commit required')
        def fetch(key):
            if not key.startswith(store.PRIVATE) or not key.endswith('.bin') or len(key)!=len(store.PRIVATE)+68:
                raise ValueError('Protected complete evidence path required')
            obj=s3.get_object(Bucket=BUCKET,Key=key);raw=store.bounded(obj['Body'])
            if obj['ContentLength']!=len(raw) or store.sha(raw)!=key[len(store.PRIVATE):-4]:raise ValueError('Complete retained identity differs')
            return raw
        baseline=store.strict(fetch(store.PRIVATE+BASELINE+'.bin'))
        for key in ('timeout','memory_mb','schedules','runtime','handler','architectures','role','ephemeral_storage_mb'):
            if baseline['runtime'][key]!=before[key]:raise ValueError('Native runtime/cadence changed: '+key)
        tests=ROOT/'aws/lambdas'/FN/'tests'
        subprocess.run([sys.executable,str(tests/'run_tests.py')],cwd=ROOT,check=True)
        sys.path.insert(0,str(tests))
        spec=importlib.util.spec_from_file_location('acquisition_test_fixtures',tests/'test_acquisition.py')
        fixtures=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixtures)
        # This compiler uses fake AWS construction. Replay replaces all source
        # reads, clocks and HTTP operations; only retained private blobs are read.
        compiler=fixtures.module(fixtures.Client())
        current=store.read(s3,BUCKET,store.HEAD)
        if current is None:raise ValueError('Existing public head missing')
        packet=current['packet'];ctx=packet.get('publication_context') or {};declared=ctx.get('native_acquisition')
        native={'status':'pending_original_daily_1200_publication','generated_at':packet['generated_at'],'engine_version':packet.get('engine_version')}
        if declared is not None:
            raw_manifest=fetch(declared['manifest']['key'])
            if len(raw_manifest)!=declared['manifest']['bytes']:raise ValueError('Complete acquisition manifest differs')
            manifest=store.strict(raw_manifest)
            if ctx['compiler_sha256']!=store.compiler_hashes():raise ValueError('Native compiler differs')
            replay=acq.replay_native(compiler,manifest,fetch)
            if ctx['complete_unmodified_calculations']!=manifest['complete_native_calculations']:raise ValueError('Calculation identity differs')
            for key,ref in manifest['complete_native_calculations'].items():
                raw=fetch(ref['key'])
                if len(raw)!=ref['bytes']:raise ValueError('Complete calculated output differs')
                if key==store.HEAD and packet!={**store.research_projection(store.strict(raw)),'publication_context':ctx}:
                    raise ValueError('Complete public projection differs')
            native.update(status='complete_native_acquisition_and_calculation_replayed',replay=replay,
                          manifest_sha256=store.sha(raw_manifest),operations=len(manifest['operations']),provider_attempts=len(manifest['http_attempts']))
        if runtime(lam,s3,events,scheduler,FN)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,baseline_sha256=BASELINE,
             synthetic_fixture_scope='All 34 countries, all three full native output packets and all recorded clocks; separately all 1254 warehouse objects across every fixture metadata page. Synthetic tests are not historical provider proof.',
             native_publication=native,provider_requests=0,native_invocations=0,account_reads=0,
             notifications_sent=0,public_writes=0,history_writes=0,schedules_changed=0,
             scope='Whole acquisition and exact parsed-calculation replay. Stored warehouse inputs remain derived; original upstream Polygon responses, definitions, historical vintages, predictive edge and portfolio qualification remain unverified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
