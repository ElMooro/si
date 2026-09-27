"""Read-only exact China package, original cadence and complete scheduled replay."""
from pathlib import Path
import importlib.util,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-china-liquidity/source')]
from ops_report import report
from market_runtime_evidence import runtime
import retained_access_evidence as access
import china_store as store
FN='justhodl-china-liquidity';BUCKET='justhodl-dashboard-live'


def main():
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')}
    lam,s3,events,scheduler=(clients[n] for n in ('lambda','s3','events','scheduler'))
    with report('ops_6227_china_research_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
        before=runtime(lam,s3,events,scheduler,FN)
        if before['receipt']!={'status':'matched','commit':expected} or before['source_files_checked']!=4:raise ValueError('Exact four-source release required')
        baseline=json.loads((ROOT/'docs/audit/2026-09-27/china-original-baseline.json').read_bytes());original=baseline['actual_producer'];cfg=original['runtime']
        mapping={'function_name':'FunctionName','runtime':'Runtime','handler':'Handler','timeout':'Timeout','memory_mb':'MemorySize','architectures':'Architectures','role':'Role'}
        if any(before[k]!=cfg[v] for k,v in mapping.items()) or before['ephemeral_storage_mb']!=cfg['EphemeralStorage']['Size'] or before['schedules']!=original['schedules']:raise ValueError('Original China runtime or cadence differs')
        path=ROOT/'aws/lambdas'/FN/'tests/run_tests.py'
        subprocess.run([sys.executable,str(path)],cwd=ROOT,check=True)
        spec=importlib.util.spec_from_file_location('china_acceptance_fixture',path);fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
        # The test loader stubs managed secrets and AWS construction. Replay
        # reads retained originals only and never imports live credentials.
        fixture.Tests.setUpClass();native=fixture.Tests.module
        found=store.get(s3,BUCKET,store.HEAD)
        if found is None:raise ValueError('Complete current public head required')
        raw=found['raw'];packet=store.strict(raw)
        publication={'status':'pending_original_daily_1430_publication','bytes':len(raw),'sha256':store.sha(raw),
                     'generated_at':packet.get('generated_at'),'version':packet.get('version')}
        protected=[baseline['baseline']['key']]
        if packet.get('contract')==store.CONTRACT:
            publication.update(status='complete_native_sources_and_china_calendar_replayed',replay=store.replay(native,s3,BUCKET,packet),
                               measurement_states={sid:{k:row.get(k) for k in ('status','unit','concept','frequency','latest_date','freshness','returned_rows')} for sid,row in packet['measurement_review']['series'].items()})
            ref=packet['publication_context']['manifest'];protected.append(ref['key']);plan=store.strict(store.retained(s3,BUCKET,ref))
            for key,expected_cache in plan['projections'].items():
                if key==store.HEAD:continue
                value=store.get(s3,BUCKET,key)
                if value is None or value['raw']!=store.retained(s3,BUCKET,expected_cache):raise ValueError('Complete current cache differs from original plan')
        privacy=access.summarize([access.check(key) for key in protected])
        if not privacy['all_denied']:raise ValueError('Original research and source manifests must remain private')
        if runtime(lam,s3,events,scheduler,FN)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,native_publication=publication,compiler_sha256=store.compiler_hashes(),**privacy,
             native_invocations=0,provider_requests=0,account_reads=0,credential_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='Whole acquired sources, complete history and exact monetary/rate/price calendars. Legacy NBS/PBoC extraction, historical vintages, economic leads and portfolio consequences remain unqualified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
