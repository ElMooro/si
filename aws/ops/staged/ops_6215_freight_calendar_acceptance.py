"""Read-only exact freight package, original cadence and scheduled-source replay."""
from pathlib import Path
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-freight-pulse/source','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
import freight_store as store
import lambda_function as native
import retained_access_evidence as access
FN='justhodl-freight-pulse';BUCKET='justhodl-dashboard-live'


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6215_freight_calendar_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
        before=runtime(lam,s3,events,scheduler,FN);r.kv(actual_runtime_before_validation=before)
        if before['receipt']!={'status':'matched','commit':expected} or before['source_files_checked']!=5:raise ValueError('Exact five-source release required')
        accepted=json.loads((ROOT/'docs/audit/2026-09-27/freight-original-baseline.json').read_bytes())
        original=accepted['consumer_runtimes'][FN];cfg=original['runtime']
        mapping={'function_name':'FunctionName','runtime':'Runtime','handler':'Handler','timeout':'Timeout','memory_mb':'MemorySize','architectures':'Architectures','role':'Role'}
        if any(before[k]!=cfg[v] for k,v in mapping.items()) or before['ephemeral_storage_mb']!=cfg['EphemeralStorage']['Size'] or before['schedules']!=original['schedules']:raise ValueError('Original freight runtime or cadence differs')
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
        obj=s3.get_object(Bucket=BUCKET,Key=store.HEAD);raw=store.whole(obj['Body'],obj.get('ContentLength'));p=store.decode(raw)
        publication={'status':'pending_original_daily_1150_publication','generated_at':p.get('generated_at'),'version':p.get('version'),'whole_bytes':len(raw),'whole_sha256':store.sha(raw)}
        protected=[accepted['baseline']['key']]
        if p.get('contract')==store.CONTRACT:
            publication.update(status='complete_native_sources_and_calendar_replayed',replay=store.replay(native,s3,BUCKET,p),
               measurement_states={sid:{k:row.get(k) for k in ('status','definition_status','latest_date','unit','seasonal_adjustment','returned_rows')} for sid,row in p['measurement_review']['series'].items()},
               archive_review=p['archive_review'])
            protected.append(p['publication_context']['manifest']['key'])
        privacy=access.summarize([access.check(k) for k in protected])
        if not privacy['all_denied']:raise ValueError('Research originals must remain private')
        if runtime(lam,s3,events,scheduler,FN)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,compiler_sha256=store.hashes(),native_publication=publication,**privacy,
             native_invocations=0,provider_requests=0,archive_writes=0,public_writes=0,schedule_changes=0,account_reads=0,
             scope='Complete original response and native-calculation replay plus exact monthly descriptive measurements. Inherited composites, weekly distillate interpretation, original vintages, economic leading relationships and portfolio consequences remain unqualified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
