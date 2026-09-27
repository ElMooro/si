"""Read-only package/cadence and stored Macro Leads explanation replay."""
from pathlib import Path
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-page-ai/source')]
from ops_report import report
from market_runtime_evidence import runtime
import page_explanation_store as store
import retained_access_evidence as access
FN='justhodl-page-ai';BUCKET='justhodl-dashboard-live'


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6217_page_explanation_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
        actual=runtime(lam,s3,events,scheduler,FN);r.kv(actual_runtime_before_validation=actual)
        if actual['receipt']!={'status':'matched','commit':expected} or actual['source_files_checked']!=6:raise ValueError('Exact six-source release required')
        accepted=json.loads((ROOT/'docs/audit/2026-09-27/page-explanation-original-baseline.json').read_bytes())
        original=accepted['producer_runtimes'][FN];cfg=original['runtime']
        mapping={'function_name':'FunctionName','runtime':'Runtime','handler':'Handler','timeout':'Timeout','memory_mb':'MemorySize','architectures':'Architectures','role':'Role'}
        if any(actual[k]!=cfg[v] for k,v in mapping.items()) or actual['ephemeral_storage_mb']!=cfg['EphemeralStorage']['Size'] or actual['schedules']!=original['schedules']:raise ValueError('Original runtime or either original schedule differs')
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
        got=store.get(s3,BUCKET,'data/page-ai/macro-leads.json')
        if got is None:raise ValueError('Stored public Macro Leads explanation absent')
        packet=store.strict(got['raw']);publication={'status':'pending_original_wave_cursor','generated_at':packet.get('generated_at'),'bytes':len(got['raw']),'sha256':store.sha(got['raw']),'page':packet.get('page')}
        protected=[accepted['baseline']['key']]
        if packet.get('contract')==store.CONTRACT:
            publication.update(status='complete_stored_inputs_and_inventory_replayed',replay=store.replay(s3,BUCKET,packet),source_coverage=packet['source_coverage'])
            protected.append(packet['publication_context']['manifest']['key'])
        privacy=access.summarize([access.check(k) for k in protected])
        if not privacy['all_denied']:raise ValueError('Research originals must remain private')
        if runtime(lam,s3,events,scheduler,FN)!=actual:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=actual,compiler_sha256=store.compiler_hashes(),native_publication=publication,**privacy,
             native_invocations=0,provider_requests=0,model_requests=0,archive_writes=0,public_writes=0,schedule_changes=0,account_reads=0,
             scope='Whole source inventory only. Existing 05:10 rule and hourly :15 Scheduler retained. No model, observation-definition, forecast or portfolio qualification; other pages require their native cursor publications.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
