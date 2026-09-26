"""Verify deployed reader against ten existing objects; never register or invoke."""
from pathlib import Path
import importlib.util,resource,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
from research_source_reader import read,policy
from prospective_journal import projection
FN='justhodl-signal-harvester';BUCKET='justhodl-dashboard-live'
KEYS=('data/data-census.json','data/euro-fragmentation.json','data/feed-catalog.json','data/fortress.json',
      'data/industry-case.json','data/khalid-candidates.json','data/khalid.json','data/quiver-lobbying-cache.json',
      'data/report.json','data/tradingview.json')


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6179_research_source_read_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/research_source_reader.py'],cwd=ROOT,text=True).strip()
        before=runtime(lam,s3,events,scheduler,FN)
        if before['receipt']!={'status':'matched','commit':expected} or (before['memory_mb'],before['timeout'])!=(1024,900):raise ValueError('Exact original runtime and new package required')
        tests=ROOT/'aws/lambdas'/FN/'tests/run_tests.py'
        subprocess.run([sys.executable,str(tests)],cwd=ROOT,check=True)
        spec=importlib.util.spec_from_file_location('harvester_offline_tests',tests);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        extract=module.load()['extract_picks'];rows=[];failures=[]
        for key in KEYS:
            try:
                source,sha,at=read(s3,BUCKET,key);picks=extract(source,key)
                selected=projection(key,source,picks,sha,at) if picks else None
                rows.append({'source_key':key,'stored_sha256':sha,'received_at':at,'generated_at':source.get('generated_at'),
                             'extracted_observations':len(picks),'eligibility_reasons':selected['eligibility_reasons'] if selected else ['no_direction_or_rank_observations'],
                             'unsupported_identity_count':selected['unsupported_identity_count'] if selected else 0})
                del source,picks,selected
            except Exception as exc:
                failures.append({'source_key':key,'error_type':type(exc).__name__,'reason':str(exc) if str(exc).startswith('SOURCE_') else 'source_or_projection_rejected'})
        if runtime(lam,s3,events,scheduler,FN)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,source_read_policy=policy(),sources_read=rows,failures=failures,
            peak_process_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            new_normal_scheduled_capture_verified=False,provider_requests=0,native_invocations=0,forecast_registrations=0,
            public_writes=0,history_writes=0,account_reads=0,schedules_changed=0,
            scope='Whole stored source transport and pure observation projection only; stale/ineligible sources retain their reasons. No original-model or investment qualification.')
        if failures:raise ValueError('One or more complete research sources remain rejected')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
