"""Read-only exact Term Premium producer, original workbook and own predecessors."""
from pathlib import Path
import hashlib,json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared','aws/lambdas/justhodl-term-premium/source')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_6272_summary_context_acceptance import expected_commit
import retained_access_evidence as access
import term_premium_store as store
import term_premium_model as model
FN='justhodl-term-premium';BUCKET='justhodl-dashboard-live'
BASELINE=ROOT/'docs/audit/2026-09-28/term-premium-accepted-runtime.json'


def own_predecessor(ref,s3):
    if (not isinstance(ref,dict) or set(ref)!={'key','sha256','bytes'} or not isinstance(ref['sha256'],str)
        or not re.fullmatch('[a-f0-9]{64}',ref['sha256']) or type(ref['bytes']) is not int or not 0<ref['bytes']<=store.MAX
        or ref['key']!=store.PRIVATE+ref['sha256']+'.bin'):
        raise ValueError('Only whole declared Term Premium predecessor allowed')
    raw=store.bounded(s3.get_object(Bucket=BUCKET,Key=ref['key'])['Body'])
    if len(raw)!=ref['bytes'] or store.sha(raw)!=ref['sha256']:raise ValueError('Whole predecessor differs')
    return ref['key']


def publication(raw,s3):
    packet=store.strict(raw)
    if not isinstance(packet,dict):raise ValueError('Whole public producer packet required')
    result={'bytes':len(raw),'sha256':store.sha(raw),'generated_at':packet.get('generated_at'),'contract':packet.get('contract'),
            'status':'pending_original_schedule_publication','original_workbook_replayed':False,'investment_authority':False}
    if packet.get('contract')!=model.CONTRACT:return result,[]
    read=store.reader(s3,BUCKET);output=store.replay(packet,read)
    if set(output['predecessors'])!={'packet','parsed_archive'}:raise ValueError('Both whole legacy predecessors required')
    protected=[own_predecessor(ref,s3) for ref in output['predecessors'].values()]
    if any(output.get(k) is not False for k in model.arithmetic.AUTHORITY):raise ValueError('Research authority differs')
    tables={name:{'rows':len(t['rows']),'columns':len(t['headers']),
                    'first_observation':min(r['observation_date'] for r in t['rows']),
                    'last_observation':max(r['observation_date'] for r in t['rows'])} for name,t in output['tables'].items()}
    result.update(status='complete_native_workbook_replayed',original_workbook_replayed=True,
        source=output['source'],quality=output['quality'],acquisition=output['acquisition'],series=len(output['series']),
        tables=tables,original_arithmetic_checks=output['original_arithmetic_checks'],replay=packet['replay'],
        point_in_time_backtest_qualified=False,own_predecessors_verified=len(protected))
    return result,protected


def latest_source_journal(s3):
    prefix=store.PRIVATE+'requests/';items=[]
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET,Prefix=prefix):
        for item in page.get('Contents',[]):
            if not re.fullmatch(re.escape(prefix)+r'[a-f0-9]{64}\.json',item['Key']):
                raise ValueError('Unexpected own acquisition journal key')
            items.append(item)
    if not items:return {'status':'no_retained_native_acquisition_journal'},[]
    item=max(items,key=lambda row:(row['LastModified'],row['Key']))
    raw=store.bounded(s3.get_object(Bucket=BUCKET,Key=item['Key'])['Body']);journal=store.strict(raw)
    if not isinstance(journal,dict):raise ValueError('Whole own acquisition journal required')
    result={k:journal[k] for k in ('status','started_at','provider_request_attempts','error_type','acquisition','result') if k in journal}
    result.update(key=item['Key'],bytes=len(raw),sha256=store.sha(raw),last_modified=item['LastModified'].isoformat())
    return result,[item['Key']]


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    clients=[boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')]
    with report('ops_6292_term_premium_normal_acceptance') as r:
        expected=expected_commit(FN);before=runtime(*clients,FN)
        if before!=json.loads(BASELINE.read_bytes()) or before['receipt']!={'status':'matched','commit':expected}:
            raise ValueError('Accepted complete package or original runtime/cadence differs')
        raw=store.bounded(clients[1].get_object(Bucket=BUCKET,Key=model.CURRENT)['Body'])
        result,protected=publication(raw,clients[1]);journal,paths=latest_source_journal(clients[1]);protected.extend(paths)
        privacy=access.summarize([access.check(key) for key in sorted(set(protected))])
        if not privacy['all_denied']:raise ValueError('Own protected predecessors are anonymously accessible')
        if runtime(*clients,FN)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,native_publication=result,own_acquisition_journal=journal,predecessor_access=privacy,
            native_invocations=0,provider_requests=0,consumer_output_reads=0,private_state_reads=0,account_reads=0,
            public_writes=0,history_writes=0,schedule_changes=0,
            scope='Exact public Term Premium producer, retained original workbook, own predecessor artifacts and latest own acquisition journal only. No downstream consumers, provider acquisition, model refit or investment qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
