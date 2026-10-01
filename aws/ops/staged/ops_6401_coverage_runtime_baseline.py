"""Read-only pre-repair native package/control baseline; no engine data access."""
from pathlib import Path
import hashlib,json,re,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import runtime,BUCKET
FN='justhodl-coverage-gap-report'
SOURCE_COMMIT='281862152ccba8f7f324580068b349d13a9d55f7'
SOURCE_HASHES={'aws/lambdas/justhodl-coverage-gap-report/source/lambda_function.py': 'cfe938f70fc781c211088d0ec1e60bb95043e559759d25955dfdeae7a5912258'}


class ReceiptOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**kw):
        if kw!={'Bucket':BUCKET,'Key':'data/ops/releases/'+FN+'.json'}:raise ValueError('Exact public native receipt only')
        return self.client.get_object(**kw)


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)


def validate(value):
    receipt=value.get('receipt')
    matched=(isinstance(receipt,dict) and set(receipt)=={'status','commit'} and receipt.get('status')=='matched'
             and isinstance(receipt.get('commit'),str) and re.fullmatch('[a-f0-9]{40}',receipt['commit']))
    if value.get('function_name')!=FN or not (matched or receipt=={'status':'missing_predecessor_receipt'}):
        raise ValueError('Hash-matched predecessor receipt or explicit missing receipt required')
    for key in ('source_files_checked','handler_bytes','timeout','memory_mb','ephemeral_storage_mb'):
        if type(value.get(key)) is not int or value[key]<=0:raise ValueError('Positive exact runtime counts required')
    for key in ('code_sha256','runtime','handler','role'):
        if type(value.get(key)) is not str or not value[key]:raise ValueError('Native control identity required')
    if not isinstance(value.get('architectures'),list) or not value['architectures'] or not isinstance(value.get('schedules'),list):
        raise ValueError('Complete native control arrays required')
    if not all(type(x) is str and x for x in value['architectures']):raise ValueError('Architecture identity required')
    for row in value['schedules']:
        if not isinstance(row,dict) or any(type(row.get(k)) is not str or not row[k] for k in ('kind','name','state','expression')):
            raise ValueError('Complete native schedule identity required')
        if type(row.get('native_targets')) is not int or row['native_targets']<1:raise ValueError('Bound native target required')


def normalized(value):
    validate(value)
    return {**value,'schedules':sorted(value['schedules'],key=lambda r:(r['kind'],r.get('group','default'),r['name']))}


def main():
    import boto3
    from ops_report import report
    for path,digest in SOURCE_HASHES.items():
        if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=digest:raise ValueError('Reviewed predecessor source changed')
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    clients=(lam,ReceiptOnly(s3),events,scheduler)
    with report('ops_6401_coverage_runtime_baseline') as out:
        before=normalized(runtime(*clients,FN));after=normalized(runtime(*clients,FN))
        if encoded(before)!=encoded(after):raise ValueError('Native producer changed during baseline inspection')
        cfg=json.loads((ROOT/'aws/lambdas'/FN/'config.json').read_bytes())
        declared={'function_name':cfg['function_name'],'runtime':cfg['runtime'],'handler':cfg['handler'],
                  'timeout':cfg['timeout'],'memory_mb':cfg['memory'],'role':cfg['role']}
        matches=encoded({k:before[k] for k in declared})==encoded(declared)
        enabled=bool(before['schedules']) and all(r['state']=='ENABLED' for r in before['schedules'])
        out.kv(evidence={'status':'baseline_observed' if matches and enabled else 'baseline_observed_review_required',
          'repository_source_commit':SOURCE_COMMIT,'commit_bound_release_verified':before['receipt'].get('status')=='matched','reviewed_source_hashes':SOURCE_HASHES,'native_before':before,'native_after':after,
          'declared_settings':declared,'declared_settings_match':matches,'all_observed_schedules_enabled':enabled,
          'normal_publication_verified':False,'source_replay_verified':False,'investment_authority':False,
          'native_invocations':0,'provider_requests':0,'current_packet_reads':0,'private_reads':0,'account_reads':0,
          'consumer_reads':0,'native_writes':0,'schedule_changes':0,'application_log_queries':0,
          'scope':'Exact native package, public release receipt and selected resource/schedule controls only. No environment values, signed package URL, source summaries, original archive, current report or logs are returned.'})


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
