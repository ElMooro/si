"""Retain complete freight/air/trade/macro predecessors without running them.

Only declared public research packets, their research history, original code and
runtime/schedule metadata. No provider requests, account reads or native invokes.
"""
from pathlib import Path
from datetime import datetime, timezone
import ast, hashlib, json, re, subprocess, sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged')]
from ops_report import report
from market_runtime_evidence import bounded
from ops_6204_shipping_consumer_baseline import runtime
import retained_access_evidence as access
BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/freight-research/'
PINS={'justhodl-freight-pulse':'8c39ed0209c0103b0e1faf9b650d063c156caf398beb0ef4ad512c93df6a845a',
      'justhodl-air-cargo':'6fa370a2ae7f1082f110180abcf16aedfc71d1c04e76598d81405e78c613ea61',
      'justhodl-trade-nowcast':'ffca69fa5e6ec7d009dab8a1ef0125ae59d8e97734ed6c567f250364a5b45a49',
      'justhodl-macro-leads':'8de2b9eb82b35f78d44e20fe6cc18c3eb4177f2c47dc76cd9b2a90abd945b94f'}
KEYS=('data/freight-pulse.json','data/air-cargo.json','data/trade-nowcast.json','data/macro-leads.json',
      'air/hkia-cargo-levels.json','data/impact/exposure-graph.json','data/impact/betas.json')
ARCHIVE='data/archive/freight-pulse/'
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def source_check(function,raw):
    if function not in PINS or sha(raw)!=PINS[function]:raise ValueError('Unreviewed full producer source')
    ast.parse(raw.decode('utf-8'))
    return {'bytes':len(raw),'sha256':sha(raw),'imported_or_executed':False}


def allowed(key):
    if key in KEYS:return True
    if not isinstance(key,str) or not re.fullmatch(re.escape(ARCHIVE)+r'\d{4}-\d{2}-\d{2}\.json',key):return False
    try:datetime.strptime(key[len(ARCHIVE):-5],'%Y-%m-%d')
    except ValueError:return False
    return True


def retain(s3,raw):
    if not isinstance(raw,bytes) or len(raw)>64*1024*1024:raise ValueError('Complete bounded original required')
    ref={'key':PRIVATE+sha(raw)+'.bin','bytes':len(raw),'sha256':sha(raw)}
    try:s3.put_object(Bucket=BUCKET,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    obj=s3.get_object(Bucket=BUCKET,Key=ref['key']);back=bounded(obj['Body'])
    if obj.get('ContentLength')!=len(back) or back!=raw:raise ValueError('Complete retained readback differs')
    return ref


def capture(s3,key,required=False):
    if not allowed(key):raise ValueError('Undeclared research input')
    try:obj=s3.get_object(Bucket=BUCKET,Key=key)
    except Exception as exc:
        if not required and str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('404','NoSuchKey'):return {'status':'missing'}
        raise
    raw=bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw) or not obj.get('ETag'):raise ValueError('Complete versioned object required')
    ref=retain(s3,raw)
    meta={'original_provider_verified':False}
    try:p=json.loads(raw)
    except (ValueError,UnicodeError):p=None
    if isinstance(p,dict):
        meta.update(generated_at=p.get('generated_at'),version=p.get('version'),contract=p.get('contract'),fields=sorted(p))
        if isinstance(p.get('series'),dict):meta['series_ids']=sorted(p['series'])
    return {'status':'whole_object_retained','original':ref,'etag':obj['ETag'],
            'last_modified':obj['LastModified'].isoformat(),'metadata':meta}


def archive_keys(s3):
    keys=set();tokens=set();token=None
    for _ in range(100):
        params={'Bucket':BUCKET,'Prefix':ARCHIVE,'MaxKeys':1000}
        if token:params['ContinuationToken']=token
        page=s3.list_objects_v2(**params)
        if type(page.get('IsTruncated')) is not bool:raise ValueError('Explicit archive pagination required')
        rows=page.get('Contents',[])
        if not isinstance(rows,list):raise ValueError('Complete archive listing required')
        for row in rows:
            key=row.get('Key') if isinstance(row,dict) else None
            if not allowed(key) or not key.startswith(ARCHIVE) or key in keys:raise ValueError('Unique dated archive identities required')
            keys.add(key)
        if not page['IsTruncated']:return sorted(keys)
        token=page.get('NextContinuationToken')
        if not isinstance(token,str) or not token or token in tokens:raise ValueError('Archive pagination stalled')
        tokens.add(token)
    raise ValueError('Archive exceeds reviewed enumeration bound; nothing truncated')


def main(report_name='ops_6213_freight_original_baseline'):
    subprocess.run([sys.executable,str(ROOT/'tests/test_freight_original_baseline.py')],cwd=ROOT,check=True)
    subprocess.run([sys.executable,str(ROOT/'tests/test_shipping_consumer_baseline.py')],cwd=ROOT,check=True)
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')}
    with report(report_name) as r:
        s3=clients['s3'];baseline={'contract':'freight-original-baseline.v1','captured_at':datetime.now(timezone.utc).isoformat(),
             'snapshot_atomic':False,'source_checks':{},'consumers':{},'captures':{}}
        for fn in PINS:
            baseline['source_checks'][fn]=source_check(fn,(ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_bytes())
            baseline['consumers'][fn]=runtime(clients['lambda'],s3,clients['events'],clients['scheduler'],fn)
        history=archive_keys(s3);total=0
        for key in [*KEYS,*history]:
            row=capture(s3,key,required=key in history);baseline['captures'][key]=row
            total+=row.get('original',{}).get('bytes',0)
            if total>512*1024*1024:raise ValueError('Research input population exceeds reviewed total bound')
        if archive_keys(s3)!=history:raise ValueError('Archive membership changed during complete capture')
        baseline.update(archive_keys=history,archive_membership_rechecked=True,input_bytes=total)
        ref=retain(s3,encode(baseline));protected=[ref['key']]
        protected.extend(row['whole_zip']['key'] for row in baseline['consumers'].values() if 'whole_zip' in row)
        privacy=access.summarize([access.check(k) for k in protected])
        if not privacy['all_denied']:raise ValueError('Original code and research baseline must remain private')
        summaries={fn:{k:v for k,v in row.items() if k not in ('inventory','repository_sources')} for fn,row in baseline['consumers'].items()}
        for fn,row in baseline['consumers'].items():
            if 'inventory' in row:summaries[fn].update({k:row['inventory'][k] for k in ('code_matches_repository','source_files_checked','source_differences')})
        r.kv(baseline=ref,consumer_runtimes=summaries,whole_source_pins=baseline['source_checks'],
             static_inputs={k:baseline['captures'][k] for k in KEYS},archive_count=len(history),archive_first=history[0] if history else None,
             archive_last=history[-1] if history else None,total_input_bytes=total,**privacy,native_invocations=0,provider_requests=0,
             account_reads=0,learning_log_reads=0,notifications_sent=0,public_writes=0,history_writes=0,schedules_changed=0,
             scope='Complete declared code, stored inputs and freight history retained. No economic definition, provider-original vintage, leading relationship or investment effect is qualified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
