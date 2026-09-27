"""Preserve the whole Asia producer and its four declared research objects.

Never import or invoke the producer: its imports resolve managed secrets.
Only immutable private copies are written; public/history heads are untouched.
"""
from pathlib import Path
from datetime import datetime,timezone
import ast,hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged')]
from ops_report import report
from market_runtime_evidence import bounded
from ops_6204_shipping_consumer_baseline import runtime
import retained_access_evidence as access
FN='justhodl-asia-leads';BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/asia-leads-research/'
PIN='1a2b4500ee50c13c3889bfe7f23487de6231015f3372830833a9420169897dfb'
KEYS=('data/asia-leads.json','asia/kr-flash-tape.json','kcs/flash-cache.json','asia/tw-orders-levels.json')
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def source_check(raw):
    if sha(raw)!=PIN:raise ValueError('Unreviewed whole Asia producer')
    constants={n.value for n in ast.walk(ast.parse(raw)) if isinstance(n,ast.Constant) and isinstance(n.value,str)}
    if not set(KEYS)<=constants:raise ValueError('Declared research object absent from pinned producer')
    return {'bytes':len(raw),'sha256':sha(raw),'imported_or_executed':False}


def retain(s3,raw):
    if not isinstance(raw,bytes) or len(raw)>64*1024*1024:raise ValueError('Complete bounded original required')
    ref={'key':PRIVATE+sha(raw)+'.bin','bytes':len(raw),'sha256':sha(raw)}
    try:s3.put_object(Bucket=BUCKET,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    obj=s3.get_object(Bucket=BUCKET,Key=ref['key']);back=bounded(obj['Body'])
    if obj.get('ContentLength')!=len(back) or back!=raw:raise ValueError('Complete immutable readback differs')
    return ref


def capture(s3,key):
    if key not in KEYS:raise ValueError('Undeclared Asia research object')
    try:obj=s3.get_object(Bucket=BUCKET,Key=key)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('404','NoSuchKey'):return {'status':'missing'}
        raise
    raw=bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw) or not obj.get('ETag'):raise ValueError('Complete versioned source required')
    return {'status':'whole_object_retained','original':retain(s3,raw),'etag':obj['ETag'],
            'last_modified':obj['LastModified'].isoformat(),'original_provider_verified':False}


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/ops/test_asia_original_baseline.py')],cwd=ROOT,check=True)
    subprocess.run([sys.executable,str(ROOT/'tests/test_shipping_consumer_baseline.py')],cwd=ROOT,check=True)
    checked=source_check((ROOT/'aws/lambdas'/FN/'source/lambda_function.py').read_bytes())
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')}
    with report('ops_6224_asia_original_baseline') as r:
        s3=clients['s3'];actual=runtime(clients['lambda'],s3,clients['events'],clients['scheduler'],FN)
        originals={key:capture(s3,key) for key in KEYS}
        baseline={'contract':'asia-original-baseline.v1','captured_at':datetime.now(timezone.utc).isoformat(),
                  'snapshot_atomic':False,'source_check':checked,'producer':actual,'captures':originals}
        ref=retain(s3,encode(baseline));protected=[ref['key']]
        if 'whole_zip' in actual:protected.append(actual['whole_zip']['key'])
        protected.extend(row['original']['key'] for row in originals.values() if 'original' in row)
        privacy=access.summarize([access.check(key) for key in protected])
        if not privacy['all_denied']:raise ValueError('Original producer and research objects must remain private')
        summary={k:v for k,v in actual.items() if k not in ('inventory','repository_sources')}
        if 'inventory' in actual:summary.update({k:actual['inventory'][k] for k in ('code_matches_repository','source_files_checked','source_differences')})
        r.kv(baseline=ref,actual_producer=summary,source_check=checked,originals=originals,**privacy,
             native_invocations=0,provider_requests=0,account_reads=0,learning_log_reads=0,notifications_sent=0,
             public_writes=0,history_writes=0,schedule_changes=0,
             scope='Whole original code and four exact research objects only. Source definitions, quoted-news extraction, vintages, forecasting and portfolio consequences remain unqualified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
