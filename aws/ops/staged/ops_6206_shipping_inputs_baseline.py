"""Preserve shipping model inputs and full history before qualification repairs.

No producer/provider invocation, account reads, notifications or public writes.
"""
from pathlib import Path
from datetime import datetime,timezone
import ast,hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged')]
from ops_report import report
from market_runtime_evidence import bounded
from ops_6204_shipping_consumer_baseline import runtime,source_check
import retained_access_evidence as access
BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/shipping-input-research/'
BOOM_INPUTS={'data/asia-leads.json','data/portwatch.json','data/china-liquidity.json',
             'data/industry-boom.json','data/air-cargo.json','data/freight-pulse.json'}
CARGO_INPUTS={'data/portwatch.json','data/warm/portwatch/layer-choice.json',
              'data/impact/exposure-graph.json','data/impact/betas.json'}
FUNCTIONS=('justhodl-boom-stage','justhodl-port-cargo')
KEYS=tuple(sorted(BOOM_INPUTS|CARGO_INPUTS|{'boom/boom-stage-history.json','data/boom-stage.json','data/port-cargo.json'}))
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda p:json.dumps(p,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def source_reads(raw):
    source_check('justhodl-boom-stage',raw)
    tree=ast.parse(raw.decode('utf-8'))
    reads={ast.literal_eval(n.args[0]) for n in ast.walk(tree)
           if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='_get'}
    if reads!=BOOM_INPUTS:raise ValueError('Complete reviewed Boom Stage population required')
    return sorted(reads)


def decoded_metadata(raw,key):
    try:p=json.loads(raw)
    except (ValueError,UnicodeError):return {'decoded_type':'invalid','source_vintage_verified':False}
    result={'decoded_type':type(p).__name__,'source_vintage_verified':False}
    if not isinstance(p,dict):return result
    result.update(generated_at=p.get('generated_at'),version=p.get('version'),contract=p.get('contract'))
    if key=='boom/boom-stage-history.json':
        days=p.get('days');result['history_shape_valid']=isinstance(days,dict)
        if isinstance(days,dict):
            result.update(dates=len(days),first_date=min(days,default=None),last_date=max(days,default=None),
                          pair_records=sum(len(v) for v in days.values() if isinstance(v,dict)),
                          malformed_date_rows=sum(not isinstance(v,dict) for v in days.values()))
    for field in ('pairs','ports','countries','signals','accelerating','decelerating'):
        if isinstance(p.get(field),list):result[field+'_rows']=len(p[field])
    return result


def retain(s3,raw):
    if not isinstance(raw,bytes) or len(raw)>64*1024*1024:raise ValueError('Complete bounded bytes required')
    ref={'key':PRIVATE+sha(raw)+'.bin','sha256':sha(raw),'bytes':len(raw)}
    try:s3.put_object(Bucket=BUCKET,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    obj=s3.get_object(Bucket=BUCKET,Key=ref['key']);back=bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(back) or back!=raw:raise ValueError('Whole retained readback differs')
    return ref


def capture(s3,key):
    if key not in KEYS:raise ValueError('Only reviewed public research/history objects allowed')
    try:obj=s3.get_object(Bucket=BUCKET,Key=key)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('NoSuchKey','404'):return {'status':'missing'}
        raise
    raw=bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw) or not obj.get('ETag'):
        raise ValueError('Whole versioned stored object required')
    ref=retain(s3,raw)
    meta=decoded_metadata(raw,key)
    return {'status':'whole_object_retained','original':ref,'etag':obj['ETag'],
            'last_modified':obj['LastModified'].isoformat(),'metadata':meta}


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_shipping_inputs_baseline.py')],cwd=ROOT,check=True)
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')}
    with report('ops_6206_shipping_inputs_baseline') as r:
        baseline={'contract':'shipping-inputs-baseline.v1','captured_at':datetime.now(timezone.utc).isoformat(),
                  'snapshot_atomic':False,'consumers':{},'captures':{}}
        source_reads((ROOT/'aws/lambdas/justhodl-boom-stage/source/lambda_function.py').read_bytes())
        for fn in FUNCTIONS:
            source_check(fn,(ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_bytes())
            baseline['consumers'][fn]=runtime(clients['lambda'],clients['s3'],clients['events'],clients['scheduler'],fn)
        baseline['captures']={k:capture(clients['s3'],k) for k in KEYS}
        ref=retain(clients['s3'],encode(baseline))
        protected={ref['key']}
        protected.update(v['original']['key'] for v in baseline['captures'].values() if 'original' in v)
        for row in baseline['consumers'].values():
            if 'whole_zip' in row:protected.add(row['whole_zip']['key'])
            protected.update(v['key'] for v in row.get('repository_sources',{}).values())
        privacy=access.summarize([access.check(k) for k in sorted(protected)])
        if not privacy['all_denied']:raise ValueError('Retained baseline must remain private')
        r.kv(baseline=ref,captures={k:{a:b for a,b in v.items() if a in ('status','original','metadata')} for k,v in baseline['captures'].items()},
             functions=list(FUNCTIONS),**privacy,native_invocations=0,provider_requests=0,account_reads=0,
             notifications_sent=0,public_writes=0,history_writes=0,schedules_changed=0,
             scope='Complete declared stored research inputs and original history retained. Provider acquisitions, economic interpretation and model performance remain unqualified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
