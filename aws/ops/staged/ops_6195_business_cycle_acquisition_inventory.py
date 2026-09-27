"""Preserve exact Business Cycle code and enumerate its native price-read family.

Metadata inventory only: no price-body/provider acquisition, producer invocation,
account/secret read, public/history write or schedule change.
"""
from pathlib import Path
from datetime import datetime,timedelta,timezone
import base64,hashlib,json,subprocess,sys,urllib.request
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-global-business-cycle/source')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from release_package_evidence import shared_imports
import business_cycle_store as store
import retained_access_evidence as access
FN='justhodl-global-business-cycle'
BUCKET='justhodl-dashboard-live'
PREFIX='data/warm/polygon-full/grouped/'
SOURCE_SHA='e5b53343b86b8a525b5af5dc8f8552ad6ca43b706a0c33b87e544e1b2645995a'
EXPECTED_COMMIT='c6c61129abccdad8a754f68e8bc52a281af24437'


def inventory(client,at):
    """Match the native five-year key selection; retain every selected item."""
    if at.tzinfo is None:raise ValueError('Aware inventory clock required')
    today=at.astimezone(timezone.utc).date();cutoff=(today-timedelta(days=365*5)).isoformat()
    selected=[];excluded=[];pages=0;seen=set()
    paginator=client.get_paginator('list_objects_v2')
    for year in range(today.year-5,today.year+1):
        prefix=PREFIX+str(year)+'/'
        for page in paginator.paginate(Bucket=BUCKET,Prefix=prefix):
            pages+=1
            if 'Contents' in page and not isinstance(page['Contents'],list):raise ValueError('Malformed listing')
            for item in page.get('Contents',[]):
                key=item.get('Key')
                if not isinstance(key,str) or not key.startswith(prefix) or key in seen:raise ValueError('Listing identity or pagination overlap')
                seen.add(key)
                if type(item.get('Size')) is not int or item['Size']<0 or not item.get('ETag') or not isinstance(item.get('LastModified'),datetime) or item['LastModified'].tzinfo is None:
                    raise ValueError('Complete object metadata required')
                row={**item,'LastModified':item['LastModified'].isoformat()}
                if key.endswith('.json.gz') and key.rsplit('/',1)[1][:10]>=cutoff:selected.append(row)
                else:excluded.append(row)
    selected.sort(key=lambda x:x['Key']);excluded.sort(key=lambda x:x['Key'])
    return {'contract':'business-cycle-price-acquisition-inventory.v1','captured_at':at.isoformat(),
            'snapshot_atomic':False,'listing_pages':pages,'native_years':5,'native_workers':12,
            'cutoff_date':cutoff,'selected':selected,'excluded':excluded,
            'selected_stored_bytes':sum(r['Size'] for r in selected),
            'largest_stored_object_bytes':max((r['Size'] for r in selected),default=0),
            'source_bodies_read':0,'decompressed_sizes_verified':False,'original_provider_verified':False}


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_business_cycle_acquisition_inventory.py')],cwd=ROOT,check=True)
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6195_business_cycle_acquisition_inventory') as r:
        source=ROOT/'aws/lambdas'/FN/'source'
        if store.sha((source/'lambda_function.py').read_bytes())!=SOURCE_SHA:raise ValueError('Native acquisition selector changed')
        before=runtime(lam,s3,events,scheduler,FN)
        if before['receipt']!={'status':'matched','commit':EXPECTED_COMMIT}:raise ValueError('Expected current package required')
        actual=lam.get_function(FunctionName=FN)
        raw=bounded(urllib.request.urlopen(actual['Code']['Location'],timeout=40))
        if base64.b64encode(hashlib.sha256(raw).digest()).decode()!=before['code_sha256']:raise ValueError('Actual complete package differs')
        package=store.retain(s3,BUCKET,raw)
        paths=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
        paths+=shared_imports(ROOT,paths)+[source.parent/'config.json']
        sources={p.relative_to(ROOT).as_posix():store.retain(s3,BUCKET,p.read_bytes()) for p in paths}
        population=inventory(s3,datetime.now(timezone.utc))
        ref=store.retain(s3,BUCKET,store.encode(population))
        archive=store.retain(s3,BUCKET,store.encode({'contract':'business-cycle-acquisition-baseline.v1',
            'runtime':before,'whole_package':package,'repository_sources':sources,'inventory':ref}))
        privacy=access.summarize([access.check(k) for k in (package['key'],ref['key'],archive['key'])])
        if not privacy['all_denied']:raise ValueError('Complete code and inventory must remain protected')
        if runtime(lam,s3,events,scheduler,FN)!=before:raise ValueError('Runtime changed during inventory')
        r.kv(actual_runtime=before,baseline=archive,whole_package=package,inventory=ref,
             selected_objects=len(population['selected']),excluded_objects=len(population['excluded']),
             selected_stored_bytes=population['selected_stored_bytes'],largest_stored_object_bytes=population['largest_stored_object_bytes'],
             listing_pages=population['listing_pages'],native_years=5,native_workers=12,**privacy,
             market_source_bodies_read=0,provider_requests=0,native_invocations=0,account_reads=0,
             notifications_sent=0,public_writes=0,history_writes=0,schedules_changed=0,
             scope='Complete current code plus native warehouse selection metadata. No historical price body, provider original, decoded size, runtime overhead or point-in-time availability is certified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
