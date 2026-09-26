"""Verify exact Sentinel source closure and any normal native original-source head."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,subprocess,sys,urllib.request,urllib.error
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','scripts','aws/lambdas/justhodl-us10y-sentinel/source')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from replay_us10y_sentinel import verify
import sentinel_store as store
FUNCTION='justhodl-us10y-sentinel';BUCKET='justhodl-dashboard-live'


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FUNCTION+'/source'],cwd=ROOT,text=True).strip()
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6161_sentinel_original_runtime') as r:
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')],cwd=ROOT,check=True)
        before=runtime(lam,s3,events,scheduler,FUNCTION)
        if before['receipt']!={'status':'matched','commit':expected}:raise ValueError('Exact source receipt required')
        if before['timeout']!=300 or before['memory_mb']!=512:raise ValueError('Existing runtime reserve changed')
        if not any(row['expression']=='cron(20 0,6,12,16,20 * * ? *)' and row['state']=='ENABLED' and row['timezone']=='UTC' for row in before['schedules']):
            raise ValueError('Existing native clock changed')
        read=store.reader(s3,BUCKET);raw=read(store.model.CURRENT);packet=store.strict(raw)
        request=urllib.request.Request('https://justhodl.ai/'+store.model.CURRENT+'?exact=1&nogen=1',
            headers={'User-Agent':'justhodl-verify-release/1.0','Cache-Control':'no-cache'})
        if bounded(urllib.request.urlopen(request,timeout=40))!=raw:raise ValueError('Anonymous current packet differs')
        native=packet.get('contract')==store.model.CONTRACT
        proof=verify(packet,read) if native else {'status':'pending_normal_schedule','generated_at':packet.get('generated_at')}
        protection='pending_native_archive'
        if native:
            key=packet['original_sources']['SP500']['sources']['observations']['evidence']['key']
            for origin in ('https://justhodl-data-proxy.raafouis.workers.dev/', 'https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/'):
                try:
                    with urllib.request.urlopen(urllib.request.Request(origin+key,method='HEAD'),timeout=30) as response:
                        raise ValueError('Complete source archive is anonymously readable')
                except urllib.error.HTTPError as exc:
                    if exc.code!=403:raise ValueError('Archive protection not established') from None
            protection='anonymous_edge_and_s3_denied'
        if runtime(lam,s3,events,scheduler,FUNCTION)!=before:raise ValueError('Package or schedule changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,normal_publication=proof,
            public_sha256=hashlib.sha256(raw).hexdigest(),public_bytes=len(raw),archive_protection=protection,checked_at=datetime.now(timezone.utc).isoformat(),
            producer_invocations=0,consumer_invocations=0,provider_requests=0,public_writes=0,
            private_account_reads=0,notifications_sent=0,schedules_changed=0,
            scope='Existing source archives are read only if a normal native publication exists. No forced acquisition or publication.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
