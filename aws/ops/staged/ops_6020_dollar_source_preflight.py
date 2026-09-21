"""Retain Dollar Radar predecessors and reconstruct existing canonical FRED originals.

No legacy handler invocation: it contains a Telegram side effect. This audit
reads approved public research, Lambda packages and schedule descriptions only.
Protected retention is the only mutation; no public head or provider collection.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
from threading import Lock
import gzip,hashlib,io,json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from release_package_evidence import check_packages
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import canonical_fred_replay

BUCKET='justhodl-dashboard-live'
PREFIX='audit-private/20260909-originals/dollar-research/'
REQUEST='chatgpt-dollar-source-preflight-6020'
STATUS=PREFIX+'requests/'+hashlib.sha256(REQUEST.encode()).hexdigest()+'.json'
FN='justhodl-dollar-radar'
PACKETS=('data/dollar-radar.json','data/dollar-radar-history.json','data/report-measurements.json',
    'data/eurodollar-stress.json','data/cb-stance.json','data/china-liquidity.json','data/cftc-all-cache.json',
    'data/bond-vol.json','data/repo-market.json','data/indicator-bus.json')
SERIES=('DTWEXBGS','DTWEXAFEGS','DTWEXEMEGS','RTWEXBGS','DEXUSEU','DEXUSUK','DEXJPUS','DEXCHUS',
    'DEXCAUS','DEXMXUS','DEXKOUS','DEXSZUS','DEXINUS','DEXBZUS','DEXUSAL','DEXSIUS','DEXTAUS','DEXSDUS',
    'WALCL','WRESBAL','RRPONTSYD','WTREGEN','DFII10','DGS10','DGS2','IRLTLT01DEM156N','VIXCLS',
    'BAMLH0A0HYM2','DCOILWTICO','SWPT','NFCI','T10YIE')
CONSUMERS=tuple('justhodl-'+name for name in ('alpha-compass','auction-interpreter','calibration-fleet',
    'canary-warroom','correlation-break-trade-router','crypto-confluence','cross-asset-flow-state',
    'crisis-composite','cycle-clock','equity-confluence','history-snapshotter','katlin','market-interpreter',
    'master-allocator','morning-intelligence','regime-conditional-router','rotation-dashboard','risk-regime',
    'signal-board','streaming-fanout','wl-fusion'))
MAX=32*1024*1024


def now():return datetime.now(timezone.utc).isoformat()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def missing(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code','')) in ('404','NoSuchKey')
def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if not 0<len(raw)<=MAX:raise ValueError('Dollar original byte bound')
    return raw
def get(s3,key):return bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
def retained(s3,raw):
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Whole bounded original required')
    key=PREFIX+sha(raw)+'.bin'
    try:s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code','')) not in ('PreconditionFailed','ConditionalRequestConflict','409','412'):raise
    if get(s3,key)!=raw:raise ValueError('Protected original readback differs')
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}
def status_write(s3,doc,claim=False):
    raw=encoded(doc)
    s3.put_object(Bucket=BUCKET,Key=STATUS,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    if get(s3,STATUS)!=raw:raise ValueError('Audit request readback differs')
def capture(s3,key):
    if key not in PACKETS:raise ValueError('Explicit public Dollar research key required')
    try:raw=get(s3,key)
    except Exception as exc:
        if not missing(exc):raise
        return {'source_key':key,'status':'missing','acquired_at':now(),'original':None},None
    doc=json.loads(raw)
    if not isinstance(doc,(dict,list)):raise ValueError('Public research object or history required')
    return {'source_key':key,'status':'retained','acquired_at':now(),'original':retained(s3,raw)},doc
def canonical_key(key):
    return isinstance(key,str) and bool(re.fullmatch(r'data/(?:evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz|report-research/(?:runs|inputs|outputs|compilers)/[a-f0-9]{64}\.(?:json|py))',key))
def describe(doc):
    if doc is None:return {'status':'missing'}
    if isinstance(doc,list):return {'shape':'list','rows':len(doc)}
    return {'shape':'object','root_fields':sorted(doc),'contract':doc.get('contract'),
        'generated_at':doc.get('generated_at'),'as_of':doc.get('as_of'),'replay':doc.get('replay'),
        'quality':doc.get('quality'),'history_rows':len(doc.get('rows') or doc.get('history') or []),
        'reported_authority':{k:doc.get(k) for k in ('regime','pressure','score','call','calls_eligible','forecast_qualified','sizing_eligible')},
        'original_provider_replay_performed_by_inventory':False}
def inventory(canonical,restored):
    out={}
    for sid,entry in restored.items():
        if entry is None:out[sid]={'status':'absent'};continue
        definitions=entry['definition'].get('seriess',[]);rows=entry['observations'].get('observations',[])
        out[sid]={'status':'originals_replayed','definition':[{k:x.get(k) for k in ('id','title','units','frequency_short','seasonal_adjustment_short','observation_start','observation_end')} for x in definitions],
            'acquired_at':entry['acquired_at'],'rows':len(rows),'nonmissing_rows':sum(x.get('value') not in (None,'.','') for x in rows),
            'first_date':min((x['date'] for x in rows),default=None),'last_date':max((x['date'] for x in rows),default=None),
            'canonical_measurement':{k:canonical['measurements'][sid].get(k) for k in ('current_decimal','unit','date','quality')}}
    return out


def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_6020_dollar_source_preflight') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_dollar_source_preflight.py')],cwd=ROOT,check=True)
        try:prior=json.loads(get(s3,STATUS))
        except Exception as exc:
            if not missing(exc):raise
            prior=None
        if prior:
            assert prior['status']=='complete','Inspect existing incomplete request; do not recapture'
            ref=prior['manifest'];raw=get(s3,ref['key']);assert len(raw)==ref['bytes'] and sha(raw)==ref['sha256']
            manifest=json.loads(raw);r.kv(adopted_completed_request=True)
        else:
            status_write(s3,{'status':'claimed','request_id':REQUEST,'generated_at':now()},claim=True)
            actual=runtime(lam,s3,events,scheduler,FN)
            # Consumer mismatch is an audit finding, never a qualification claim.
            with ThreadPoolExecutor(max_workers=4) as pool:
                packages=[result[0] for result in pool.map(lambda fn:check_packages(lam,ROOT,[fn]),CONSUMERS)]
            refs={};docs={};total=0
            for key in PACKETS:
                refs[key],docs[key]=capture(s3,key);total+=(refs[key]['original'] or {}).get('bytes',0)
                assert total<=128*1024*1024,'Whole packet capture budget'
                status_write(s3,{'status':'capturing','request_id':REQUEST,'captures':refs})
            assert docs[PACKETS[0]] is not None and docs[PACKETS[1]] is not None,'Complete live predecessor and history required'
            originals={};lock=Lock();source_bytes=0
            def read(key):
                nonlocal source_bytes
                if not canonical_key(key):raise ValueError('Unreviewed canonical source path')
                raw=get(s3,key)
                if key.endswith('.gz'):raw=bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)))
                original=retained(s3,raw)
                with lock:
                    if key not in originals:source_bytes+=len(raw)
                    if source_bytes>128*1024*1024:raise ValueError('Canonical original byte budget')
                    originals[key]=original
                return raw
            canonical=docs['data/report-measurements.json'];restored=canonical_fred_replay.restore(canonical,SERIES,read)
            manifest={'contract':'dollar-source-preflight.v1','generated_at':now(),'runtime':actual,
                'consumer_packages':packages,'captures':refs,'packet_inventory':{k:describe(v) for k,v in docs.items()},
                'canonical_originals':originals,'canonical_inventory':inventory(canonical,restored),
                'canonical_replay':canonical['replay'],'retained_packet_bytes':total,'retained_canonical_bytes':source_bytes,
                'forecast_qualified':False,'sizing_eligible':False,'calls_eligible':False}
            ref=retained(s3,encoded(manifest));status_write(s3,{'status':'complete','request_id':REQUEST,'manifest':ref})
        protected={STATUS,ref['key'],*(x['original']['key'] for x in manifest['captures'].values() if x['original']),*(x['key'] for x in manifest['canonical_originals'].values())}
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        problems=[x for x in manifest['consumer_packages'] if not x['pass']]
        r.kv(retained_manifest=ref,runtime=manifest['runtime'],packet_inventory=manifest['packet_inventory'],canonical_inventory=manifest['canonical_inventory'],
            consumer_packages_checked=len(manifest['consumer_packages']),consumer_package_mismatches=problems,
            retained_packet_bytes=manifest['retained_packet_bytes'],retained_canonical_bytes=manifest['retained_canonical_bytes'],
            protected_artifacts_checked=len(protected),originals_anonymously_denied=True,
            provider_requests=0,engine_invocations=0,public_head_writes=0,private_account_reads=0,paid_ai_calls=0,
            notifications_sent=0,portfolio_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
