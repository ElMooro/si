"""Preserve VRP predecessor and replay existing canonical sources without invocation.

No new provider request, credentials, account data, notification or schedule change.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,gzip,hashlib,io,json,re,subprocess,sys,urllib.request,urllib.error,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from release_package_evidence import shared_imports
import canonical_fred_replay

FN='justhodl-vrp';BUCKET='justhodl-dashboard-live'
PREFIX='audit-private/20260909-originals/vrp-research/'
PACKETS=('data/vrp.json','data/vrp-history.json','data/vix-curve.json','data/vix-curve-history.json',
         'data/vol-surface.json','data/report-measurements.json')
SERIES=('SP500','VIXCLS','VXVCLS','VIX9D')

def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def bounded(stream,limit=32*1024*1024):
    try:raw=stream.read(limit+1)
    finally:stream.close()
    assert len(raw)<=limit,'Reviewed byte bound exceeded'
    return raw
def code(exc):return getattr(exc,'response',{}).get('Error',{}).get('Code')
def retain(s3,raw):
    key=PREFIX+sha(raw)+'.bin'
    try:s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if code(exc) not in ('PreconditionFailed','ConditionalRequestConflict','409','412'):raise
    assert bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])==raw
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}
def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)

def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5939_vrp_source_preflight') as r:
        source=ROOT/'aws/lambdas'/FN/'source';deployed=lam.get_function(FunctionName=FN);cfg=deployed['Configuration']
        assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
        raw=bounded(urllib.request.urlopen(deployed['Code']['Location'],timeout=40),64*1024*1024)
        assert base64.b64encode(hashlib.sha256(raw).digest()).decode()==cfg['CodeSha256']
        paths=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
        expected={p.relative_to(source).as_posix():p for p in paths}
        expected.update({p.name:p for p in shared_imports(ROOT,paths) if not (source/p.name).exists()})
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            for name,p in expected.items():assert z.read(name)==p.read_bytes(),'Actual packaged source differs: '+name
        try:
            receipt=json.loads(bounded(s3.get_object(Bucket=BUCKET,Key='data/ops/releases/'+FN+'.json')['Body']))
            assert receipt['code_sha256']==cfg['CodeSha256']
            receipt_status={'status':'matched','commit':receipt['commit']}
        except Exception as exc:
            if code(exc) not in ('NoSuchKey','404'):raise
            receipt_status={'status':'missing_predecessor_receipt'}
        schedules=[]
        for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):
            for name in page['RuleNames']:
                rule=events.describe_rule(Name=name);targets=events.list_targets_by_rule(Rule=name)['Targets']
                schedules.append({'kind':'EventBridge rule','name':name,'state':rule['State'],'expression':rule.get('ScheduleExpression'),
                    'native_targets':sum(t.get('Arn')==cfg['FunctionArn'] for t in targets)})
        conf=json.loads((source.parent/'config.json').read_bytes());name=conf['eventbridge_scheduler']['schedule_name']
        actual=scheduler.get_schedule(Name=name);assert actual['Target']['Arn']==cfg['FunctionArn']
        schedules.append({'kind':'EventBridge Scheduler','name':name,'state':actual['State'],'expression':actual['ScheduleExpression'],
            'timezone':actual['ScheduleExpressionTimezone'],'native_targets':1})
        runtime={'code_sha256':cfg['CodeSha256'],'source_files_checked':len(expected),'handler_bytes':len((source/'lambda_function.py').read_bytes()),
            'timeout':cfg['Timeout'],'memory_mb':cfg['MemorySize'],'receipt':receipt_status,'schedules':schedules}
        refs={};metadata={};packets={};originals={}
        for key in PACKETS:
            try:raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
            except Exception as exc:
                if code(exc) not in ('NoSuchKey','404'):raise
                metadata[key]={'status':'missing'};continue
            p=json.loads(raw);assert isinstance(p,(dict,list))
            refs[key]={**retain(s3,raw),'acquired_at':datetime.now(timezone.utc).isoformat()};packets[key]=p
            metadata[key]={'bytes':len(raw),'shape':type(p).__name__}
            if isinstance(p,dict):metadata[key].update({k:p.get(k) for k in ('contract','generated_at','as_of','updated_at','replay','quality','regime','realized','vrp')})
            if isinstance(p,dict):metadata[key]['history_rows']=len(p.get('snapshots') or p.get('history') or [])
        def read(key):
            assert re.fullmatch(r'data/(?:evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz|report-research/(?:runs|inputs|outputs|compilers)/[a-f0-9]{64}\.(?:json|py))',key),'Unreviewed canonical key'
            raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
            if key.endswith('.gz'):raw=bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)))
            originals[key]=retain(s3,raw)
            return raw
        canonical=packets['data/report-measurements.json'];restored=canonical_fred_replay.restore(canonical,SERIES,read)
        available={}
        for sid,entry in restored.items():
            if entry is None:available[sid]={'status':'absent'};continue
            definition=entry['definition'].get('seriess',[]);rows=entry['observations'].get('observations',[])
            valid=[x for x in rows if isinstance(x,dict) and x.get('value') not in (None,'.','')]
            available[sid]={'status':'originals_replayed','definition':[{k:x.get(k) for k in ('id','title','units','frequency_short','seasonal_adjustment_short','observation_start','observation_end')} for x in definition],
                'acquired_at':entry['acquired_at'],'rows':len(rows),'nonmissing_rows':len(valid),
                'first_date':min((x.get('date','') for x in rows),default=None),'last_date':max((x.get('date','') for x in rows),default=None),
                'canonical_measurement':{k:canonical['measurements'][sid].get(k) for k in ('value','unit','observation_date','quality')}}
        manifest={'contract':'vrp-source-preflight.v1','generated_at':datetime.now(timezone.utc).isoformat(),'runtime':runtime,
            'packets':refs,'metadata':metadata,'canonical_originals':originals,'canonical_inventory':available}
        ref=retain(s3,encoded(manifest))
        for item in (ref,refs['data/vrp.json']):
            assert denied('https://justhodl.ai/'+item['key']) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+item['key'])
        r.kv(runtime=runtime,retained_manifest=ref,packet_inventory=metadata,canonical_inventory=available,
            retained_canonical_artifacts=len(originals),engine_invocations=0,provider_requests=0,private_account_reads=0,
            paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,originals_anonymously_denied=True,
            next_work='Native dated same-underlying volatility research; trailing gaps are not expected premium or realized option P&L.')

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
