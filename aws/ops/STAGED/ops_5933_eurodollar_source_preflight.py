"""Inspect existing Eurodollar and canonical research without invoking any engine.

Retain whole public predecessor/source packets privately. No new provider fetch,
credential acquisition, account read, notification, schedule or permission change.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,io,json,subprocess,sys,urllib.request,urllib.error,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from ops_report import report
from release_package_evidence import shared_imports

FN='justhodl-eurodollar-stress';BUCKET='justhodl-dashboard-live'
PREFIX='audit-private/20260909-originals/eurodollar-research/'
PACKETS=('data/eurodollar-stress.json','data/report-measurements.json','data/crisis-composite.json','data/credit-stress.json','data/vol-surface.json','data/polygon-fx-regime.json')
SERIES=('STLFSI4','BAMLH0A0HYM2','BAMLC0A0CM','VIXCLS','DTWEXBGS','DTB3','DGS10','SOFR','DFF')
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
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5933_eurodollar_source_preflight') as r:
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
                rule=events.describe_rule(Name=name)
                targets=events.list_targets_by_rule(Rule=name)['Targets']
                schedules.append({'name':name,'state':rule['State'],'schedule':rule.get('ScheduleExpression'),
                    'targets':sum(t.get('Arn')==cfg['FunctionArn'] for t in targets)})
        r.kv(runtime={'code_sha256':cfg['CodeSha256'],'source_files_checked':len(expected),
            'handler_bytes':len((source/'lambda_function.py').read_bytes()),'timeout':cfg['Timeout'],'memory_mb':cfg['MemorySize'],
            'receipt':receipt_status,'schedules':schedules},engine_invocations=0,private_account_reads=0,provider_requests=0,notifications_sent=0,portfolio_writes=0)
        refs={};metadata={}
        for key in PACKETS:
            raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']);p=json.loads(raw)
            refs[key]={**retain(s3,raw),'acquired_at':datetime.now(timezone.utc).isoformat()}
            metadata[key]={'contract':p.get('contract'),'generated_at':p.get('generated_at'),'as_of':p.get('as_of'),
                'replay':p.get('replay'),'bytes':len(raw)}
            if key=='data/crisis-composite.json':
                assert p['contract']=='crisis-research.v1'
                metadata[key]['required_series']={sid:{'unit':p['measurements'][sid]['unit'],
                    'observation_date':p['measurements'][sid]['observation_date'],'quality':p['measurements'][sid]['quality'],
                    'history_rows':len(p['measurements'][sid]['history'])} for sid in SERIES}
        manifest={'contract':'eurodollar-canonical-preflight.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'runtime_code_sha256':cfg['CodeSha256'],'packets':refs,'metadata':metadata}
        ref=retain(s3,encoded(manifest))
        for item in (refs[PACKETS[0]],ref):
            assert denied('https://justhodl.ai/'+item['key']) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+item['key'])
        r.kv(retained_manifest=ref,source_inventory=metadata,originals_anonymously_denied=True,
            next_work='Reconstruct from canonical original-source runs; preserve all nine FRED identities and unqualified FX context. No additional independent vote from reused credit, volatility or macro inputs.')

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
