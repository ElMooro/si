"""Read-only Crisis Composite runtime/input audit; preserve complete public predecessors."""
from datetime import datetime,timezone
from pathlib import Path
import ast,base64,hashlib,io,json,sys,urllib.request,urllib.error,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops')]
from ops_report import report
BUCKET='justhodl-dashboard-live';FN='justhodl-crisis-composite'
PRIVATE='audit-private/20260909-originals/crisis-composite/'
MAX=32*1024*1024

def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if len(raw)>MAX:raise ValueError('public source exceeds bound')
    return raw
def public(key):
    return bounded(urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30))
def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)

def main():
    lam=boto3.client('lambda',region_name='us-east-1');s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5909_crisis_composite_preflight') as r:
        cfg=lam.get_function_configuration(FunctionName=FN)
        archive=bounded(urllib.request.urlopen(lam.get_function(FunctionName=FN)['Code']['Location'],timeout=45))
        assert base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
        with zipfile.ZipFile(io.BytesIO(archive)) as z:source=z.read('lambda_function.py')
        assert source==(ROOT/'aws/lambdas'/FN/'source/lambda_function.py').read_bytes()
        receipt=json.loads(public('data/ops/releases/'+FN+'.json'));assert receipt['code_sha256']==cfg['CodeSha256'] and receipt['commit']=='e349fe43a1e3038a17e2e8b95e1140711652698e'
        r.kv(runtime={'commit':receipt['commit'],'code_sha256':cfg['CodeSha256'],'source_matches':True,'source_bytes':len(source)},
             engine_invocations=0,private_account_reads=0,notifications_sent=0,portfolio_writes=0,paid_ai_calls=0)
        preserved={}
        for leaf in ('crisis-composite','defcon','crisis-composite-history'):
            raw=bounded(s3.get_object(Bucket=BUCKET,Key='data/'+leaf+'.json')['Body'])
            sha=hashlib.sha256(raw).hexdigest();key=PRIVATE+sha+'.bin'
            try:s3.put_object(Bucket=BUCKET,Key=key,Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
            except Exception as exc:
                if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('PreconditionFailed','412','ConditionalRequestConflict','409'):raise
            assert bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])==raw
            assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
            exposed=public('data/'+leaf+'.json');assert json.loads(exposed)==json.loads(raw)
            preserved[leaf]={'sha256':sha,'bytes':len(raw),'anonymous_original_denied':True,'public_packet_matches':True}
        r.kv(whole_preceding_products=preserved)
        tree=ast.parse(source.decode('utf-8'));assignment=next(n for n in tree.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='COMPONENTS')
        keys=[ast.literal_eval(row.elts[0]) for row in assignment.value.elts]
        permitted=('data/eurodollar-plumbing.json','data/global-sovereign.json','data/crisis-plumbing.json','data/treasury-noise.json',
            'data/credit-stress.json','data/regime-composite.json','data/vol-surface.json','data/market-internals.json','data/global-liquidity.json',
            'data/leading-markets.json','data/canary-grid.json','data/global-stress.json','data/dollar-radar.json','data/ecb-hist/ciss_ea.json')
        assert set(keys)==set(permitted),'Review any expanded context before reading'
        inputs={}
        for key in keys:
            try:
                raw=public(key);doc=json.loads(raw)
                assert isinstance(doc,dict),'public object required'
                inputs[key]={'http_status':200,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
                    'contract':doc.get('contract'),'generated_at':doc.get('generated_at'),'declared_quality':doc.get('quality'),
                    'declared_calls_eligible':doc.get('calls_eligible'),'declared_sizing_eligible':doc.get('sizing_eligible'),
                    'has_replay':isinstance(doc.get('replay'),dict),'root_keys':list(doc)[:55]}
            except urllib.error.HTTPError as exc:inputs[key]={'http_status':exc.code}
        r.kv(public_input_inventory=inputs)
        rule=events.describe_rule(Name='crisis-composite-hourly');targets=[]
        for page in events.get_paginator('list_targets_by_rule').paginate(Rule='crisis-composite-hourly'):targets+=page.get('Targets',[])
        bound=[t for t in targets if t['Arn'].split(':function:')[-1].split(':')[0]==FN]
        assert rule['State']=='ENABLED' and len(bound)==1 and rule['ScheduleExpression']=='cron(15 * * * ? *)'
        r.kv(schedule={'state':rule['State'],'expression':rule['ScheduleExpression'],'bound_targets':len(bound)},
             next_work='Replace arbitrary label mappings and weight renormalization with original-source dependency-aware crisis research; block unqualified DEFCON playbooks and downstream portfolio/alert authority.')

if __name__=='__main__':
    try:main()
    except Exception:
        print('Crisis preflight failed; inspect the committed report before retrying. No producer was invoked.')
        sys.exit(1)
