"""Read-only Crisis Plumbing runtime/data audit and bounded original-source probes."""
from pathlib import Path
import ast,base64,hashlib,io,json,sys,time,urllib.request,urllib.error,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-daily-report-v3/source')]
from ops_report import report
from report_source_store import original,error_label
BUCKET='justhodl-dashboard-live';FN='justhodl-crisis-plumbing'
PRIVATE='audit-private/20260909-originals/crisis-plumbing/'
MAX=32*1024*1024
PROBES=('BAMLEM4BRRBLCRPIOAS','DRTSCLM','ECBESTRVOLWGTTRMDMNRT','NFCINONFINLEVERAGE',
        'OFRFSI','RECPROUSM156N','SAHMREALTIME','WCBSL')


def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if len(raw)>MAX:raise ValueError('source exceeds explicit bound')
    return raw


def public(key):
    return bounded(urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,
        headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=40))


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    lam=boto3.client('lambda',region_name='us-east-1');s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5912_crisis_plumbing_preflight') as r:
        live=lam.get_function(FunctionName=FN);cfg=live['Configuration']
        archive=bounded(urllib.request.urlopen(live['Code']['Location'],timeout=45))
        assert base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
        with zipfile.ZipFile(io.BytesIO(archive)) as z:source=z.read('lambda_function.py')
        assert source==(ROOT/'aws/lambdas'/FN/'source/lambda_function.py').read_bytes(),'Runtime differs; review before rewriting'
        try:
            receipt=json.loads(public('data/ops/releases/'+FN+'.json'))
            assert receipt['code_sha256']==cfg['CodeSha256'],'Existing receipt differs from runtime'
            receipt_status={'commit':receipt['commit'],'matches_runtime':True}
        except urllib.error.HTTPError as exc:
            if exc.code not in (403,404):raise
            receipt_status={'http_status':exc.code,'matches_runtime':None}
        r.kv(runtime={'code_sha256':cfg['CodeSha256'],'source_matches':True,'source_bytes':len(source),
                     'memory_mb':cfg['MemorySize'],'timeout_s':cfg['Timeout'],'prior_receipt':receipt_status},
             engine_invocations=0,private_account_reads=0,notifications_sent=0,portfolio_writes=0,paid_ai_calls=0)
        raw=bounded(s3.get_object(Bucket=BUCKET,Key='data/crisis-plumbing.json')['Body'])
        sha=hashlib.sha256(raw).hexdigest();key=PRIVATE+sha+'.bin'
        try:s3.put_object(Bucket=BUCKET,Key=key,Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
        except Exception as exc:
            if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('PreconditionFailed','412','ConditionalRequestConflict','409'):raise
        assert bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])==raw
        assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
        packet=json.loads(raw);assert json.loads(public('data/crisis-plumbing.json'))==packet
        r.kv(whole_preceding_product={'sha256':sha,'bytes':len(raw),'anonymous_denied':True,'generated_at':packet.get('generated_at')},
             legacy_shape={'keys':list(packet),'composite':packet.get('composite'),'indices':packet.get('crisis_indices'),
                'enrichment_series':{group:{sid:{k:row.get(k) for k in ('value','unit','date','n')} for sid,row in rows.items()}
                    for group,rows in (packet.get('enrichment',{}).get('categories') or {}).items()}})
        definitions={n.targets[0].id:ast.literal_eval(n.value) for n in ast.parse(source.decode('utf-8')).body
            if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id in
            ('ENRICH_SERIES','CRISIS_INDICES','PLUMBING_TIER2','CROSS_CURRENCY_BASIS_INPUTS','FUNDING_CREDIT_SIGNALS')}
        series=sorted(set(definitions['CRISIS_INDICES'])|{v.get('real_id',k) for k,v in definitions['PLUMBING_TIER2'].items()}
            |set(definitions['CROSS_CURRENCY_BASIS_INPUTS'])|{v['fred_id'] for v in definitions['FUNDING_CREDIT_SIGNALS'].values()}
            |{x[0] for x in definitions['ENRICH_SERIES']})
        assert len(series)==53 and set(PROBES)<=set(series),'Review expanded original source scope'
        macro=json.loads(public('data/report-measurements.json'));inventory={}
        for sid in series:
            row=macro.get('measurements',{}).get(sid,{})
            inventory[sid]={k:row.get(k) for k in ('name','current_decimal','date','unit','frequency','quality','acquired_at')}
        funding=json.loads(public('data/eurodollar-plumbing.json'))
        ofr={k:{name:row.get(name) for name in ('id','as_of','value_decimal','unit','quality','evidence','history')}
             for k,row in funding.get('measurements',{}).items() if k.startswith('ofr_fsi:')}
        r.kv(canonical_macro={'generated_at':macro.get('generated_at'),'replay':macro.get('replay'),'series':inventory},
             canonical_funding={'generated_at':funding.get('generated_at'),'contract':funding.get('contract'),'replay':funding.get('replay'),'ofr_fsi':ofr})
        # Existing FRED data credential stays exclusively in runner memory.
        fred_key=boto3.client('ssm',region_name='us-east-1').get_parameter(Name='/justhodl/fred/api-key',WithDecryption=True)['Parameter']['Value']
        probes={};deadline=time.monotonic()+240
        for sid in PROBES:
            try:
                definition,dref,dat=original(s3,BUCKET,sid,'definition',fred_key,deadline)
                obs,oref,at=original(s3,BUCKET,sid,'observations',fred_key,deadline)
                probes[sid]={'definition':definition,'evidence':{'definition':dref,'observations':oref},'acquired_at':at,
                    'rows':len(obs.get('observations',[])),'latest_rows':obs.get('observations',[])[:3]}
            except Exception as exc:probes[sid]={'error':error_label(exc)}
        del fred_key
        r.kv(bounded_original_probes=probes)
        rules=[]
        for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):
            for name in page.get('RuleNames',[]):
                rule=events.describe_rule(Name=name);targets=[]
                for batch in events.get_paginator('list_targets_by_rule').paginate(Rule=name):targets+=batch.get('Targets',[])
                bound=[t for t in targets if t['Arn'].split(':function:')[-1].split(':')[0]==FN]
                rules.append({'name':name,'state':rule['State'],'expression':rule.get('ScheduleExpression'),'bound_targets':len(bound)})
        r.kv(classic_schedule_bindings=rules,next_work='Preserve all 53 source identities, retain native dates/units and original bytes, replace synthetic basis and unqualified crisis scores, verify canonical OFR reconstruction and downstream consumers.')


if __name__=='__main__':
    try:main()
    except Exception:
        print('Crisis Plumbing preflight failed; inspect the committed report before retrying. No producer was invoked.')
        sys.exit(1)
