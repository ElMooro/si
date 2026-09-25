"""Exact deployed package and schedule evidence, including governed live aliases.

Only read operations; environment variables and signed package URLs are never
returned. A qualified schedule is accepted only after verifying its actual code.
"""
from pathlib import Path
import base64,hashlib,io,json,subprocess,urllib.request,zipfile
from release_package_evidence import shared_imports
ROOT=Path(__file__).resolve().parents[3]
BUCKET='justhodl-dashboard-live'

def bounded(stream,limit=64*1024*1024):
    try:raw=stream.read(limit+1)
    finally:stream.close()
    if len(raw)>limit:raise ValueError('Whole package byte bound exceeded')
    return raw

def verified_alias(lam,function,configuration,conf):
    if not conf.get('release_validation'):return None
    alias=lam.get_alias(FunctionName=function,Name='live');version=alias.get('FunctionVersion')
    if not isinstance(version,str) or not version.isdigit() or int(version)<1 or (alias.get('RoutingConfig') or {}).get('AdditionalVersionWeights'):
        raise ValueError('One fully promoted numbered live version required')
    live=lam.get_function_configuration(FunctionName=function,Qualifier='live')
    if (live.get('Version')!=version or live.get('CodeSha256')!=configuration['CodeSha256'] or live.get('State')!='Active'
            or live.get('LastUpdateStatus')!='Successful'):
        raise ValueError('Active alias package or state differs from verified latest code')
    for key in ('Runtime','Handler','Timeout','MemorySize','Architectures','Role','EphemeralStorage'):
        if live.get(key)!=configuration.get(key):raise ValueError('Active alias runtime differs: '+key)
    return {'alias':'live','version':version,'code_sha256':live['CodeSha256']}

def schedule_evidence(events,scheduler,function,arn,conf,alias):
    allowed={arn}
    if alias:allowed.add(arn+':live')
    rows=[];seen_rules=set();seen_schedules=set()
    for target in sorted(allowed):
        for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=target):
            for name in page['RuleNames']:
                if name in seen_rules:continue
                seen_rules.add(name);rule=events.describe_rule(Name=name);targets=events.list_targets_by_rule(Rule=name)['Targets']
                native=[t['Arn'] for t in targets if t.get('Arn') in allowed]
                rows.append({'kind':'EventBridge rule','name':name,'state':rule['State'],'expression':rule.get('ScheduleExpression'),
                    'native_targets':len(native),**({'qualified_targets':sorted(v for v in native if v!=arn)} if alias else {})})
    declared=conf.get('eventbridge_scheduler') or {}
    def append(actual,declared_primary=False):
        key=(actual.get('GroupName','default'),actual['Name'])
        if key in seen_schedules:return
        target=actual['Target']['Arn']
        if target not in allowed:raise ValueError('Schedule targets an unverified function/version: '+actual['Name'])
        if declared_primary and alias and target!=arn+':live':raise ValueError('Governed primary schedule must target verified live alias')
        seen_schedules.add(key)
        value={'kind':'EventBridge Scheduler','name':actual['Name'],'state':actual['State'],
            'expression':actual['ScheduleExpression'],'timezone':actual['ScheduleExpressionTimezone'],'native_targets':1}
        if not declared_primary:value['group']=key[0]
        if alias:value['target_qualifier']='live' if target.endswith(':live') else '$LATEST'
        rows.append(value)
    if declared.get('schedule_name'):append(scheduler.get_schedule(Name=declared['schedule_name']),True)
    for page in scheduler.get_paginator('list_schedules').paginate(NamePrefix=function):
        for item in page.get('Schedules',[]):
            target=item.get('Target',{}).get('Arn','')
            if target!=arn and not target.startswith(arn+':'):continue
            append(scheduler.get_schedule(Name=item['Name'],GroupName=item['GroupName']))
    return rows

def runtime(lam,s3,events,scheduler,function):
    source=ROOT/'aws/lambdas'/function/'source';deployed=lam.get_function(FunctionName=function);cfg=deployed['Configuration']
    if cfg['State']!='Active' or cfg['LastUpdateStatus']!='Successful':raise ValueError('Function is not ready')
    raw=bounded(urllib.request.urlopen(deployed['Code']['Location'],timeout=40))
    if base64.b64encode(hashlib.sha256(raw).digest()).decode()!=cfg['CodeSha256']:raise ValueError('Actual ZIP hash differs')
    paths=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
    expected={p.relative_to(source).as_posix():p for p in paths}
    expected.update({p.name:p for p in shared_imports(ROOT,paths) if not (source/p.name).exists()})
    if not expected:raise ValueError('Tracked source is required')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        for name,path in expected.items():
            if archive.read(name)!=path.read_bytes():raise ValueError('Actual packaged source differs: '+name)
    try:
        receipt=json.loads(bounded(s3.get_object(Bucket=BUCKET,Key='data/ops/releases/'+function+'.json')['Body']))
        if receipt['code_sha256']!=cfg['CodeSha256']:raise ValueError('Receipt code hash differs')
        receipt_status={'status':'matched','commit':receipt['commit']}
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('NoSuchKey','404'):raise
        receipt_status={'status':'missing_predecessor_receipt'}
    path=source.parent/'config.json';conf=json.loads(path.read_bytes()) if path.exists() else {}
    alias=verified_alias(lam,function,cfg,conf)
    result={'code_sha256':cfg['CodeSha256'],'source_files_checked':len(expected),'handler_bytes':len((source/'lambda_function.py').read_bytes()),
        'timeout':cfg['Timeout'],'memory_mb':cfg['MemorySize'],'receipt':receipt_status,
        'schedules':schedule_evidence(events,scheduler,function,cfg['FunctionArn'],conf,alias),
        'function_name':cfg['FunctionName'],'runtime':cfg['Runtime'],'handler':cfg['Handler'],
        'architectures':cfg['Architectures'],'role':cfg['Role'],'ephemeral_storage_mb':cfg['EphemeralStorage']['Size']}
    if alias:result['active_alias']=alias
    return result
