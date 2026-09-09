"""Observe a pinned release using only allowlisted AWS reads; never invoke engines."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
import audit_20260909_release as release

READ_METHODS={
 'lambda':frozenset(('get_function','get_alias','get_function_url_config')),
 's3':frozenset(('get_object',)),
 'events':frozenset(('describe_rule','list_targets_by_rule','list_rules')),
 'scheduler':frozenset(('get_schedule','list_schedules')),
}
class ReadOnlyClient:
    def __init__(self,client,service):self._client=client;self._allowed=READ_METHODS[service]
    def __getattr__(self,name):
        if name not in self._allowed:raise RuntimeError('unreviewed_aws_operation')
        return getattr(self._client,name)

def pages(method, field, **kwargs):
    seen=set();token=None
    while True:
        response=method(**kwargs,**({'NextToken':token} if token else {}))
        yield from response.get(field,[])
        token=response.get('NextToken')
        if not token:return
        if token in seen:raise RuntimeError('repeated_metadata_page')
        seen.add(token)

def schedule_discovery(clients,functions):
    result={name:[] for name in functions}
    def owner(arn):return arn.split(':function:')[-1].split(':')[0] if ':function:' in arn else None
    for row in pages(clients['scheduler'].list_schedules,'Schedules'):
        name=owner(row.get('Target',{}).get('Arn',''))
        if name not in result:continue
        current=clients['scheduler'].get_schedule(Name=row['Name'],GroupName=row.get('GroupName','default'))
        result[name].append({'service':'scheduler','name':row['Name'],'group':row.get('GroupName','default'),
                            'target_arn':current.get('Target',{}).get('Arn'),'state':current.get('State'),
                            'expression':current.get('ScheduleExpression'),
                            'input_empty_object':release.input_matches(current.get('Target',{}).get('Input'),{})})
    def inspect_rule(row):
        matches=[]
        for target in pages(clients['events'].list_targets_by_rule,'Targets',Rule=row['Name']):
            name=owner(target.get('Arn',''))
            if name in result:matches.append((name,{'service':'events','name':row['Name'],'target_arn':target['Arn'],
                                'state':row.get('State'),'expression':row.get('ScheduleExpression'),
                                'input_empty_object':release.input_matches(target.get('Input'),{})}))
        return matches
    rules=list(pages(clients['events'].list_rules,'Rules'))
    with ThreadPoolExecutor(max_workers=6) as pool:
        for matches in pool.map(inspect_rule,rules):
            for name,proof in matches:result[name].append(proof)
    return {'scope':'All Scheduler groups and default EventBridge bus; matches include base, numeric version and alias targets. Input bodies withheld.','functions':result}

def publication_diagnostics(clients,report):
    result=[]
    for name,rows in report['outputs'].items():
        for row in rows:
            if row['status']!='PENDING_OUTPUT' and name not in ('justhodl-ka-metrics','justhodl-khalid-metrics'):continue
            item={'function':name,'key':row['key'],'generation_candidates':{}}
            try:
                response=clients['s3'].get_object(Bucket=release.BUCKET,Key=row['key']);doc=release.strict_document(response['Body'].read())
                for field in ('generated_at','generated','computed_at','updated_at','as_of','asof','timestamp','updated','utc','ts'):
                    if field not in doc:continue
                    stamp=release.parse_timestamp(doc[field]);item['generation_candidates'][field]={'valid_clock':stamp is not None,'timestamp':stamp.isoformat() if stamp else None}
                if doc.get('error') in ('metrics_unavailable','analysis_provider_unavailable','analysis_unavailable'):item['fixed_error_code']=doc['error']
            except Exception as exc:item.update(error_type=type(exc).__name__,error_code=release.safe_label(getattr(exc,'response',{}).get('Error',{}).get('Code')))
            result.append(item)
    return result

def observe(root,clients,privacy):
    scope=release.changed_scope(root);artifacts=release.artifact_map(root,scope)
    report={'ops':5283,'read_only':True,'aws_mutations':0,'private_payloads_reported':0,
            'expected_release_sha':release.git(root,'rev-parse','HEAD'),'engine_scope':scope,
            'artifact_scope':artifacts,'started_at':release.utcnow().isoformat()}
    with ThreadPoolExecutor(max_workers=6) as pool:
        code=list(pool.map(lambda name:release.check_packages(clients['lambda'],root,[name])[0],scope))
    report['code']={row['function']:row for row in code};report['source_parity_verified']=all(row.get('pass') for row in code)
    report['privacy_prerequisite']=release.privacy_receipt_summary(root,privacy)
    if not report['source_parity_verified']:
        report.update(ok=False,status='SOURCE_PARITY_FAILED');return report
    def outputs(name):
        return name,[release.inspect_output(clients['s3'],name,key,report['code'][name],root=root) for key in artifacts[name]['primary_keys']]
    with ThreadPoolExecutor(max_workers=6) as pool:report['outputs']=dict(pool.map(outputs,scope))
    report['schedules']=release.observe_schedules(clients,root,scope)
    report['function_urls']=release.observe_function_urls(clients['lambda'],root)
    missing={row['function'] for row in report['schedules'] if row['status']=='PENDING_CONFIGURATION'}
    report['schedule_discovery']=schedule_discovery(clients,missing) if missing else {'functions':{}}
    report['publication_diagnostics']=publication_diagnostics(clients,report)
    pending=[];failures=[];blocked=[]
    for name,rows in report['outputs'].items():
        if not rows:pending.append({'function':name,'reason':'API_REQUEST_FIXTURE_OR_PRIMARY_OUTPUT_MAPPING_REQUIRED'})
        for row in rows:
            if row['status']=='PENDING_OUTPUT':pending.append({'function':name,'key':row['key'],'requirements':row['requirements']})
            elif row['status']=='CONTRACT_FAILED':failures.append({'function':name,'key':row['key'],'errors':row['errors'],'error_type':row.get('error_type')})
            elif row['requirements']:blocked.append({'function':name,'key':row['key'],'requirements':row['requirements']})
    pending.extend({'function':row['function'],'reason':'SCHEDULE_CONFIGURATION_PENDING','schedule':row['name']} for row in report['schedules'] if row['status'] not in ('VERIFIED','OBSERVED_CADENCE_ONLY'))
    pending.extend({'function':row['function'],'reason':'FUNCTION_URL_IDENTITY_UNPROVEN'} for row in report['function_urls'] if row['status']!='VERIFIED')
    report.update(pending_outputs=pending,contract_failures=failures,blocked_requirements=blocked,
                  ok=not pending and not failures and report['privacy_prerequisite']['verified'],
                  status='CONTRACT_FAILURE' if failures else 'PENDING_OUTPUT_OR_REQUEST_PROOF' if pending else 'OBSERVED_WITH_REQUIREMENTS' if blocked else 'OBSERVED')
    return report

def main():
    if os.environ.get('GITHUB_ACTIONS')!='true':raise RuntimeError('runner_required')
    anchor=os.environ['AUDIT_RELEASE_SHA']
    if not re.fullmatch('[a-f0-9]{40}',anchor):raise ValueError('explicit_source_sha_required')
    report={'ops':5283,'ok':False,'read_only':True,'status':'INITIALIZATION_FAILED','aws_mutations':0,'private_payloads_reported':0}
    def git(*args):return release.git(ROOT,*args)
    try:
        git('fetch','--no-tags','origin',anchor)
        git('cat-file','-e',release.BASE+'^{commit}')
        with tempfile.TemporaryDirectory(prefix='audit-readonly-') as temp:
            root=Path(temp)/'source';git('worktree','add','--detach',str(root),anchor)
            try:
                import boto3
                from botocore.config import Config
                clients={name:ReadOnlyClient(boto3.client(name,region_name=release.REGION,config=Config(connect_timeout=10,read_timeout=45,retries={'max_attempts':2})),name) for name in READ_METHODS}
                privacy=json.loads((ROOT/'aws/ops/reports/5230_audit_privacy_migration.json').read_text())
                report.update(observe(root,clients,privacy))
            finally:git('worktree','remove','--force',str(root))
    except Exception as exc:report.update(ok=False,status='OBSERVATION_FAILED',error_type=type(exc).__name__)
    report.update(checker_checkout_sha=git('rev-parse','HEAD'),finished_at=release.utcnow().isoformat(),workflow_run_id=os.environ.get('GITHUB_RUN_ID'))
    path=ROOT/'aws/ops/reports/5283_release_observation.json';path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
    print(json.dumps({k:report.get(k) for k in ('ops','ok','status','source_parity_verified','expected_release_sha','aws_mutations')}))
    return 0 if report['ok'] else 1
if __name__=='__main__':raise SystemExit(main())
