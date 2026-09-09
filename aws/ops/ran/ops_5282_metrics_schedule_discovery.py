"""Discover actual metric scheduling by target identity, without changing schedules or invoking functions."""
import json,os,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
FUNCTIONS=('justhodl-ka-metrics','justhodl-khalid-metrics')

def main():
    if os.environ.get('GITHUB_ACTIONS')!='true':raise RuntimeError('runner_only')
    import boto3
    clients={name:boto3.client(name,region_name='us-east-1') for name in ('lambda','scheduler','events')}
    report={'ops':5282,'read_only':True,'input_bodies_reported':0,'source_sha':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'functions':{name:[] for name in FUNCTIONS}}
    for page in clients['scheduler'].get_paginator('list_schedules').paginate():
        for summary in page.get('Schedules',[]):
            arn=summary.get('Target',{}).get('Arn','');function=arn.split(':function:')[-1].split(':')[0]
            if function not in FUNCTIONS:continue
            row=clients['scheduler'].get_schedule(Name=summary['Name'],GroupName=summary['GroupName'])
            target=row.get('Target',{});input_doc=json.loads(target.get('Input') or '{}')
            report['functions'][function].append({'service':'scheduler','name':row['Name'],'group':row['GroupName'],'expression':row['ScheduleExpression'],'timezone':row.get('ScheduleExpressionTimezone'),'state':row['State'],'target_arn':target['Arn'],'role_arn':target.get('RoleArn'),'input_is_empty_object':input_doc=={},'input_has_http_envelope':isinstance(input_doc,dict) and any(key in input_doc for key in ('requestContext','httpMethod','headers'))})
    for function in FUNCTIONS:
        base='arn:aws:lambda:us-east-1:857687956942:function:'+function;targets=[base]
        for page in clients['lambda'].get_paginator('list_aliases').paginate(FunctionName=function):targets.extend(base+':'+a['Name'] for a in page.get('Aliases',[]))
        names=set()
        for arn in targets:
            for page in clients['events'].get_paginator('list_rule_names_by_target').paginate(TargetArn=arn):names.update(page.get('RuleNames',[]))
        for name in sorted(names):
            rule=clients['events'].describe_rule(Name=name)
            report['functions'][function].append({'service':'events','name':name,'expression':rule.get('ScheduleExpression'),'state':rule['State']})
    (ROOT/'aws/ops/reports/5282_metrics_schedules.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'ops':5282,'bindings':{name:len(rows) for name,rows in report['functions'].items()}}));return 0
if __name__=='__main__':raise SystemExit(main())
