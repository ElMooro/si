"""Read-only metric cadence/CORS metadata; never run paid analysis or report API payloads."""
import json
import os
from pathlib import Path
import subprocess
ROOT=Path(__file__).resolve().parents[3]

def main():
    if os.environ.get('GITHUB_ACTIONS')!='true':raise RuntimeError('runner_only')
    import boto3
    events=boto3.client('events',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    report={'ops':5281,'read_only':True,'source_sha':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'functions':{},'rules':{},'payloads_reported':0}
    for function in ('justhodl-ka-metrics','justhodl-khalid-metrics'):
        try:
            c=lam.get_function_url_config(FunctionName=function)
            report['functions'][function]={k:c.get(k) for k in ('FunctionUrl','FunctionArn','AuthType','Cors')}
        except Exception as exc:report['functions'][function]={'error_type':type(exc).__name__}
    for name in ('justhodl-ka-metrics-refresh','justhodl-khalid-metrics-refresh'):
        try:
            rule=events.describe_rule(Name=name);targets=[];token=None
            while True:
                page=events.list_targets_by_rule(Rule=name,**({'NextToken':token} if token else {}))
                targets.extend({k:t.get(k) for k in ('Id','Arn','RoleArn')} for t in page.get('Targets',[]))
                next_token=page.get('NextToken')
                if not next_token:break
                if next_token==token:raise ValueError('repeated_page')
                token=next_token
            report['rules'][name]={**{k:rule.get(k) for k in ('Arn','ScheduleExpression','State','EventBusName')},'targets':targets}
        except Exception as exc:report['rules'][name]={'error_type':type(exc).__name__}
    (ROOT/'aws/ops/reports/5281_metrics_configuration.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'ops':5281,'read_only':True,'functions':len(report['functions']),'rules':len(report['rules'])}))
    return 0
if __name__=='__main__':raise SystemExit(main())
