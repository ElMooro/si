"""Restore only the unintended Scarcity rule added by deployment 390047d7e.
Preserve the original Scheduler, archive rollback evidence, disable the extra
rule and detach its sole deployer-created target. Never invoke any producer.
"""
from pathlib import Path
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_6232_sec_search_research_acceptance import check_runtime
from ops_6244_scarcity_radar_original_baseline import retain
import retained_access_evidence as access
FN='justhodl-scarcity-radar';RULE='justhodl-scarcity-radar-daily';ID='audit-'+FN
ARN='arn:aws:lambda:us-east-1:857687956942:function:'+FN
RULE_ARN='arn:aws:events:us-east-1:857687956942:rule/'+RULE
SID='EventBridge-'+RULE


def plan(actual,baseline,rule,targets,permission):
    original=baseline['schedules'];extra=[s for s in actual['schedules'] if s.get('name')==RULE and s.get('kind')=='EventBridge rule']
    rest=[s for s in actual['schedules'] if s not in extra]
    if rest!=original or len(extra)!=1:raise ValueError('Only the proven extra rule may be detached')
    expected={'kind':'EventBridge rule','name':RULE,'state':extra[0]['state'],'expression':'cron(45 22 ? * MON-FRI *)','native_targets':1}
    if extra[0]!=expected or expected['state'] not in ('ENABLED','DISABLED'):raise ValueError('Unreviewed duplicate schedule')
    if rule.get('Arn')!=RULE_ARN or rule.get('Name')!=RULE or rule.get('ScheduleExpression')!=expected['expression'] or rule.get('EventPattern') or rule.get('EventBusName','default')!='default':raise ValueError('Rule identity changed')
    if targets!=[{'Id':ID,'Arn':ARN}]:raise ValueError('Extra target has non-deployer state; preserve it')
    if permission is not None:
        expected_permission={'Sid':SID,'Effect':'Allow','Principal':{'Service':'events.amazonaws.com'},'Action':'lambda:InvokeFunction','Resource':ARN,'Condition':{'ArnLike':{'AWS:SourceArn':RULE_ARN}}}
        if permission!=expected_permission:raise ValueError('Invoke permission is not the extra rule grant')
    return {'rule':rule,'targets':targets,'permission':permission,'before':actual,'original_schedules':original}


def restore(events,lam,evidence):
    # Recheck immediately before mutation; never touch the original Scheduler.
    rule=events.describe_rule(Name=RULE);targets=events.list_targets_by_rule(Rule=RULE)['Targets']
    stable=lambda value:{k:v for k,v in value.items() if k!='ResponseMetadata'}
    if stable(rule)!=stable(evidence['rule']) or targets!=evidence['targets']:raise ValueError('Schedule changed after archival')
    events.disable_rule(Name=RULE)
    if evidence['permission'] is not None:lam.remove_permission(FunctionName=FN,StatementId=SID,RevisionId=evidence['permission_revision'])
    result=events.remove_targets(Rule=RULE,Ids=[ID])
    if result.get('FailedEntryCount')!=0:raise ValueError('Could not detach extra target')
    if events.describe_rule(Name=RULE)['State']!='DISABLED' or events.list_targets_by_rule(Rule=RULE)['Targets']:raise ValueError('Extra rule must be disabled and empty')


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/ops/test_scarcity_schedule_restore.py')],cwd=ROOT,check=True)
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')};args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    baseline=json.loads((ROOT/'docs/audit/2026-09-27/scarcity-radar-original-baseline.json').read_bytes())['actual_producers'][FN]
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    with report('ops_6246_restore_scarcity_original_schedule') as r:
        before=runtime(*args,FN);r.kv(actual_before=before,original_schedules=baseline['schedules'])
        if before['schedules']==baseline['schedules']:
            check_runtime(before,baseline,expected,2);r.kv(status='original_schedule_already_restored',native_invocations=0);return
        checked=dict(before);checked['schedules']=baseline['schedules'];check_runtime(checked,baseline,expected,2)
        events=clients['events'];lam=clients['lambda']
        rule=events.describe_rule(Name=RULE);targets=events.list_targets_by_rule(Rule=RULE)['Targets']
        policy=lam.get_policy(FunctionName=FN);statements=json.loads(policy['Policy'])['Statement'];selected=[s for s in statements if s.get('Sid')==SID]
        if len(selected)>1:raise ValueError('Ambiguous rule permission')
        evidence=plan(before,baseline,rule,targets,selected[0] if selected else None)
        if selected and not policy.get('RevisionId'):raise ValueError('Policy revision is required before removing the extra grant')
        evidence['permission_revision']=policy.get('RevisionId')
        original=retain(clients['s3'],json.dumps(evidence,sort_keys=True,separators=(',',':')).encode())
        privacy=access.summarize([access.check(original['key'])])
        if not privacy['all_denied']:raise ValueError('Rollback original must remain protected')
        restore(events,lam,evidence)
        after=runtime(*args,FN);check_runtime(after,baseline,expected,2)
        r.kv(status='original_schedule_restored',expected_commit=expected,actual_after=after,rollback_original=original,privacy=privacy,
             extra_rule_disabled=True,extra_target_detached=True,original_scheduler_changed=False,native_invocations=0,
             provider_requests=0,public_writes=0,consumer_output_reads=0,learning_log_reads=0,notifications_sent=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
