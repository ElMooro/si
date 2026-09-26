"""Verify the deployed ICI repair, then restore only its existing weekly rule.

No producer invocation, new source request, output seeding, schedule acceleration,
account read, notification or history mutation. Normal publication stays pending.
"""
from pathlib import Path
from datetime import datetime,timezone
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/lambdas/justhodl-ici-flows/source')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6165_ici_original_baseline import retain,PRIVATE,BUCKET,FUNCTION
import retained_access_evidence as access
import ici_store as store

BASELINE={'key':PRIVATE+'16f1ae5e41fc156ed4c09c61e2acb5e170c3ebaa2d615103c0bbbe031c0755c2.bin',
          'sha256':'16f1ae5e41fc156ed4c09c61e2acb5e170c3ebaa2d615103c0bbbe031c0755c2','bytes':7691}
RULE='justhodl-ici-flows-weekly'
CRON='cron(30 16 ? * WED,THU *)'


def rule_state(events):
    value=events.describe_rule(Name=RULE)
    return {key:value.get(key) for key in ('Name','Arn','State','ScheduleExpression','EventPattern','RoleArn','Description')}


def restore_weekly(events,before,targets,accepted):
    if accepted is not True or before.get('State')!='DISABLED' or before.get('Name')!=RULE or before.get('ScheduleExpression')!=CRON:
        raise ValueError('Exact accepted disabled weekly rule required')
    if rule_state(events)!=before or events.list_targets_by_rule(Rule=RULE)['Targets']!=targets:raise ValueError('Rule changed before cadence restoration')
    events.enable_rule(Name=RULE)
    after=rule_state(events)
    if after!={**before,'State':'ENABLED'} or events.list_targets_by_rule(Rule=RULE)['Targets']!=targets:
        raise ValueError('Cadence restoration changed more than the existing state')
    return after


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')],cwd=ROOT,check=True)
    subprocess.run([sys.executable,str(ROOT/'tests/test_ici_cadence_acceptance.py')],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    with report('ops_6167_ici_native_acceptance') as r:
        read=store.reader(s3,BUCKET);baseline=store.strict(store.checked(BASELINE,read,private=True))
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FUNCTION+'/source'],cwd=ROOT,text=True).strip()
        before=runtime(lam,s3,events,scheduler,FUNCTION)
        if before['receipt']!={'status':'matched','commit':expected}:raise ValueError('Exact deployed source receipt required')
        if before['schedules']!=baseline['predecessor']['schedules'] or before['memory_mb']!=256 or before['timeout']!=120:
            raise ValueError('Preserved native schedule/runtime differs')
        rule=rule_state(events);targets=events.list_targets_by_rule(Rule=RULE)['Targets']
        if rule['State']!='DISABLED' or rule['ScheduleExpression']!=CRON or len(targets)!=1 or not targets[0]['Arn'].endswith(':function:'+FUNCTION):
            raise ValueError('One original disabled native target required')
        predecessors={}
        for name,key in (('legacy_mmf','data/history/ici-mmf.json'),('legacy_flows','data/history/ici-flows.json')):
            ref=baseline['captures'][key]['original'];original=store.checked(ref,read,private=True)
            if bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])!=original:raise ValueError('Legacy history changed')
            predecessors[name]=ref
        inputs={'contract':'ici-inputs.v1','generated_at':baseline['finished_at'],'predecessors':predecessors,'sources':{}}
        for name,entry in baseline['sources'].items():
            inputs['sources'][name]={'url':entry['url'],'http_status':entry['http_status'],'acquired_at':entry['received_at'],'original':entry['original']}
        output,proof=store.compile_inputs(inputs,read)
        evidence={'contract':'ici-native-acceptance.v1','commit':expected,'checked_at':datetime.now(timezone.utc).isoformat(),
            'actual_runtime':before,'source_proof':proof,'original_baseline':BASELINE,
            'native_publication_verified':False,'provider_requests':0,'native_invocations':0,
            'compilers':{name:store.sha(path.read_bytes()) for name,path in store.compilers().items()}}
        ref=retain(s3,store.encode(evidence));privacy=access.summarize([access.check(ref['key']),access.check(BASELINE['key'])])
        if not privacy['all_denied']:raise ValueError('Protected original evidence must remain denied')
        if runtime(lam,s3,events,scheduler,FUNCTION)!=before:raise ValueError('Runtime changed during acceptance')
        head_before,_=store.head(s3,BUCKET)
        if head_before is not None:raise ValueError('Unexpected public ICI head before dormant repair acceptance')
        # The old rule was disabled after broad source discovery failed. Exact
        # official URLs now work on AWS and both original releases reconcile.
        # Restore that same weekly cadence; never invoke to manufacture proof.
        after=restore_weekly(events,rule,targets,accepted=True)
        r.kv(expected_commit=expected,actual_runtime=before,accepted_original_checks=proof,acceptance_evidence=ref,**privacy,
            schedule_before=rule,schedule_after=after,normal_publication_verified=False,
            next_normal_run='2026-09-30T16:30:00Z',provider_requests=0,native_invocations=0,public_writes=0,
            history_writes=0,account_reads=0,notifications_sent=0,existing_rules_enabled=1,new_rules=0,
            scope='Actual deployed bytes and complete retained official releases accepted. Existing Wed/Thu 16:30 UTC rule restored unchanged; first normal publication pending.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
