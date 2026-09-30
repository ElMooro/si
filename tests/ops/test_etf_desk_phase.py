"""Offline schedule migration safety tests; no AWS clients or producer invocations."""
import copy
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from datetime import datetime, timezone

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('phase',ROOT/'aws/ops/checks/etf_desk_phase.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class Scheduler:
    code='ResourceNotFoundException'
    def get_schedule(self,**kw):
        if self.code:raise Error(self.code)
        return {'Name':m.NAME}
class Events:
    def __init__(self):self.rule=copy.deepcopy(m.BEFORE_RULE);self.targets=copy.deepcopy(m.TARGETS);self.writes=[];self.upstream='cron(45 22 * * ? *)';self.ignore_write=False
    def describe_rule(self,Name):
        return copy.deepcopy(self.rule) if Name==m.NAME else {'ScheduleExpression':self.upstream,'State':'ENABLED'}
    def list_targets_by_rule(self,Rule):
        return {'Targets':copy.deepcopy(self.targets) if Rule==m.NAME else [{'Id':'1','Arn':'arn:aws:lambda:us-east-1:857687956942:function:justhodl-etf-constituents'}]}
    def put_rule(self,**kw):
        self.writes.append(kw)
        if not self.ignore_write:self.rule={**kw,'Arn':m.RULE_ARN,**({'CreatedBy':self.rule['CreatedBy']} if 'CreatedBy' in self.rule else {})}
class Lambda:
    timeout=840
    def get_function_configuration(self,FunctionName):return {'State':'Active','LastUpdateStatus':'Successful','Runtime':'python3.12','Timeout':900 if FunctionName==m.FN else self.timeout,'MemorySize':4096 if FunctionName==m.FN else 3072,'Environment':{'Variables':{'NEVER_REPORT':'secret'}}}
class S3:
    def __init__(self,doc):self.docs={m.MANIFEST:copy.deepcopy(doc)};self.versions={m.MANIFEST:1};self.writes=[];self.fail=None
    def get_object(self,Bucket,Key):
        if Key not in self.docs:raise Error('NoSuchKey')
        return {'Body':io.BytesIO(m.encoded(self.docs[Key])),'ETag':str(self.versions[Key])}
    def put_object(self,**kw):
        k=kw['Key']
        if k==self.fail:raise Error('AccessDenied')
        if kw.get('IfNoneMatch')=='*' and k in self.docs:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=str(self.versions[k]):raise Error('PreconditionFailed')
        self.writes.append(k);self.docs[k]=json.loads(kw['Body']);self.versions[k]=self.versions.get(k,0)+1
class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.doc={'version':1,'metadata':'preserve','rules':[{'name':'unrelated','expr':'leave','other':42}],'schedules':[{'name':'other-scheduler','timezone':'Europe/London'}]}
        self.events=Events();self.scheduler=Scheduler();self.lam=Lambda();self.s3=S3(self.doc);self.hour=20
        self.prepare()
    def prepare(self,rollback=False,before=None):
        config={'schedule':{'name':m.NAME,'expression':m.OLD if rollback else m.NEW,'description':m.OLD_DESCRIPTION if rollback else m.NEW_DESCRIPTION}}
        for name,value in [('aws/lambdas/'+m.FN+'/config.json',config),('config/schedule-manifest.json',m.replace_entry(self.doc,before if rollback else m.entry(m.NEW))),('aws/ops/audit/schedule-manifest.json',m.replace_entry(self.doc,before if rollback else m.entry(m.NEW)))]:
            p=self.root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(value))
    def run_op(self,rollback=False):return m.migrate(self.events,self.scheduler,self.lam,self.s3,root=self.root,rollback=rollback,clock=lambda:datetime(2026,9,30,self.hour,tzinfo=timezone.utc),revision='reviewed')
    def test_forward_and_idempotence_preserve_every_unrelated_entry_and_target(self):
        self.assertEqual(self.run_op()['status'],'migrated');self.assertEqual(self.events.rule,m.AFTER_RULE);self.assertEqual(self.events.targets,m.TARGETS)
        self.assertEqual(m.replace_entry(self.s3.docs[m.MANIFEST],None),self.doc);self.assertEqual(self.s3.writes,[m.ARCHIVE,m.MANIFEST])
        self.hour=23;self.assertEqual(self.run_op()['aws_configuration_writes'],0);self.assertEqual(len(self.events.writes),1)
    def test_read_only_creator_metadata_is_archived_but_never_sent_to_put_rule(self):
        self.events.rule['CreatedBy']='857687956942';self.run_op()
        self.assertEqual(self.s3.docs[m.ARCHIVE]['rule']['CreatedBy'],'857687956942')
        self.assertNotIn('CreatedBy',self.events.writes[0])
        self.prepare(rollback=True);self.run_op(True)
        self.assertEqual(self.events.rule['CreatedBy'],'857687956942')

    def test_complete_rollback_and_idempotence(self):
        self.run_op();self.prepare(rollback=True);self.assertEqual(self.run_op(True)['status'],'restored');self.assertEqual(self.events.rule,m.BEFORE_RULE);self.assertEqual(self.s3.docs[m.MANIFEST],self.doc);self.assertEqual(self.run_op(True)['aws_configuration_writes'],0)
    def test_restore_preexisting_manifest_entry(self):
        self.s3=S3(m.replace_entry(self.doc,m.entry(m.OLD)));self.run_op();self.prepare(rollback=True,before=m.entry(m.OLD));self.run_op(True);self.assertEqual(m.selected(self.s3.docs[m.MANIFEST]),m.entry(m.OLD))
    def test_scheduler_denial_no_classic_fallback(self):
        self.scheduler.code='AccessDeniedException'
        with self.assertRaisesRegex(ValueError,'scheduler_read_denied'):self.run_op()
        self.assertEqual(self.events.writes+self.s3.writes,[])
    def test_new_scheduler_refused(self):
        self.scheduler.code=None
        with self.assertRaisesRegex(ValueError,'unexpected_scheduler'):self.run_op()
    def test_added_target_settings_are_not_dropped(self):
        for key,value in [('RetryPolicy',{'MaximumRetryAttempts':1}),('DeadLetterConfig',{'Arn':'dlq'}),('Input','{}')]:
            self.events.targets=[{**m.TARGETS[0],key:value}]
            with self.assertRaisesRegex(ValueError,'target_binding_changed'):self.run_op()
        self.assertEqual(self.s3.writes,[])
    def test_unreviewed_rule_state_refused(self):
        self.events.rule['State']='DISABLED'
        with self.assertRaisesRegex(ValueError,'unreviewed_rule'):self.run_op()
    def test_changed_upstream_phase_or_runtime_refused(self):
        self.events.upstream='cron(0 23 * * ? *)'
        with self.assertRaisesRegex(ValueError,'upstream_phase'):self.run_op()
        self.events.upstream='cron(45 22 * * ? *)';self.lam.timeout=900
        with self.assertRaisesRegex(ValueError,'runtime_changed'):self.run_op()
    def test_late_window_prevents_extra_same_day_invocation(self):
        self.hour=22
        with self.assertRaisesRegex(ValueError,'outside_safe'):self.run_op()
        self.assertEqual(self.s3.writes,[])
    def test_repository_must_be_ready_before_archive_or_migration(self):
        self.prepare(rollback=True)
        with self.assertRaisesRegex(ValueError,'repository_phase'):self.run_op()
        self.assertEqual(self.s3.writes,[])
    def test_archive_failure_prevents_configuration_writes(self):
        self.s3.fail=m.ARCHIVE
        with self.assertRaisesRegex(ValueError,'archive_write_failed'):self.run_op()
        self.assertEqual(self.events.writes+self.s3.writes,[])
    def test_manifest_failure_preserves_rule_and_retry_resumes(self):
        self.s3.fail=m.MANIFEST
        with self.assertRaisesRegex(ValueError,'manifest_compare'):self.run_op()
        self.assertEqual(self.events.writes,[]);self.assertIn(m.ARCHIVE,self.s3.docs)
        self.s3.fail=None;self.assertEqual(self.run_op()['status'],'migrated')
    def test_interrupted_after_manifest_can_resume(self):
        self.events.ignore_write=True
        with self.assertRaisesRegex(ValueError,'migration_readback'):self.run_op()
        self.assertEqual(m.selected(self.s3.docs[m.MANIFEST]),m.entry(m.NEW));self.events.ignore_write=False;self.assertEqual(self.run_op()['status'],'migrated')
    def test_existing_new_rule_without_reversal_refused(self):
        self.events.rule=copy.deepcopy(m.AFTER_RULE)
        with self.assertRaisesRegex(ValueError,'reversal_record_required'):self.run_op()
    def test_manifest_duplicate_or_unreviewed_entry_refused(self):
        self.s3.docs[m.MANIFEST]['rules'] += [m.entry(m.OLD),m.entry(m.OLD)]
        with self.assertRaisesRegex(ValueError,'ambiguous_manifest'):self.run_op()
    def test_unknown_reversal_record_refused(self):
        self.s3.docs[m.ARCHIVE]={'contract':'unknown'};self.s3.versions[m.ARCHIVE]=1
        with self.assertRaisesRegex(ValueError,'reversal_record_differs'):self.run_op()
    def test_concurrent_rule_change_refuses_before_configuration_write(self):
        original=self.events.describe_rule; calls=0
        def read(Name):
            nonlocal calls
            if Name==m.NAME:
                calls+=1
                if calls==2:self.events.rule['Description']='another lane'
            return original(Name)
        self.events.describe_rule=read
        with self.assertRaisesRegex(ValueError,'unreviewed_rule'):self.run_op()
        self.assertEqual(self.events.writes,[]);self.assertEqual(self.s3.writes,[m.ARCHIVE])
    def test_concurrent_manifest_change_refuses_before_configuration_write(self):
        original=self.s3.get_object; calls=0
        def read(**kw):
            nonlocal calls
            if kw['Key']==m.MANIFEST:
                calls+=1
                if calls==2:self.s3.docs[m.MANIFEST]['metadata']='another lane'
            return original(**kw)
        self.s3.get_object=read
        with self.assertRaisesRegex(ValueError,'manifest_changed_before_write'):self.run_op()
        self.assertEqual(self.events.writes,[]);self.assertEqual(self.s3.writes,[m.ARCHIVE])

    def test_repository_phase_follows_upstream_runtime_bound(self):
        desk=json.loads((ROOT/'aws/lambdas/justhodl-etf-global-desk/config.json').read_text())
        upstream=json.loads((ROOT/'aws/lambdas/justhodl-etf-constituents/config.json').read_text())
        def minutes(expr):
            fields=expr.removeprefix('cron(').removesuffix(')').split()
            self.assertEqual(fields[2:],['*','*','?','*'])
            return int(fields[1])*60+int(fields[0])
        self.assertGreaterEqual(minutes(desk['schedule']['expression'])-minutes(upstream['schedule']['expression'])-upstream['timeout']/60,6)

    def test_guard_detects_original_dependency_order_regression(self):
        # Old 22:20 desk phase is before the 22:45 + 14-minute producer bound.
        old=22*60+20;new=23*60+5;bound=22*60+45+840/60
        self.assertLess(old,bound);self.assertGreaterEqual(new-bound,6)

if __name__=='__main__':unittest.main()
