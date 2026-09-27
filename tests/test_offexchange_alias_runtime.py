from pathlib import Path
from unittest.mock import Mock
from copy import deepcopy
import sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/ops/checks'))
import market_runtime_evidence as op
ARN='arn:aws:lambda:us-east-1:123456789012:function:justhodl-test'
class Tests(unittest.TestCase):
    def config(self):return {'State':'Active','LastUpdateStatus':'Successful','CodeSha256':'exact','Runtime':'python3.12','Handler':'lambda_function.lambda_handler','Timeout':300,'MemorySize':1024,'Architectures':['x86_64'],'Role':'role','EphemeralStorage':{'Size':512}}
    def clients(self,target):
        events=Mock();events.get_paginator.return_value.paginate.return_value=[{'RuleNames':[]}]
        scheduler=Mock();scheduler.get_paginator.return_value.paginate.return_value=[{'Schedules':[]}]
        scheduler.get_schedule.return_value={'Name':'daily','Target':{'Arn':target},'State':'ENABLED','ScheduleExpression':'rate(1 day)','ScheduleExpressionTimezone':'UTC'}
        return events,scheduler
    def test_live_alias_requires_exact_numbered_unweighted_code_and_runtime(self):
        lam=Mock();lam.get_alias.return_value={'FunctionVersion':'12'};lam.get_function_configuration.return_value={**self.config(),'Version':'12'}
        self.assertEqual(op.verified_alias(lam,'justhodl-test',self.config(),{'release_validation':True})['version'],'12')
        for changes in ({'CodeSha256':'other'},{'Version':'11'},{'MemorySize':512},{'LastUpdateStatus':'Failed'}):
            lam.get_function_configuration.return_value={**self.config(),'Version':'12',**changes}
            with self.assertRaises(ValueError):op.verified_alias(lam,'justhodl-test',self.config(),{'release_validation':True})
        lam.get_alias.return_value={'FunctionVersion':'12','RoutingConfig':{'AdditionalVersionWeights':{'11':0.1}}}
        with self.assertRaises(ValueError):op.verified_alias(lam,'justhodl-test',self.config(),{'release_validation':True})
    def test_declared_alias_schedule_records_real_target_without_rewriting_it(self):
        events,scheduler=self.clients(ARN+':live');before=deepcopy(scheduler.get_schedule.return_value)
        rows=op.schedule_evidence(events,scheduler,'justhodl-test',ARN,{'eventbridge_scheduler':{'schedule_name':'daily'}},{'alias':'live'})
        self.assertEqual(rows[0]['target_qualifier'],'live');self.assertEqual(scheduler.get_schedule.return_value,before)
    def test_unknown_alias_and_wrong_unqualified_primary_are_rejected(self):
        for target in (ARN,ARN+':candidate','other'):
            events,scheduler=self.clients(target)
            with self.assertRaises(ValueError):op.schedule_evidence(events,scheduler,'justhodl-test',ARN,{'eventbridge_scheduler':{'schedule_name':'daily'}},{'alias':'live'})
    def test_unqualified_producer_schedule_preserves_baseline_shape(self):
        events,scheduler=self.clients(ARN)
        rows=op.schedule_evidence(events,scheduler,'justhodl-test',ARN,{'eventbridge_scheduler':{'schedule_name':'daily'}},None)
        self.assertNotIn('target_qualifier',rows[0]);self.assertEqual(rows[0]['native_targets'],1)

    def test_complete_paginated_scan_finds_unrelated_names_and_groups_without_reading_other_payloads(self):
        events,scheduler=self.clients(ARN)
        scheduler.get_paginator.return_value.paginate.return_value=[
            {'Schedules':[{'Name':'unrelated','GroupName':'default','Target':{'Arn':'another-function'}}]},
            {'Schedules':[{'Name':'macro-cycle-refresh','GroupName':'research','Target':{'Arn':ARN}}]}]
        scheduler.get_schedule.return_value.update(Name='macro-cycle-refresh',GroupName='research')
        scheduler.get_schedule.return_value['Target']['Input']='private fixture payload'
        rows=op.schedule_evidence(events,scheduler,'justhodl-test',ARN,{},None)
        self.assertEqual([(r['name'],r['group']) for r in rows],[('macro-cycle-refresh','research')])
        scheduler.get_paginator.return_value.paginate.assert_called_once_with()
        scheduler.get_schedule.assert_called_once_with(Name='macro-cycle-refresh',GroupName='research')
        self.assertNotIn('private fixture payload',str(rows));scheduler.update_schedule.assert_not_called()

    def test_declared_nondefault_group_is_read_exactly_and_deduplicated_with_inventory(self):
        events,scheduler=self.clients(ARN)
        scheduler.get_schedule.return_value['GroupName']='research'
        scheduler.get_paginator.return_value.paginate.return_value=[{'Schedules':[{'Name':'daily','GroupName':'research','Target':{'Arn':ARN}}]}]
        rows=op.schedule_evidence(events,scheduler,'justhodl-test',ARN,{'eventbridge_scheduler':{'schedule_name':'daily','group_name':'research'}},None)
        self.assertEqual(len(rows),1)
        self.assertTrue(all(call.kwargs=={'Name':'daily','GroupName':'research'} for call in scheduler.get_schedule.call_args_list))
        scheduler.get_schedule.return_value['GroupName']='default'
        with self.assertRaisesRegex(ValueError,'identity differs'):
            op.schedule_evidence(events,scheduler,'justhodl-test',ARN,{'eventbridge_scheduler':{'schedule_name':'daily','group_name':'research'}},None)

    def test_incomplete_inventory_and_unverified_qualified_targets_do_not_become_no_schedule(self):
        events,scheduler=self.clients(ARN)
        def partial(**kw):
            yield {'Schedules':[]}
            raise RuntimeError('second page denied')
        scheduler.get_paginator.return_value.paginate.side_effect=partial
        with self.assertRaisesRegex(RuntimeError,'second page denied'):op.schedule_evidence(events,scheduler,'justhodl-test',ARN,{},None)
        scheduler.get_paginator.return_value.paginate.side_effect=None
        scheduler.get_paginator.return_value.paginate.return_value=[{'Schedules':[{'Name':'daily','GroupName':'default','Target':{'Arn':ARN+':unverified'}}]}]
        scheduler.get_schedule.return_value['Target']['Arn']=ARN+':unverified'
        with self.assertRaisesRegex(ValueError,'unverified function/version'):op.schedule_evidence(events,scheduler,'justhodl-test',ARN,{},None)
if __name__=='__main__':unittest.main(verbosity=2)
