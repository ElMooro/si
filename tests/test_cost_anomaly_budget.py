"""Actual cost-detector source with invented CE data and all transport replaced."""
from contextlib import redirect_stdout
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import types
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
SOURCE=ROOT/'aws/lambdas/justhodl-cost-anomaly/source/lambda_function.py'
CONFIG=ROOT/'aws/lambdas/justhodl-cost-anomaly/config.json'


def load(environment=None):
    boto=types.ModuleType('boto3')
    boto.client=lambda *args,**kwargs: None
    module=types.ModuleType('cost_detector_fixture')
    with patch.dict('sys.modules',{'boto3':boto}),patch.dict(os.environ,environment or {},clear=True):
        exec(compile(SOURCE.read_text(encoding='utf-8'),str(SOURCE),'exec'),module.__dict__)
    return module


class Clock(datetime):
    @classmethod
    def now(cls,tz=None):return cls(2020,4,11,tzinfo=timezone.utc)


class CE:
    def __init__(self):self.calls=[]
    def get_cost_and_usage(self,**kw):
        self.calls.append(kw)
        return {'ResultsByTime':[{'TimePeriod':{'Start':f'2020-04-{day:02d}'},
                  'Groups':[{'Keys':['InventedService'],
                    'Metrics':{'UnblendedCost':{'Amount':'4','Unit':'USD'}}}]} for day in range(1,11)]}


class Tests(unittest.TestCase):
    def test_declared_and_default_budget_agree(self):
        self.assertEqual(json.loads(CONFIG.read_text(encoding='utf-8'))['env']['MONTHLY_BUDGET_USD'],'150')
        self.assertEqual(load().MONTHLY_BUDGET_USD,150)
        self.assertEqual(load({'MONTHLY_BUDGET_USD':'321'}).MONTHLY_BUDGET_USD,321)

    def test_complete_synthetic_ce_result_only_changes_budget_percentage(self):
        old,new=load({'MONTHLY_BUDGET_USD':'300'}),load()
        for module in (old,new):module.datetime=Clock;module.ce=CE()
        before,after=old.fetch_aws_spend(),new.fetch_aws_spend()
        old_pct,new_pct=before.pop('projected_vs_budget_pct'),after.pop('projected_vs_budget_pct')
        self.assertEqual(before,after)
        self.assertEqual(old.ce.calls,new.ce.calls)
        self.assertEqual(len(new.ce.calls),1)
        self.assertAlmostEqual(new_pct,old_pct*2,delta=0.1)

    def test_existing_alert_boundary_and_output_contract_preserved(self):
        for pct,expected in ((100,False),(110,False),(110.1,True)):
            with self.subTest(pct=pct):
                module=load();messages=[];writes=[]
                module.fetch_aws_spend=lambda:{'anomalies':[],'projected_vs_budget_pct':pct}
                module.detect_lambda_invocation_anomalies=lambda:{'anomalies':[]}
                module.fetch_anthropic_spend=lambda:{}
                module.check_zai_balance=lambda:{'status':'ok'}
                module.s3=types.SimpleNamespace(put_object=lambda **kw:writes.append(kw))
                module.send_telegram=lambda msg:messages.append(msg)
                with redirect_stdout(io.StringIO()):module.lambda_handler()
                self.assertEqual(bool(messages),expected)
                self.assertEqual(len(writes),2)
                self.assertEqual(writes[0]['Key'],'data/cost-anomaly.json')
                self.assertEqual(json.loads(writes[0]['Body'])['monthly_budget_usd'],150)
                self.assertEqual(json.loads(writes[0]['Body'])['aws_spend']['projected_vs_budget_pct'],pct)

    def test_daily_schedule_unchanged(self):
        cfg=json.loads(CONFIG.read_text(encoding='utf-8'))
        self.assertEqual(cfg['schedule']['rule_name'],'cost-anomaly-daily')
        self.assertEqual(cfg['schedule']['cron'],'cron(0 9 * * ? *)')

if __name__=='__main__':unittest.main()
