from datetime import date, timedelta
from decimal import Decimal
import unittest
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'aws/shared'))
from global_liquidity_calendar import POLICY, effective_date, series_rows, select, subtotal, endpoint_change


def series(sid, values):
    return series_rows(sid, {'observations': {'observations': [{'date': day, 'value': value} for day, value in values]}},
        {'unit': POLICY[sid][0], 'frequency': POLICY[sid][1], 'definition': {'frequency': 'Monthly, End of Period'}})


class Tests(unittest.TestCase):
    def test_no_future_or_missing_value_fill(self):
        rows=series('DEXJPUS',[('2026-09-11','.'),('2026-09-10','150')])
        self.assertEqual(select('DEXJPUS',rows,'2026-09-11')['reason'],'latest_observation_missing')
        self.assertEqual(select('DEXJPUS',rows,'2021-01-01')['reason'],'no_observation_on_or_before_valuation')
        self.assertEqual(select('DEXJPUS',rows,'2026-09-10')['status'],'descriptive')

    def test_month_end_unit_and_positive_fx(self):
        rows=series('JPNASSETS',[('2026-08-01','10'),('2026-07-01','9')])
        self.assertEqual(select('JPNASSETS',rows,'2026-08-05')['selected']['native_decimal'],'9')
        self.assertEqual(effective_date('JPNASSETS','2024-02-01').isoformat(),'2024-02-29')
        self.assertEqual(select('DEXJPUS',series('DEXJPUS',[('2026-09-11','0')]),'2026-09-11')['status'],'unavailable')
        self.assertEqual(select('WALCL',series('WALCL',[('2026-09-11','0')]),'2026-09-11')['status'],'descriptive')
        bad=series_rows('WALCL',{'observations':{'observations':[]}}, {'unit':'Billions of U.S. Dollars','frequency':'W'})
        self.assertEqual(bad['status'],'unreviewed_definition')

    def test_carry_limit_and_period_basis(self):
        rows=series('ECBASSETSW',[('2026-09-01','100')])
        self.assertEqual(select('ECBASSETSW',rows,'2026-09-15')['status'],'descriptive')
        self.assertEqual(select('ECBASSETSW',rows,'2026-09-16')['reason'],'observation_exceeds_carry_limit')
        bad=series_rows('JPNASSETS',{'observations':{'observations':[]}}, {'unit':'100 Million Yen','frequency':'M','definition':{'frequency':'Monthly'}})
        self.assertEqual(bad['status'],'unreviewed_period_basis')

    def snapshot(self, day, fed, euro, yen, eurusd, usdjpy):
        month=day[:7]+'-01'
        d=date.fromisoformat(day)
        if d.day<28:
            month=(d.replace(day=1)-timedelta(days=1)).replace(day=1).isoformat()
        inputs={sid:series(sid,[(month if sid=='JPNASSETS' else day,value)]) for sid,value in (
            ('WALCL',fed),('ECBASSETSW',euro),('JPNASSETS',yen),('DEXUSEU',eurusd),('DEXJPUS',usdjpy))}
        return subtotal(inputs,day)

    def test_fx_balance_decomposition_reconciles(self):
        end='2026-09-18';start='2026-06-19'
        old=self.snapshot(start,'100','100','10','1','100')
        new=self.snapshot(end,'100','110','10','1.2','100')
        self.assertEqual(Decimal(old['total_usd_millions_decimal']),210)
        self.assertEqual(Decimal(new['total_usd_millions_decimal']),242)
        result=endpoint_change({start:old,end:new},end,13)
        self.assertEqual(result['calendar_days'],91)
        self.assertEqual(Decimal(result['change_usd_millions_decimal']),32)
        self.assertEqual(Decimal(result['balance_effect_usd_millions_decimal']),10)
        self.assertEqual(Decimal(result['fx_effect_usd_millions_decimal']),22)
        self.assertEqual(Decimal(result['rounding_residual_usd_millions_decimal']),0)
        self.assertEqual(endpoint_change({end:new},end,52)['status'],'missing_endpoint')

    def test_reciprocal_fx_rounding_explicit_and_missing_component_not_subtotal_zero(self):
        start='2026-06-19';end='2026-09-18'
        a=self.snapshot(start,'6746548','5911237','6446620','1.1604','153.71')
        b=self.snapshot(end,'6736117','5910896','6447265','1.1879','149.93')
        result=endpoint_change({start:a,end:b},end,13)
        amount=Decimal(result['change_usd_millions_decimal'])
        parts=[Decimal(result[k]) for k in ('balance_effect_usd_millions_decimal','fx_effect_usd_millions_decimal','rounding_residual_usd_millions_decimal')]
        self.assertEqual(amount,sum(parts))
        incomplete=subtotal({'WALCL':series('WALCL',[(end,'0')])},end)
        self.assertIsNone(incomplete['total_usd_millions_decimal'])
        self.assertEqual(incomplete['missing_components'],['ECBASSETSW','JPNASSETS'])


if __name__=='__main__':unittest.main()
