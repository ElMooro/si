"""Counterexamples in the whole retained predecessor, using only synthetic data.

This is defect evidence, not native acceptance. No ledger or provider is read.
"""
from pathlib import Path
from datetime import date,timedelta
import hashlib,json,math,random,sys,types,unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
SOURCE=ROOT/'tests/fixtures/pre-genealogy-calendar-audit.py.txt'

def legacy():
    captured={}
    def forbidden(*args,**kwargs):raise AssertionError('External acquisition is forbidden in this audit')
    s3=types.SimpleNamespace(put_object=lambda **kw:captured.update(json.loads(kw['Body'])))
    aws=types.ModuleType('boto3');aws.resource=lambda *a,**k:None;aws.client=lambda *a,**k:s3
    secret=types.ModuleType('managed_secret');secret.managed_secret=forbidden
    scope={'__name__':'isolated_legacy_genealogy','__file__':str(SOURCE)}
    with patch.dict(sys.modules,{'boto3':aws,'managed_secret':secret}),patch('urllib.request.urlopen',forbidden):
        exec(compile(SOURCE.read_bytes(),str(SOURCE),'exec'),scope)
    return scope,captured

class CalendarAudit(unittest.TestCase):
    def test_whole_predecessor_is_exact(self):
        raw=SOURCE.read_bytes()
        self.assertEqual(len(raw),17498)
        self.assertEqual(hashlib.sha256(raw).hexdigest(),EXPECTED_SHA)

    def test_array_lag_one_can_represent_three_calendar_days(self):
        module,_=legacy();rng=random.Random(271828)
        left=[rng.uniform(-1,1) for _ in range(40)];right=[2]+left[:-1]
        best,_=module['lead_lag'](left,right,max_lag=4)
        days=[date(2026,1,1)+timedelta(days=3*i) for i in range(40)]
        self.assertEqual(best['lag'],1)
        self.assertEqual({(b-a).days for a,b in zip(days,days[1:])},{3})

    def test_missing_benchmark_returns_become_zero_and_spy_tests_are_unreported(self):
        module,captured=legacy();days=[str(date(2026,1,1)+timedelta(days=i)) for i in range(40)]
        items=[{'signal_type':'synthetic_family','logged_at':d+'T12:00:00Z','predicted_dir':'UP'} for d in days]
        module['scan_outcomes']=lambda:items
        module['_spy_history']=lambda:{days[0]:100,days[1]:110,days[3]:115}
        calls=[];original=module['lead_lag']
        def record(a,b,*args,**kwargs):
            value=original(a,b,*args,**kwargs);calls.append((list(b),value));return value
        module['lead_lag']=record
        module['lambda_handler']({},None)
        benchmark,(_,curve)=calls[0]
        self.assertAlmostEqual(benchmark[1],10)
        self.assertEqual(benchmark[2],0.0,'Missing current close became a zero return')
        self.assertEqual(benchmark[3],0.0,'Missing previous close became a zero return')
        self.assertGreater(len(curve),0)
        self.assertEqual(captured['n_hypothesis_tests'],0,'The output counts pair tests, omitting all SPY lag tests')

    def test_incomplete_scan_is_returned_without_coverage_metadata(self):
        module,_=legacy();calls=[]
        def scan(**kwargs):
            calls.append(kwargs)
            return {'Items':[{'synthetic':True}]*50001,'LastEvaluatedKey':{'next':'page-'+str(len(calls))}}
        module['ddb']=types.SimpleNamespace(Table=lambda name:types.SimpleNamespace(scan=scan))
        records=module['scan_outcomes']()
        self.assertEqual(len(calls),2);self.assertEqual(len(records),100002)
        self.assertIsInstance(records,list)
        self.assertEqual(calls[1],{'ExclusiveStartKey':{'next':'page-1'}})

    def test_reported_pair_count_includes_an_untested_pair_and_omits_remaining_coverage(self):
        module,captured=legacy();days=[str(date(2026,1,1)+timedelta(days=i)) for i in range(20)]
        items=[{'signal_type':'family_'+str(n),'logged_at':d+'T12:00:00Z','predicted_dir':'UP'} for n in range(100) for d in days]
        module['scan_outcomes']=lambda:items;module['_spy_history']=lambda:{}
        calls=[]
        def record(*args,**kwargs):calls.append(kwargs);return {},[]
        module['lead_lag']=record;module['lambda_handler']({},None)
        actual=sum(call.get('max_lag')==14 for call in calls)
        self.assertEqual(actual,3500);self.assertEqual(captured['n_pairs_tested'],3501)
        self.assertEqual(100*99//2-actual,1450)
        self.assertNotIn('untested_pairs',captured)

    def test_perfect_correlation_and_constant_series_share_zero_test_statistic(self):
        module,_=legacy()
        self.assertEqual(module['_tstat'](1.0,40),0.0)
        self.assertEqual(module['pval'](module['_tstat'](1.0,40)),1.0)
        self.assertEqual(module['_corr']([1]*40,[2]*40),0.0)

    def test_normal_approximation_changes_a_small_sample_five_percent_decision(self):
        module,_=legacy();n=15;r=2/math.sqrt(17);stat=module['_tstat'](r,n)
        self.assertAlmostEqual(stat,2.0);self.assertLess(module['pval'](stat),0.05)
        # Independently integrate the Student t density under iid normal pairs.
        # Even this reference assumption is not established for the time series.
        df=n-2;scale=math.gamma((df+1)/2)/(math.sqrt(df*math.pi)*math.gamma(df/2))
        density=lambda x:scale*(1+x*x/df)**(-(df+1)/2)
        steps=20000;h=stat/steps
        area=h/3*(density(0)+density(stat)+sum((4 if i%2 else 2)*density(i*h) for i in range(1,steps)))
        exact=1-2*area
        self.assertGreater(exact,0.066);self.assertLess(exact,0.067)

    def test_non_dates_enter_the_reported_daily_axis(self):
        module,_=legacy()
        rows=module['build_series']([{'signal_type':'synthetic','logged_at':'not-a-date-and-clock','predicted_dir':'UP'}])
        self.assertEqual(rows,{'synthetic':{'not-a-date':[1,1]}})

EXPECTED_SHA='d2140ca6a4647a6944d07b14ce2e63d6f52a1c8c2928d7fddbf74093c19cd660'
if __name__=='__main__':unittest.main()
