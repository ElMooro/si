from pathlib import Path
from datetime import datetime, timezone
from copy import deepcopy
import importlib.util,io,json,socket,sys,unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import compound_overlays as m
NOW=datetime(2026,10,2,12,tzinfo=timezone.utc)
CONTRACTS={'score_basis':'invented-basis','numeric_contract':'compound-numeric.v1'}
ROW={'symbol':'QAONLY','systems':['nobrainers','insiders'],'n_systems':2,'compound_score':150}


def contexts():
    docs={'data/trend-reversal.json':{'rows':[{'ticker':'QAONLY','direction':None,'reversal_score':0,'spk':[90,100,105,110]}],'breadth':{'bottom_pct':0,'top_pct':0}},
          'data/compound-firstseen.json':{},'data/risk-gate.json':{'posture':'NEUTRAL'},'data/compound-history.json':{'days':[]}}
    return {key:{'document':doc,'evidence':{'source':key,'status':'received'}} for key,doc in docs.items()}


def run(c=None,rows=None,complete=True):
    return m.apply(rows or [ROW],contexts() if c is None else c,NOW,CONTRACTS,complete)


class Overlay(unittest.TestCase):
    def setUp(self):
        p=patch.object(socket.socket,'connect',side_effect=AssertionError('Offline only'));p.start();self.addCleanup(p.stop)

    def test_normal_lifecycle_is_explicitly_not_observation_freshness(self):
        rows,state,history,evidence=run();row=rows[0]
        self.assertEqual(row['desk_score'],187.5);self.assertEqual(row['lifecycle_decay'],1)
        self.assertEqual(state['QAONLY|nobrainers'],'2026-10-02')
        calc=row['desk_score_calculation'];self.assertEqual(calc['family_prior_mean'],1.25)
        self.assertFalse(calc['observation_freshness_qualified']);self.assertFalse(calc['forecast_qualified'])
        self.assertEqual(calc['components'][0]['origin'],'initialized_this_evaluation')
        self.assertEqual(history['days'][-1]['scores'],{'QAONLY':150})

    def test_invalid_future_and_underflowing_first_seen_never_boosts_score(self):
        for value in ('invalid','2026-10-03','0001-01-01',None,True,17):
            c=contexts();c['data/compound-firstseen.json']['document']={'QAONLY|nobrainers':value,'QAONLY|insiders':value}
            rows,state,_,_=run(c)
            self.assertIsNone(rows[0]['desk_score']);self.assertIsNone(rows[0]['freshness'])
            self.assertEqual(rows[0]['compound_score'],150);self.assertEqual(state['QAONLY|nobrainers'],value)

    def test_unavailable_state_does_not_bootstrap_current_dates_or_write_state(self):
        c=contexts();c['data/compound-firstseen.json']['document']=None
        rows,state,_,e=run(c);self.assertIsNone(state);self.assertIsNone(rows[0]['desk_score']);self.assertFalse(e['first_seen_write_planned'])

    def test_missing_context_is_not_clean_tape_fresh_or_neutral_zero(self):
        c={key:{'document':None,'evidence':{'status':'unavailable'}} for key in m.CONTEXT_KEYS}
        rows,_,history,_=run(c,complete=False);row=rows[0]
        for key in ('archetype','entry_quality','chg5_proxy_pct','desk_score','freshness'):self.assertIsNone(row[key])
        self.assertEqual(row['regime'],{'posture':None,'turn_net':None});self.assertIsNone(history)

    def test_breadth_zero_is_valid_but_missing_boolean_and_bad_ranges_are_not(self):
        for breadth,expected in [({'top_pct':0,'bottom_pct':0},0),({'top_pct':7,'bottom_pct':0},-7),({'top_pct':0},None),({'top_pct':True,'bottom_pct':4},None),({'top_pct':-1,'bottom_pct':4},None),({'top_pct':4,'bottom_pct':101},None)]:
            c=contexts();c['data/trend-reversal.json']['document']['breadth']=breadth
            self.assertEqual(run(c)[0][0]['regime']['turn_net'],expected)

    def test_undated_sparkline_change_is_never_called_five_day_or_entry_quality(self):
        row=run()[0][0];self.assertEqual(row['sparkline_change_pct'],10)
        self.assertIsNone(row['chg5_proxy_pct']);self.assertIsNone(row['entry_quality'])
        calc=row['reversal_evidence']['sparkline_calculation'];self.assertEqual(calc['interval_count'],2);self.assertIsNone(calc['calendar_days'])
        self.assertEqual(calc['start_pointer'],'/rows/0/spk/1');self.assertEqual(calc['end_pointer'],'/rows/0/spk/3')

    def test_duplicate_reversal_rows_cannot_change_label_by_order(self):
        records=[{'ticker':'QAONLY','direction':'TOP_FORMING','reversal_score':50},{'ticker':'qaonly','direction':'BOTTOM_FORMING','reversal_score':50}]
        for order in (records,list(reversed(records))):
            c=contexts();c['data/trend-reversal.json']['document']['rows']=order
            rows,_,_,e=run(c);self.assertIsNone(rows[0]['archetype']);self.assertIsNone(rows[0]['sparkline_change_pct'])
            self.assertEqual([r['record'] for r in e['reversal']['occurrences']],order)
            self.assertTrue(all(r['reason']=='duplicate_reversal_symbol' for r in e['reversal']['occurrences']))

    def test_reversal_numeric_string_is_typed_and_keeps_original(self):
        c=contexts();r={'ticker':'QAONLY','direction':'BOTTOM_FORMING','reversal_score':'50'};c['data/trend-reversal.json']['document']['rows']=[r]
        row=run(c)[0][0];self.assertEqual(row['archetype'],'BOTTOM_FORMING_REPORTED');self.assertEqual(row['reversal_context']['score'],50)
        self.assertEqual(row['reversal_evidence']['record'],r);self.assertFalse(row['reversal_evidence']['direction_qualified'])

    def test_history_requires_prior_ninety_calendar_days_and_current_contract(self):
        dates=['2001-01-01','2026-07-03','2026-07-04','2026-10-01','2026-10-02','2026-10-03','invalid']
        c=contexts();records=[{'d':d,**CONTRACTS,'scores':{'QAONLY':0}} for d in dates]
        c['data/compound-history.json']['document']['days']=records
        rows,_,history,e=run(c)
        included=[x['record']['d'] for x in e['history']['cohorts'] if x['status']=='included']
        self.assertEqual(included,['2026-07-04','2026-10-01']);self.assertEqual(rows[0]['pctile_90d_all'],100)
        self.assertEqual(rows[0]['history_comparison']['observations_all'],2)
        self.assertEqual([x['record'] for x in e['history']['cohorts']],records)
        self.assertEqual(history['days'][0],records[0])

    def test_duplicate_history_days_all_excluded_and_originals_preserved(self):
        records=[{'d':'2026-10-01',**CONTRACTS,'scores':{'QAONLY':v}} for v in (0,999)]
        c=contexts();c['data/compound-history.json']['document']['days']=records
        rows,_,_,e=run(c);self.assertNotIn('pctile_90d_all',rows[0]);self.assertEqual(e['history']['observations'],0)
        self.assertEqual([r['record'] for r in e['history']['cohorts']],records)

    def test_invalid_or_ambiguous_score_map_excludes_whole_cohort(self):
        for scores in ({'QAONLY':True},{'QAONLY':1,'qaonly':2},{'QAONLY':None},{'QAONLY':'1e-9999'},{}):
            c=contexts();c['data/compound-history.json']['document']['days']=[{'d':'2026-10-01',**CONTRACTS,'scores':scores}]
            rows,_,_,e=run(c);self.assertNotIn('pctile_90d_self',rows[0]);self.assertEqual(e['history']['observations'],0)

    def test_partial_input_coverage_cannot_replace_daily_history(self):
        c=contexts();c['data/compound-history.json']['document']['days']=[{'d':'2026-10-02',**CONTRACTS,'scores':{'OTHER':50}}]
        before=deepcopy(c);_,_,history,e=run(c,complete=False)
        self.assertIsNone(history);self.assertFalse(e['history_write_planned']);self.assertEqual(c,before)

    def test_null_desk_scores_sort_after_valid_zero_and_negative_scores(self):
        rows=[{**ROW,'symbol':s,'compound_score':score} for s,score in [('BAD',999),('ZERO',0),('NEG',-10)]]
        c=contexts();c['data/compound-firstseen.json']['document']={'BAD|nobrainers':'invalid'}
        result=run(c,rows)[0];self.assertEqual([r['symbol'] for r in result],['ZERO','NEG','BAD'])
        self.assertEqual(result[0]['desk_score'],0);self.assertIsNone(result[-1]['desk_score'])

    def test_coverage_requires_a_boolean_not_truthy_strings_or_numbers(self):
        for value in ('false', 'true', 1, 0, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                run(complete=value)

    def test_context_reader_is_bounded_typed_and_never_leaks_error_text(self):
        class Storage:
            def __init__(self,raw,length):self.body=io.BytesIO(raw);self.length=length
            def get_object(self,**kw):return {'Body':self.body,'ContentLength':self.length}
        for raw,length in ((b'{}',True),(b'{}',3),(b'{"v":0,"v":1}',13),(b'{"v":NaN}',9)):
            db=Storage(raw,length);r=m.read_context(db,'invented',m.CONTEXT_KEYS[0]);self.assertIsNone(r['document']);self.assertTrue(db.body.closed)
        db=Storage(b'{}',2);r=m.read_context(db,'invented',m.CONTEXT_KEYS[0]);self.assertEqual(r['document'],{});self.assertEqual(r['evidence']['source_bytes'],2)
        with self.assertRaises(ValueError):m.read_context(db,'invented','unreviewed/private')


if __name__=='__main__':unittest.main(verbosity=2)
