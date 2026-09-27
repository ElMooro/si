from datetime import datetime,timedelta,timezone
import unittest
import portwatch_measurements as m
AT='2026-09-27T11:20:00+00:00'
END=datetime(2026,9,20,tzinfo=timezone.utc).date()


def history(days=800,kind='ports',value=10):
    rows={}
    for i in range(days):
        d=(END-timedelta(days=days-1-i)).isoformat()
        rows['port1|'+d]={'portid':'port1','date':d,'portname':'Test <port>','country':'Example','portcalls':value,'n_total':value}
    return {kind:rows,'version':'original'}


def first(h):return m.build(h,AT)['entities'][0]


class CalendarTests(unittest.TestCase):
    def test_seven_vs_seven_anniversary_not_fourteen(self):
        h=history()
        for i in range(7):h['ports']['port1|'+(END-timedelta(days=i)).isoformat()]['portcalls']=20
        # The other seven days inside the old 14-row denominator are very large;
        # they must not contaminate the matching seven-date anniversary.
        ann=END.replace(year=2025)
        for i in range(7,14):h['ports']['port1|'+(ann-timedelta(days=i)).isoformat()]['portcalls']=1000
        r=first(h);self.assertEqual(r['current_7d']['mean'],20);self.assertEqual(r['prior_year_7d']['mean'],10)
        self.assertEqual(r['year_over_year']['percent'],100);self.assertEqual(r['prior_year_7d']['expected_days'],7)
        self.assertEqual(r['observation_lag_days'],7);self.assertEqual(len(r['observations']),800)
    def test_missing_date_and_latest_null_do_not_shift_calendar_or_fill_zero(self):
        h=history();missing=(END-timedelta(days=3)).isoformat();del h['ports']['port1|'+missing]
        r=first(h);self.assertEqual(r['current_7d']['missing_dates'],[missing]);self.assertIsNone(r['current_7d']['mean'])
        h=history();h['ports']['port1|'+END.isoformat()]['portcalls']=None
        r=first(h);self.assertEqual(r['last_observation_date'],END.isoformat());self.assertIsNone(r['year_over_year']['percent'])
    def test_zero_denominator_and_zero_numerator_are_distinct(self):
        r=first(history(value=0));self.assertEqual(r['current_7d']['mean'],0);self.assertIsNone(r['year_over_year']['percent'])
        self.assertEqual(r['year_over_year']['status'],'zero_comparison_denominator')
        h=history()
        for i in range(7):h['ports']['port1|'+(END-timedelta(days=i)).isoformat()]['portcalls']=0
        self.assertEqual(first(h)['year_over_year']['percent'],-100)
    def test_exact_count_field_not_cargo_or_boolean_substitution(self):
        h=history()
        last=h['ports']['port1|'+END.isoformat()];last.pop('portcalls');last['import']=100000
        r=first(h);self.assertIsNone(r['current_7d']['mean']);self.assertEqual(r['source_field'],'portcalls')
        for invalid in (True,-1,1.5,float('inf'),10**500):self.assertFalse(m.count(invalid))
        r=first(history(kind='choke'));self.assertEqual(r['unit'],'transit_calls');self.assertEqual(r['source_field'],'n_total')
        r=first(history(kind='choke_fallback'));self.assertEqual(r['unit'],'port_calls_fallback_not_transit_calls')
    def test_all_families_and_older_rows_survive_without_ranked_subset(self):
        h=history();h['choke']=history(40,'choke')['choke'];h['choke_fallback']=history(30,'choke_fallback')['choke_fallback']
        result=m.build(h,AT);self.assertEqual(result['history_rows'],870);self.assertEqual(result['entity_count'],3)
        self.assertEqual(sum(len(r['observations']) for r in result['entities']),870)
        self.assertTrue(all(not r['sizing_eligible'] for r in result['entities']))
    def test_future_and_mismatched_entity_dates_remain_visible(self):
        h=history();row={'portid':'port1','date':'2026-10-01','portcalls':20}
        h['ports']['port1|2026-10-01']=row;r=first(h)
        self.assertEqual(r['future_rows'],1);self.assertEqual(r['last_observation_date'],END.isoformat())
        row['portid']='port2';r=first(h);self.assertEqual(r['invalid_identity_rows'],1);self.assertIsNone(r['current_7d']['mean'])
        self.assertEqual(len(r['observations']),801)
    def test_leap_day_has_no_invented_anniversary(self):
        h={'ports':{}}
        for i in range(800):
            d=(datetime(2024,2,29).date()-timedelta(days=i)).isoformat();h['ports']['p|'+d]={'date':d,'portid':'p','portcalls':10}
        r=m.build(h,'2024-03-01T00:00:00Z')['entities'][0]
        self.assertEqual(r['current_7d']['mean'],10);self.assertIsNone(r['prior_year_7d']['mean']);self.assertIsNone(r['year_over_year']['percent'])
        self.assertEqual(r['preceding_358d']['expected_days'],358)
    def test_date_and_arithmetic_bounds_are_explicit(self):
        self.assertIsNone(m.source_date(True));self.assertIsNone(m.source_date('2026-02-30'))
        self.assertIsNone(m.source_date('2026-09-20T12:00:00Z'));self.assertIsNone(m.source_date('2026-09-20T00:00:00'))
        r=first(history(value=9007199254740991));self.assertEqual(r['current_7d']['status'],'outside_exact_json_range');self.assertIsNone(r['current_7d']['mean'])


if __name__=='__main__':unittest.main()
