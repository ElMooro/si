"""Synthetic descriptive price-window tests; no provider or native invocation."""
from pathlib import Path
from datetime import date,timedelta
from decimal import Decimal,localcontext
from copy import deepcopy
import gzip,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-pump-positioning/source')]
import positioning_observations as m
import context_evidence_store as store
ASOF=date(2026,9,28)
AT='2026-09-28T01:00:00Z'


def prices(n=65):
    return [{'symbol':'SYNA','date':(ASOF-timedelta(days=n-i)).isoformat(),'open':100,'high':102,'low':98,'close':100,'volume':0} for i in range(n)]


def calculate(rows):return {r['name']:r for r in m.measurements(m.price_history(rows,'SYNA',ASOF))}


def captures():
    docs={name:{'private_context':'never publish','source_secret':'synthetic-only'} for name in m.INPUTS}
    docs['radar']={'pump_candidates':[{'ticker':'SYNA','pump_likelihood':99,'private_weight':250}]}
    sources={};inputs={};attempts=[]
    def ref(doc):
        raw=json.dumps(doc,separators=(',',':')).encode();r=store.identity(raw,m.PRIVATE,'sources');sources[r['key']]=raw;return r
    for name,key in m.INPUTS.items():
        inputs[name]={'source_key':key,'status':'received','requested_at':AT,'received_at':AT,'original_ref':ref(docs[name])}
    for kind,doc in [('history',prices()),('quote',[{'symbol':'OTHER','price':999},{'symbol':'SYNA','price':101,'timestamp':1790553600}]),('profile',[{'symbol':'SYNA','currency':'USD'}])]:
        attempts.append({'ticker':'SYNA','kind':kind,'endpoint':m.endpoint('SYNA',kind,ASOF),'status':'received','http_status':200,
            'network_attempted':True,'requested_at':AT,'received_at':AT,'original_ref':ref(doc)})
    return inputs,attempts,sources


class Tests(unittest.TestCase):
    def test_complete_projection_binds_raw_sources_and_withholds_allocation(self):
        a,b,s=captures();p=m.build(a,b,s,AT);self.assertEqual(p,m.build(a,b,s,AT))
        self.assertEqual(len(p['source_inputs']),6);self.assertEqual(len(p['acquisitions']),3)
        self.assertEqual(p['coverage']['measured_descriptive_fields'],7)
        self.assertEqual(p['candidates'],[]);self.assertIsNone(p['macro_regime']);self.assertIsNone(p['sizing_assumptions'])
        self.assertEqual(p['portfolio_basket']['positions'],[]);self.assertIsNone(p['portfolio_basket']['total_exposure'])
        self.assertEqual(p['aggressive_basket']['positions'],[]);self.assertEqual(p['call'],'WAIT')
        for flag in m.FLAGS:self.assertIs(p[flag],False)
        body=store.encode(p)
        for secret in (b'never publish',b'synthetic-only',b'private_weight',b'pump_likelihood',b'OTHER'):self.assertNotIn(secret,body)
        row=p['price_observations'][0];self.assertEqual(row['reported_quote']['price'],101)
        self.assertEqual(row['reported_quote']['source_pointer'],'/1');self.assertEqual(row['reported_profile_currency']['value'],'USD')
        self.assertIs(row['reported_profile_currency']['quote_currency_binding_verified'],False)
    def test_untrusted_original_identity_scope_clock_and_missing_outcomes_fail(self):
        for change in (lambda a,b,s:b.pop(),lambda a,b,s:b[0].update(endpoint='https://example.com/private'),
                       lambda a,b,s:b[0].update(http_status=True),lambda a,b,s:b[1].update(received_at='2099-01-01T00:00:00Z'),
                       lambda a,b,s:a['radar'].update(source_key='private/accounts.json'),
                       lambda a,b,s:s.update({b[0]['original_ref']['key']:b'wrong'})):
            a,b,s=captures();change(a,b,s)
            with self.assertRaises(ValueError):m.build(a,b,s,AT)
    def test_missing_history_preserves_previous_and_quote_never_substitutes_for_it(self):
        a,b,s=captures();b[0].pop('original_ref');b[0].update(status='not_attempted_stop',http_status=None,network_attempted=False)
        with self.assertRaises(ValueError):m.build(a,b,s,AT)
    def test_quote_wrong_issuer_duplicate_issuer_and_future_clock_do_not_select_first_row(self):
        for doc in ([{'symbol':'OTHER','price':999}], [{'symbol':'SYNA','price':101},{'symbol':'SYNA','price':102}],
                    [{'symbol':'SYNA','price':101,'timestamp':4070908800}]):
            a,b,s=captures();raw=json.dumps(doc).encode();ref=store.identity(raw,m.PRIVATE,'sources');s[ref['key']]=raw;b[1]['original_ref']=ref
            p=m.build(a,b,s,AT);self.assertIsNone(p['price_observations'][0]['reported_quote']['price'])
            self.assertEqual(p['coverage']['measured_descriptive_fields'],7)
    def test_zero_volatility_volume_and_price_change_are_real_measured_zero(self):
        out=calculate(prices())
        for name in ('price_change_5','price_change_20','price_change_60','sample_log_return_std_30','mean_volume_20'):
            self.assertEqual(out[name]['value'],0);self.assertEqual(out[name]['status'],'measured')
        self.assertEqual(out['mean_true_range_14']['value'],4)
        for row in out.values():
            for flag in m.FLAGS:self.assertIs(row[flag],False)
    def test_volume_keeps_zero_observations_and_exact_twenty_row_denominator(self):
        rows=prices();rows[-1]['volume']=100;metric=calculate(rows)['mean_volume_20']
        self.assertEqual(metric['value'],5);self.assertEqual(len(metric['operands']),20)
        self.assertEqual(metric['operands'][0]['value_exact'],'0')
        self.assertEqual(metric['operands'][-1]['source_pointer'],'/64/volume')
    def test_sample_standard_deviation_matches_independent_single_jump_identity(self):
        rows=prices();rows[-1].update(close=121,high=122)
        metric=calculate(rows)['sample_log_return_std_30']
        with localcontext() as ctx:
            ctx.prec=34;expected=(Decimal('1.21').ln()/Decimal(30).sqrt())*100
        self.assertAlmostEqual(metric['value'],float(expected),places=12)
        self.assertEqual(metric['required_observations'],31);self.assertIn('denominator 29',metric['definition']);self.assertIn('no annualization',metric['definition'])
    def test_price_change_has_exact_endpoints_and_is_not_total_return(self):
        rows=prices();rows[-1].update(close=110,high=112);p=calculate(rows)['price_change_20']
        self.assertEqual(p['value'],10);self.assertEqual(p['operands'][0]['source_index'],44);self.assertEqual(p['operands'][-1]['source_index'],64)
        self.assertEqual(p['operands'][0]['value_exact'],'100');self.assertEqual(p['operands'][-1]['value_exact'],'110');self.assertIn('not total return',p['definition'])
    def test_missing_member_cannot_disappear_or_backfill_an_older_valid_price(self):
        rows=prices();rows[-5]['close']=None;out=calculate(rows)
        for name in ('price_change_5','sample_log_return_std_30','mean_true_range_14'):
            self.assertIsNone(out[name]['value']);self.assertEqual(out[name]['reason'],'invalid_member_in_selected_window')
        self.assertIsNone(next(o for o in out['price_change_5']['operands'] if o['source_index']==60)['value_exact'])
    def test_insufficient_window_is_unavailable_even_with_six_valid_returns(self):
        out=calculate(prices(7));self.assertEqual(out['price_change_5']['value'],0)
        self.assertIsNone(out['sample_log_return_std_30']['value']);self.assertEqual(out['sample_log_return_std_30']['reason'],'insufficient_completed_observations')
    def test_boolean_nonfinite_and_inconsistent_ohlcv_are_not_numbers(self):
        for edits in ({'close':True},{'volume':True},{'close':float('nan')},{'high':90},{'low':101},{'volume':-1},{'open':0}):
            rows=prices();rows[-1].update(edits);out=calculate(rows)
            self.assertTrue(all(r['value'] is None for r in out.values()))
    def test_wrong_issuer_bad_date_and_duplicates_block_all_history_measurements(self):
        for edits in ({'symbol':'OTHER'},{'date':'2026-09-01 garbage'},{'date':prices()[1]['date']}):
            rows=prices();rows[0].update(edits);out=calculate(rows)
            self.assertTrue(all(r['reason']=='source_identity_or_date_unavailable' for r in out.values()))
    def test_current_and_future_dates_are_excluded_explicitly(self):
        rows=prices();rows.extend([dict(rows[-1],date=ASOF.isoformat()),dict(rows[-1],date=(ASOF+timedelta(days=1)).isoformat())])
        h=m.price_history(rows,'SYNA',ASOF);self.assertEqual(h['reported_count'],67);self.assertEqual(h['completed_date_count'],65)
        self.assertEqual([i['reason'] for i in h['issues']],['current_or_future_utc_date_excluded']*2)
        self.assertEqual(m.measurements(h)[0]['value'],0);self.assertIs(h['exchange_sessions_verified'],False)
    def test_invalid_old_numeric_member_does_not_change_shorter_selected_window(self):
        rows=prices();rows[0]['close']=None;out=calculate(rows)
        self.assertEqual(out['price_change_5']['value'],0);self.assertEqual(out['price_change_60']['value'],0)
    def test_source_row_coordinates_survive_provider_descending_order(self):
        h=m.price_history(prices()[::-1],'SYNA',ASOF);out=m.measurements(h)
        self.assertEqual(out[0]['operands'][-1]['source_pointer'],'/0/close')
        self.assertEqual(out[0]['operands'][0]['source_pointer'],'/5/close')
    def test_history_outside_requested_window_is_retained_as_issue_not_used(self):
        rows=prices(100);h=m.price_history(rows,'SYNA',ASOF)
        self.assertEqual(h['reported_count'],100);self.assertEqual(h['completed_date_count'],90)
        self.assertEqual(len(h['issues']),10)
        self.assertTrue(all(i['reason']=='outside_declared_lookback_excluded' for i in h['issues']))
        self.assertEqual(m.measurements(h)[-1]['operands'][0]['source_pointer'],'/99/close')
    def test_http_error_status_and_network_attempt_are_not_inferred(self):
        for change in (dict(http_status=True),dict(http_status=700),dict(network_attempted=None),
                       dict(status='http_error',http_status=200),dict(status='not_attempted_stop')):
            a,b,s=captures();b[1].update(change)
            with self.assertRaises(ValueError):m.build(a,b,s,AT)
    def test_original_twelve_occurrence_scope_duplicate_names_and_empty_population(self):
        rows=[{'ticker':'SYNA'},{'ticker':'SYNA'},{'ticker':'../BAD'}]+[{'ticker':'SYN'+str(i)} for i in range(12)]
        out=m.selection({'pump_candidates':rows});self.assertEqual(len(out['occurrences']),15)
        self.assertEqual(len(out['selected_tickers']),10);self.assertEqual(out['occurrences'][1]['status'],'selected_duplicate_occurrence')
        self.assertEqual(out['occurrences'][2]['status'],'invalid_literal_symbol');self.assertEqual(out['occurrences'][12]['status'],'outside_original_twelve_occurrences')
        self.assertEqual(m.selection({'pump_candidates':[]})['status'],'reported_empty_selection')
        with self.assertRaises(ValueError):m.selection({'pump_candidates':[{'ticker':'../BAD'}]})
        with self.assertRaises(ValueError):m.selection({})
    def test_exact_json_precision_duplicate_keys_nonfinite_and_whole_gzip(self):
        raw=b'{"price":0.12345678901234567890123456789}'
        self.assertEqual(m.strict(raw)['price'],Decimal('0.12345678901234567890123456789'))
        self.assertEqual(m.strict(gzip.compress(raw),'gzip'),m.strict(raw))
        for raw in (b'{"v":1,"v":2}',b'{"v":NaN}',b'{"v":1e999}',gzip.compress(b'{}')[:-2],gzip.compress(b'{}')+gzip.compress(b'{}')):
            with self.assertRaises(ValueError):m.strict(raw)


if __name__=='__main__':unittest.main(verbosity=2)
