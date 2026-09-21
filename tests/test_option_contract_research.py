from pathlib import Path
from decimal import Decimal
import copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import option_contract_research as model

RECEIVED='2026-09-21T12:00:00+00:00'
def row(kind='call',strike=100):
    return {'details':{'ticker':'O:SPY260925'+('C' if kind=='call' else 'P')+str(strike*1000).zfill(8),
        'contract_type':kind,'strike_price':strike,'expiration_date':'2026-09-25','shares_per_contract':100,'exercise_style':'american'},
        'underlying_asset':{'ticker':'SPY','price':101},'day':{'volume':10,'last_updated':1789759800000000001},
        'open_interest':0,'implied_volatility':.2,'greeks':{'delta':.5 if kind=='call' else -.5,'gamma':.01}}
def page(rows,index=1,next_url=None,url=None):
    doc={'status':'OK','results':rows}
    if next_url:doc['next_url']=next_url
    raw=json.dumps(doc,separators=(',',':')).encode();digest=model.capture.sha(raw)
    url=url or model.capture.next_url(model.capture.initial_url('SPY'),'SPY')
    return {'page':index,'acquired_at':RECEIVED,'raw':raw,'request_url':url,'request_sha256':model.capture.sha(url.encode()),
        'original':{'key':'audit-private/20260909-originals/options-research/'+digest+'.bin','sha256':digest,'bytes':len(raw)}}
def compute(rows):return model.compile_rows('SPY',[page(rows)],True)

class Tests(unittest.TestCase):
    def test_zero_open_interest_survives(self):
        d=compute([row()]);r=d['rows'][0]
        self.assertTrue(r['identity_eligible']);self.assertEqual(r['metrics']['open_interest']['value'],'0')
        self.assertEqual(r['metrics']['open_interest']['state'],'reported_zero')
        self.assertIsNone(r['open_interest_date']);self.assertFalse(r['dealer_inventory_observed'])
        self.assertFalse(d['calls_eligible']);self.assertEqual(d['independent_investment_votes'],0)
    def test_missing_null_zero_different(self):
        a=row('call',100);b=row('call',101);c=row('call',102)
        del a['open_interest'];b['open_interest']=None
        d=compute([a,b,c]);self.assertEqual([r['metrics']['open_interest']['state'] for r in d['rows']],['missing','null','reported_zero'])
        self.assertFalse(d['reported_open_interest']['calls']['complete_field_coverage'])
    def test_pagination_can_not_be_falsely_complete(self):
        p=page([row()],next_url='https://api.polygon.io/v3/snapshot/options/SPY?cursor=two')
        with self.assertRaises(ValueError):model.compile_rows('SPY',[p],True)
        self.assertFalse(model.compile_rows('SPY',[p],False)['coverage']['pagination_complete'])
    def test_wrong_request_path_rejected(self):
        p=page([row()]);p['request_url']=p['request_url'].replace('SPY','NVDA')
        with self.assertRaises(ValueError):model.compile_rows('SPY',[p],True)
    def test_wrong_original_hash_rejected(self):
        p=page([row()]);p['raw']+=b' '
        with self.assertRaises(ValueError):model.compile_rows('SPY',[p],True)
    def test_duplicate_rows_retained_but_neither_aggregated(self):
        d=compute([row(),row()]);self.assertEqual(len(d['rows']),2)
        self.assertEqual(d['coverage']['eligible_identity_rows'],0)
        self.assertEqual(d['coverage']['duplicate_identities'],1)
        self.assertIsNone(d['reported_open_interest']['calls']['value'])
    def test_contract_identity_mismatch_is_local(self):
        bad=row('call',101);bad['details']['strike_price']=102
        d=compute([row(),bad]);self.assertEqual(len(d['rows']),2)
        self.assertFalse(d['rows'][1]['identity_eligible']);self.assertTrue(d['rows'][0]['identity_eligible'])
        self.assertIn('strike_differs_from_contract_id',d['rows'][1]['identity_reasons'])
    def test_nonstandard_deliverable_not_silently_100(self):
        r=row();r['details']['shares_per_contract']=150
        d=compute([r])['rows'][0]
        self.assertEqual(d['metrics']['shares_per_contract']['value'],'150');self.assertFalse(d['identity_eligible'])
    def test_equivalent_decimal_multiplier_valid(self):
        r=row();r['details']['shares_per_contract']=100.0
        self.assertTrue(compute([r])['rows'][0]['identity_eligible'])
    def test_mixed_bar_dates_stay_separate(self):
        old=row('call',100);recent=row('put',101);old['day']['last_updated']=1717099200000000000
        d=compute([old,recent]);self.assertEqual(len(d['daily_bar_update_groups']),2)
        self.assertIsNone(d['total_session_volume'])
        for g in d['daily_bar_update_groups']:self.assertFalse(g['market_session_completeness_verified'])
    def test_negative_gamma_preserved_but_unqualified(self):
        r=row();r['greeks']['gamma']=-.00001
        cell=compute([r])['rows'][0]['metrics']['vendor_gamma']
        self.assertEqual(cell['reported_value'],'-0.00001');self.assertIsNone(cell['value']);self.assertEqual(cell['state'],'outside_domain')
    def test_zero_gamma_is_valid(self):
        r=row();r['greeks']['gamma']=0
        self.assertEqual(compute([r])['rows'][0]['metrics']['vendor_gamma']['value'],'0')
    def test_zero_ratio_numerator_and_denominator_are_distinct(self):
        c=row();p=row('put');p['open_interest']=3
        d=compute([c,p]);self.assertEqual(d['reported_open_interest']['call_put_ratio']['value'],'0')
        p['open_interest']=0
        ratio=compute([c,p])['reported_open_interest']['call_put_ratio']
        self.assertIsNone(ratio['value']);self.assertEqual(ratio['status'],'zero_denominator')
    def test_ratio_requires_all_field_values_in_captured_population(self):
        c=row();p=row('put');del p['open_interest']
        ratio=compute([c,p])['reported_open_interest']['call_put_ratio']
        self.assertIsNone(ratio['value']);self.assertEqual(ratio['status'],'incomplete_field_coverage')
    def test_no_fake_oi_or_greek_clock_from_day(self):
        d=compute([row()])['rows'][0]
        self.assertEqual(d['clocks']['daily_bar_updated']['raw_nanoseconds'],'1789759800000000001')
        self.assertIsNone(d['metrics']['vendor_gamma']['observed_at']);self.assertIsNone(d['open_interest_date'])
        self.assertIsNone(d['iv_observation_date']);self.assertIsNone(d['greek_observation_date'])
    def test_future_nanosecond_rejected_even_below_microsecond_precision(self):
        r=row();r['day']['last_updated']=1789992000000000001
        cell=compute([r])['rows'][0]['clocks']['daily_bar_updated']
        self.assertEqual(cell['state'],'future_timestamp');self.assertIsNone(cell['value'])
    def test_absent_and_fractional_quantities_not_zero(self):
        r=row();r['day']['volume']=.5;r['open_interest']=False
        cells=compute([r])['rows'][0]['metrics']
        self.assertEqual(cells['daily_volume']['state'],'non_integer_quantity')
        self.assertEqual(cells['open_interest']['state'],'invalid_number')
    def test_precision_and_exponent_bound(self):
        for value in (Decimal('1e-1000000'),Decimal('1e1000000'),Decimal('1.'+'2'*129)):
            with self.subTest(value=str(value)[:50]),self.assertRaises(ValueError):model.text(value)
        self.assertEqual(model.text(Decimal('0e-1000000')),'0')
        self.assertEqual(model.text(Decimal('0.1234567890123456789')),'0.1234567890123456789')
    def test_crossed_quote_does_not_qualify(self):
        r=row();r['last_quote']={'bid':2,'ask':1}
        m=compute([r])['rows'][0]['metrics'];self.assertEqual(m['bid']['state'],'crossed_quote');self.assertIsNone(m['ask']['value'])
    def test_page_row_reference_exact(self):
        d=compute([row('call'),row('put')]);ref=d['rows'][1]['evidence']
        self.assertEqual(ref['page'],1);self.assertEqual(ref['row_index'],1);self.assertEqual(ref['row_pointer'],'/results/1')
    def test_expired_and_malformed_rows_preserved(self):
        r=row();r['details'].update(ticker='O:SPY260918C00100000',expiration_date='2026-09-18')
        d=compute([r,None]);self.assertEqual(d['coverage']['returned_rows'],2)
        self.assertIn('expired_before_capture_date',d['rows'][0]['identity_reasons'])
        self.assertEqual(d['rows'][1]['identity_reasons'],['non_object_contract'])
    def test_noncanonical_expiry_cannot_self_qualify(self):
        r=row();r['details']['expiration_date']='20260925'
        self.assertFalse(compute([r])['rows'][0]['identity_eligible'])
    def test_two_pages_keep_zero_based_row_positions(self):
        cursor='https://api.polygon.io/v3/snapshot/options/SPY?cursor=two'
        pages=[page([row()],next_url=cursor),page([row('put')],index=2,url=cursor)]
        d=model.compile_rows('SPY',pages,True)
        self.assertEqual([r['evidence']['row_index'] for r in d['rows']],[0,0])
        self.assertEqual([r['evidence']['page'] for r in d['rows']],[1,2])

if __name__=='__main__':unittest.main()
