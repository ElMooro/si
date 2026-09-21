"""Profile reconstruction must preserve identity, clocks, units and source failures."""
from pathlib import Path
from decimal import Decimal
import copy, json, sys, unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import etf_profile_native as native
AT = '2026-09-21T10:00:00+00:00'
ACQUIRED = '2026-09-21T09:00:00+00:00'
PROCESSED = '2026-09-18'


def row(**kw):
    return {'composite_ticker': 'VOO', 'processed_date': PROCESSED, 'effective_date': PROCESSED,
        'description': 'Vanguard S&P 500 ETF', 'num_holdings': 515, 'net_expenses': 0.03,
        'management_fee': 0.02, 'aum': 0, 'levered_amount': 0,
        'sector_exposure': {'technology': 0.4, 'other': 0.6}, **kw}


def fixture(rows=None, selected=None, following=None, status='complete_returned_profile_snapshot'):
    rows = [row()] if rows is None else rows
    selected = rows[:1] if selected is None else selected
    objects = {}
    def save(doc):
        raw = json.dumps(doc, separators=(',', ':')).encode()
        ref = {'key': native.PRIVATE + native.sha(raw) + '.bin', 'sha256': native.sha(raw), 'bytes': len(raw)}
        objects[ref['key']] = raw; return ref
    selection = {'url': native.selection_url('VOO', '2026-09-21'), 'acquired_at': ACQUIRED,
        'original': save({'status': 'OK', 'results': selected, 'count': len(selected)})}
    doc = {'status': 'OK', 'results': rows, 'count': len(rows)}
    if following: doc['next_url'] = following
    collection = {'ticker': 'VOO', 'cutoff': '2026-09-21', 'status': status,
        'selection': selection, 'pages': [{'url': native.snapshot_url('VOO', PROCESSED), 'acquired_at': ACQUIRED, 'original': save(doc)}]}
    return collection, objects, save


def reconstruct(rows=None, **kw):
    collection, objects, _ = fixture(rows, **kw)
    return native.reconstruct(collection, objects.__getitem__, AT)


class ProfileMeasurements(unittest.TestCase):
    def test_profile_dates_and_fees_do_not_replace_other_concepts(self):
        result = reconstruct(); profile = result['profiles'][0]
        self.assertEqual(profile['effective_date'], PROCESSED)
        self.assertEqual(profile['dates_apply_to'], 'profile_only')
        self.assertFalse(profile['holdings_effective_date_inferred'])
        self.assertFalse(result['quality']['current_holdings_confirmed'])
        values = profile['numeric']
        self.assertEqual(values['net_expenses']['value_decimal'], '0.03')
        self.assertEqual(values['management_fee']['value_decimal'], '0.02')
        self.assertFalse(values['net_expenses']['unit_certified'])
        self.assertEqual(values['total_expenses']['status'], 'missing')
        self.assertIsNone(values['total_expenses']['value_decimal'])
        self.assertTrue(values['num_holdings']['unit_certified'])
        self.assertEqual(values['num_holdings']['value_decimal'], '515')
        self.assertTrue(result['quality']['current_profile_eligible'])

    def test_zero_null_and_absent_are_different(self):
        result = reconstruct([row(net_expenses=None)]); values = result['profiles'][0]['numeric']
        self.assertEqual(values['aum']['value_decimal'], '0')
        self.assertEqual(values['levered_amount']['value_decimal'], '0')
        self.assertEqual(values['net_expenses']['status'], 'null')
        self.assertTrue(values['net_expenses']['present'])
        self.assertEqual(values['fee_waivers']['status'], 'missing')
        self.assertFalse(values['fee_waivers']['present'])
        self.assertFalse(values['aum']['unit_certified'])

    def test_exact_decimal_and_field_provenance(self):
        c, objects, save = fixture([row(num_holdings=9007199254740993)])
        # The integer remains exact even though it exceeds the reviewed count domain.
        result = native.reconstruct(c, objects.__getitem__, AT); num = result['profiles'][0]['numeric']['num_holdings']
        self.assertEqual(num['raw_decimal'], '9007199254740993')
        self.assertIsNone(num['value_decimal']); self.assertEqual(num['status'], 'invalid')
        self.assertEqual(num['source']['sha256'], c['pages'][0]['original']['sha256'])
        self.assertEqual((num['source']['row_index'],num['source']['field']),(0,'num_holdings'))
        self.assertFalse(result['quality']['current_profile_eligible'])

    def test_invalid_field_retains_other_fields_without_zero(self):
        for value in (True, '0.03', -1, 2.5):
            with self.subTest(value=value):
                result = reconstruct([row(num_holdings=value)])
                profile = result['profiles'][0]
                self.assertEqual(profile['numeric']['num_holdings']['status'], 'invalid')
                self.assertIsNone(profile['numeric']['num_holdings']['value_decimal'])
                self.assertEqual(profile['numeric']['net_expenses']['value_decimal'], '0.03')
                self.assertFalse(result['quality']['current_profile_eligible'])

    def test_numeric_exponent_and_precision_bounds(self):
        for value in (Decimal('1e-1000000'), Decimal('1e31'), Decimal('1.'+'2'*128)):
            num = native.numeric({'aum':value}, 'aum', {})
            self.assertEqual(num['status'], 'invalid'); self.assertIsNone(num['value_decimal'])
        self.assertEqual(native.numeric({'aum':Decimal('0e-1000000')},'aum',{})['value_decimal'],'0')
        self.assertEqual(native.numeric({'management_fee':Decimal('0.030000000000000000001')},'management_fee',{})['value_decimal'],'0.030000000000000000001')

    def test_exposure_totals_are_never_normalized(self):
        for values, expected in [({'long':3.1,'cash':0.071},'3.171'),({'usd':0.923},'0.923'),({'bond':0},'0')]:
            result = reconstruct([row(currency_exposure=values)])['profiles'][0]['exposures']['currency_exposure']
            self.assertEqual(Decimal(result['raw_observed_sum_decimal']),Decimal(expected))
            self.assertFalse(result['normalized']);self.assertFalse(result['unit_certified'])
            self.assertFalse(result['portfolio_weight_eligible'])
            self.assertEqual(len(result['entries']),len(values))

    def test_array_exposures_preserve_every_field_without_guessing_weights(self):
        value=[{'name':'cash','value':0,'weight':0.2},{'bucket':'swap','value':3.1}]
        exposure=reconstruct([row(currency_exposure=value)])['profiles'][0]['exposures']['currency_exposure']
        self.assertEqual(exposure['status'],'reported_array_unqualified_schema')
        self.assertEqual(len(exposure['raw_structure']['items']),2)
        self.assertEqual(exposure['raw_structure']['items'][0]['fields']['value']['decimal'],'0')
        self.assertEqual(exposure['raw_structure']['items'][1]['fields']['value']['decimal'],'3.1')
        self.assertIsNone(exposure['raw_observed_sum_decimal'])
        self.assertFalse(exposure['portfolio_weight_eligible'])

    def test_invalid_exposure_is_visible_not_complete_zero(self):
        for value in ('unknown', {'cash':None,'equity':'0.9'}, True):
            result=reconstruct([row(currency_exposure=value)])
            self.assertIn('currency_exposure',result['profiles'][0]['field_errors'])
            self.assertFalse(result['quality']['current_profile_eligible'])
            self.assertIsNone(result['profiles'][0]['exposures']['currency_exposure']['raw_observed_sum_decimal'])

    def test_unknown_fields_are_inventoried_not_used_for_inference(self):
        result=reconstruct([row(new_method={'score':99},future_score=0)])['profiles'][0]
        self.assertEqual(result['additional_field_types'],{'new_method':'object','future_score':'number'})
        self.assertNotIn('future_score',result['numeric'])

    def test_structure_depth_and_size_are_bounded(self):
        deep={};node=deep
        for _ in range(14):node['next']={};node=node['next']
        for value in (deep,list(range(20001)),'x'*8193):
            with self.assertRaises(native.SourceRejected):native.tree(value)


class ProfileAcquisition(unittest.TestCase):
    def test_duplicate_complete_profiles_remain_ambiguous(self):
        result=reconstruct([row(),row()])
        self.assertEqual(result['quality']['status'],'ambiguous_profiles')
        self.assertEqual(len(result['profiles']),2)
        self.assertIsNone(result['selected_profile'])
        self.assertFalse(result['quality']['current_profile_eligible'])
        self.assertNotEqual(result['profiles'][0]['row_id'],result['profiles'][1]['row_id'])

    def test_failed_followup_retains_partial_rows_and_cannot_claim_complete(self):
        url=native.ENDPOINT+'?cursor=second'
        result=reconstruct(following=url,status='provider_http_error')
        self.assertEqual(len(result['profiles']),1)
        self.assertEqual(result['quality']['status'],'incomplete')
        self.assertFalse(result['quality']['pagination_complete'])
        self.assertIsNone(result['selected_profile'])
        with self.assertRaises(ValueError):reconstruct(following=url)

    def test_chain_must_match_next_url_and_stop_at_complete_page(self):
        c,objects,save=fixture(following=native.ENDPOINT+'?cursor=second')
        c['pages'].append({'url':native.ENDPOINT+'?cursor=wrong','acquired_at':ACQUIRED,'original':save({'status':'OK','results':[]})})
        with self.assertRaises(ValueError):native.reconstruct(c,objects.__getitem__,AT)
        c['pages'][-1]['url']=native.ENDPOINT+'?cursor=second'
        self.assertTrue(native.reconstruct(c,objects.__getitem__,AT)['quality']['pagination_complete'])
        c['pages'].append(copy.deepcopy(c['pages'][-1]))
        with self.assertRaises(ValueError):native.reconstruct(c,objects.__getitem__,AT)

    def test_identity_date_order_and_selection_race_reject(self):
        for rows in ([row(composite_ticker='SPY')],[row(effective_date='2026-09-19')],[row(processed_date='2026-09-17')]):
            c,objects,_=fixture(rows,selected=[row()])
            with self.assertRaises(ValueError):native.reconstruct(c,objects.__getitem__,AT)
        with self.assertRaisesRegex(native.SourceRejected,'changed'):
            reconstruct([row(num_holdings=514)],selected=[row()])

    def test_source_and_compilation_clocks_are_ordered(self):
        c,objects,_=fixture()
        for at in ('2026-09-21T08:00:00Z','2026-09-20T10:00:00Z'):
            with self.assertRaises(ValueError):native.reconstruct(c,objects.__getitem__,at)
        c['pages'][0]['acquired_at']='2026-09-21T08:59:00Z'
        with self.assertRaises(ValueError):native.reconstruct(c,objects.__getitem__,AT)

    def test_profile_date_never_implies_constituents_date(self):
        result=reconstruct([row(effective_date='2026-08-31')])
        self.assertEqual(result['effective_date'],'2026-08-31')
        self.assertEqual(result['quality']['effective_age_days'],{'2026-08-31':21})
        self.assertFalse(result['profiles'][0]['holdings_effective_date_inferred'])
        c,objects,_=fixture()
        later=native.reconstruct(c,objects.__getitem__,'2026-09-23T10:00:00Z')
        self.assertTrue(later['quality']['source_check_overdue'])
        self.assertFalse(later['quality']['current_profile_eligible'])

    def test_absent_selection_cannot_hide_rows(self):
        c,objects,_=fixture();c.update(selection=None,status='credential_unavailable')
        with self.assertRaises(ValueError):native.reconstruct(c,objects.__getitem__,AT)
        c['pages']=[]
        result=native.reconstruct(c,objects.__getitem__,AT)
        self.assertEqual(result['quality']['status'],'unavailable')
        self.assertEqual(result['profiles'],[])

    def test_bad_provider_content_is_quarantined_but_integrity_failures_raise(self):
        c,objects,_=fixture([row(effective_date='invalid')])
        result=native.reconstruct_or_reject(c,objects.__getitem__,AT)
        self.assertEqual(result['quality']['status'],'source_rejected')
        self.assertEqual(result['retained_attempt'],c)
        self.assertFalse(result['quality']['current_profile_eligible'])
        objects[c['selection']['original']['key']]+=b' '
        with self.assertRaisesRegex(ValueError,'bytes differ'):native.reconstruct_or_reject(c,objects.__getitem__,AT)

    def test_original_integrity_and_duplicate_json_fields(self):
        c,objects,save=fixture();ref=c['pages'][0]['original'];objects[ref['key']]+=b' '
        with self.assertRaisesRegex(ValueError,'bytes differ'):native.original(ref,objects.__getitem__)
        for raw in (b'{"status":"OK","results":[],"results":[]}',b'{"status":"OK","results":[],"count":1}',b'{"status":"OK","results":[{"aum":1e9999999999999999999999999999}]}'):
            ref={'key':native.PRIVATE+native.sha(raw)+'.bin','sha256':native.sha(raw),'bytes':len(raw)}
            with self.assertRaises(native.SourceRejected):native.original(ref,lambda _:raw)

    def test_unauthorized_url_credentials_and_query_are_rejected(self):
        for url in ('http://api.polygon.io/etf-global/v1/profiles?cursor=x',native.ENDPOINT+'?apiKey=secret',native.ENDPOINT+'?cursor=x&cursor=y','https://api.polygon.io.evil.test/etf-global/v1/profiles?cursor=x','https://user:password@api.polygon.io/etf-global/v1/profiles?cursor=x',native.ENDPOINT+'?cursor=x#ignored',native.ENDPOINT+'?unknown=1'):
            with self.subTest(url=url),self.assertRaises(ValueError):native.next_url(url)
        self.assertEqual(native.next_url(native.ENDPOINT+'?cursor=x'),native.ENDPOINT+'?cursor=x')


if __name__=='__main__':unittest.main(verbosity=2)
