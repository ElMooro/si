from pathlib import Path
from copy import deepcopy
import json, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/shared'))
sys.path.insert(0, str(ROOT/'tests'))
import capital_structure_research as model
import capital_structure_source as source
import statement_research_identity as identity
from test_capital_structure_measurements import fixture as rows_fixture


def fixture(mutator=None):
    files = {}
    def retain(value, prefix=source.PRIVATE):
        raw = source.encoded(value); digest = source.sha(raw)
        ref = {'key': prefix+digest+'.bin', 'sha256': digest, 'bytes': len(raw)}
        files[ref['key']] = raw
        return ref
    inventory = retain({'contract': 'share-structure-baseline-inventory.v1', 'all_current_rows_conserved': True,
        'candidate_provider_labels': ['ABC'], 'candidate_provider_label_count': 1,
        'complete_reported_label_occurrences': {'ABC': [{'source_key': 'data/share-flows.json', 'path': ['tickers','ABC']}],
                                               'NOT A SYMBOL': [{'source_key': 'data/stock-valuations.json', 'path': ['rows',0]}]},
        'reported_labels_not_provider_request_eligible': ['NOT A SYMBOL']})
    baseline = retain({'status': 'complete', 'inventory': inventory})
    plan = retain({'contract': 'capital-structure-source-plan.v1', 'reported_symbols': ['ABC'], 'planned_sources': 7,
        'requests_per_batch': 2000, 'batches': 1, 'generated_at': '2026-09-25T11:00:00Z', 'baseline': baseline})
    captures = {}; counts = dict.fromkeys(('complete_sources','provider_requests','reused_sources','provider_rows','provider_bytes','empty_arrays'),0)
    all_specs = [source.spec('ABC', e, p) for e,p in
        [(e,p) for p in ('annual','quarter') for e in source.STATEMENTS]+[(e,None) for e in source.SNAPSHOTS]]
    originals = {}
    for request in all_specs:
        endpoint = request['endpoint']
        if endpoint in source.STATEMENTS:
            rows = [deepcopy(rows_fixture()[endpoint]['values'])]
            if request['period'] == 'quarter':
                rows[0].update(date='2026-03-31', fiscalYear='2026', period='Q1',
                               filingDate='2026-05-15', acceptedDate='2026-05-15 12:00:00')
        elif endpoint == 'quote': rows = [{'symbol':'ABC','price':125,'marketCap':10000,'timestamp':1790337600}]
        elif endpoint == 'shares-float': rows = [{'symbol':'ABC','date':'2026-09-24','floatShares':90,'outstandingShares':100,'freeFloat':90}]
        else: rows = [{'symbol':'ABC','date':'2020-01-01','numerator':4,'denominator':1,'splitType':'split'}]
        if mutator: mutator(request, rows)
        # Decimal values travel through original JSON numbers, not binary floats.
        raw = json.dumps(rows, default=lambda v: str(v), sort_keys=True).replace('"10.125"','10.125').encode()
        ref = {'key':source.PRIVATE+source.sha(raw)+'.bin','sha256':source.sha(raw),'bytes':len(raw)}
        files[ref['key']] = raw; originals[request['url']] = ref
        cap = {'status':'response_retained','spec':request,'http_status':200,'original':ref,
            'requested_at':'2026-09-25T11:01:00Z','received_at':'2026-09-25T11:01:01Z','headers':{'content-length':str(len(raw))}}
        captures[request['url']] = retain(cap)
        counts['complete_sources'] += 1; counts['provider_requests'] += 1
        counts['provider_rows'] += len(rows); counts['provider_bytes'] += len(raw); counts['empty_arrays'] += not rows
    batch = retain({'status':'complete','part':1,'plan':plan,'counts':counts,'captures':captures,
        'generated_at':'2026-09-25T11:00:00Z','completed_at':'2026-09-25T11:02:00Z'})
    manifest = retain({'contract':'capital-structure-complete-sources.v1','status':'complete','reported_symbols':['ABC'],
        'plan':plan,'batch_manifests':[batch],'captures':captures,'counts':counts})
    index = {str(i):{'cik_str':i+1000,'ticker':'X'+str(i),'title':'Synthetic '+str(i)} for i in range(1000)}
    index['0'] = {'cik_str':123,'ticker':'ABC','title':'Synthetic fixture issuer'}
    raw_ref = retain(index, identity.source.PRIVATE)
    identity_ref = retain({'url':identity.URL,'http_status':200,'status':'response_retained','original':raw_ref,
        'requested_at':'2026-09-25T10:00:00Z','received_at':'2026-09-25T10:00:01Z','headers':{}}, identity.source.PRIVATE)
    return {'files':files,'manifest':manifest,'identity':identity_ref,'retain':retain,'originals':originals,'specs':all_specs}


def compile_fixture(f):
    return model.compile_output(f['manifest'], f['identity'], f['files'].__getitem__)


class Tests(unittest.TestCase):
    def test_complete_sources_cash_share_counts_and_all_population_origins_are_conserved(self):
        f = fixture(); result = compile_fixture(f); packet, shard = result['packet'], result['shards']['ABC']
        self.assertEqual((packet['reported_names'],packet['provider_responses'],packet['provider_rows']), (1,7,7))
        self.assertEqual(len(shard['records']), 2); self.assertEqual(len(shard['snapshots']), 3)
        self.assertEqual(shard['source_row_count'], 7)
        coords = [(r['capture_id'],r['source_row']) for record in shard['records']+shard['snapshots'] for r in record['source_rows']]
        self.assertEqual(len(set(coords)), 7)
        self.assertEqual(packet['unrequestable_reported_labels'], ['NOT A SYMBOL'])
        self.assertIn('NOT A SYMBOL',packet['unrequestable_label_origins'])
        self.assertEqual(packet['generated_at'],'2026-09-25T11:01:01Z')
        self.assertFalse(packet['sizing_eligible']); self.assertEqual(packet['independent_investment_votes'],0)
        for record in shard['records']:
            self.assertEqual(record['status'],'complete_exact_filing_pair')
            self.assertEqual(record['measurements']['metrics']['net_common_cash_return']['value'],'95.000000000000')
        quote = next(r for r in shard['snapshots'] if r['source_rows'][0]['endpoint']=='quote')
        self.assertIsNone(quote['measurements']); self.assertFalse(quote['joined_to_cash_flows'])
        currency = next(v for v in quote['reported']['facts'] if v['field']=='currency')
        self.assertFalse(currency['present']); self.assertIsNone(currency['numeric_value'])
        split = next(r for r in shard['snapshots'] if r['source_rows'][0]['endpoint']=='splits')
        self.assertFalse(split['measurements']['applied_to_statement_shares'])
        self.assertEqual(split['measurements']['reported_numerator_over_denominator']['value'],'4.000000000000')

    def test_changed_missing_truncated_or_partial_originals_cannot_compile(self):
        for mutate in ('tamper','missing','incomplete_union','wrong_counts'):
            f = fixture()
            if mutate in ('tamper','missing'):
                key = next(iter(f['originals'].values()))['key']
                if mutate == 'tamper': f['files'][key] = b'[]'
                else: del f['files'][key]
            else:
                manifest = json.loads(f['files'][f['manifest']['key']])
                if mutate == 'incomplete_union': manifest['captures'].pop(next(iter(manifest['captures'])))
                else: manifest['counts']['provider_rows'] -= 1
                f['manifest'] = f['retain'](manifest)
            with self.assertRaises((ValueError,KeyError)): compile_fixture(f)

    def test_duplicate_endpoint_is_not_a_convenient_first_row_and_originals_survive(self):
        def duplicate(spec, rows):
            if spec['endpoint']=='cash-flow-statement' and spec['period']=='annual': rows.append(deepcopy(rows[0]))
        result = compile_fixture(fixture(duplicate)); shard = result['shards']['ABC']
        record = next(r for r in shard['records'] if r['request_period']=='annual')
        self.assertEqual(shard['source_row_count'],8)
        self.assertEqual(record['duplicate_endpoints'],['cash-flow-statement'])
        self.assertEqual(len(record['source_rows']),3)
        self.assertIsNone(record['measurements']['metrics']['cash_repurchase_outflow']['value'])
        self.assertEqual(record['measurements']['metrics']['weighted_diluted_over_basic_pct']['value'],'5.000000000000')

    def test_conflicting_cik_and_invalid_metadata_retain_rows_without_company_arithmetic(self):
        def corrupt(spec, rows):
            if spec['endpoint']=='cash-flow-statement' and spec['period']=='annual':
                rows[0]['cik']='999'
            if spec['endpoint']=='income-statement' and spec['period']=='quarter':
                rows[0]['date']={'unexpected':'object'}
        result = compile_fixture(fixture(corrupt)); records = result['shards']['ABC']['records']
        self.assertEqual(result['packet']['provider_rows'],7)
        blocked = [r for r in records if r.get('problem')]
        self.assertEqual(len(blocked),2)
        self.assertTrue(all(r['measurements'] is None for r in blocked))
        original = next(r for r in blocked if r['request_period']=='quarter')['reported_rows'][0]
        self.assertEqual(original['reported_identity']['date']['type'],'object')
        self.assertFalse(result['packet']['quality']['historical_security_continuity_verified'])

    def test_unordered_rows_join_by_filing_identity_not_position(self):
        def reorder(spec, rows):
            if spec['period'] != 'annual': return
            older = deepcopy(rows[0]); older.update(date='2024-12-31',fiscalYear='2024',filingDate='2025-02-15',acceptedDate='2025-02-15 12:00:00')
            if spec['endpoint']=='income-statement': older['revenue']=150; rows.append(older)
            else: older['stockBasedCompensation']=15; rows.insert(0,older)
        result = compile_fixture(fixture(reorder))
        records = [r for r in result['shards']['ABC']['records'] if r['request_period']=='annual']
        self.assertEqual(len(records),2)
        by_year = {r['identity']['fiscalYear']:r['measurements']['metrics']['sbc_to_revenue_pct']['value'] for r in records}
        self.assertEqual(by_year,{'2024':'10.000000000000','2025':'3.375000000000'})

    def test_empty_response_is_preserved_as_unavailable_and_not_a_zero_measurement(self):
        def empty(spec, rows):
            if spec['endpoint']=='cash-flow-statement': rows.clear()
        result = compile_fixture(fixture(empty))
        self.assertEqual(result['packet']['empty_responses'],2)
        self.assertEqual(result['packet']['provider_rows'],5)
        self.assertEqual(len(result['packet']['sources']),7)
        for r in result['shards']['ABC']['records']:
            self.assertIsNone(r['measurements']['metrics']['cash_repurchase_outflow']['value'])


if __name__ == '__main__': unittest.main(verbosity=2)
