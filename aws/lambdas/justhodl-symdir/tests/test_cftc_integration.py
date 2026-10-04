"""CFTC routing in the actual Lambda; all AWS/provider calls replaced."""
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import Mock,patch
from test_universal_provider_search import load_lambda,SYMDIR,FakeS3
import cftc_series

SID='cftc:yw9f-hn96|132741|traders_asset_mgr_short_all'

class CFTCIntegrationTests(unittest.TestCase):
    def module(self):
        m=load_lambda('cftc_integration',SYMDIR,FakeS3())
        m._cache_get=Mock(return_value=(None,None));m._cache_put=Mock()
        m._doc_lookup=Mock(side_effect=AssertionError('Unrelated directory read'))
        m.closest_ids=Mock(side_effect=AssertionError('Fuzzy substitution forbidden'))
        m.load_index=Mock(side_effect=AssertionError('Unrelated index read'))
        return m

    def test_alias_uses_exact_canonical_adapter_before_legacy_symbol_map(self):
        m=self.module();packet={'id':SID,'obs':[['2026-01-06',9]],'n':1}
        with patch.object(cftc_series,'fetch',return_value=packet) as fetch:
            result=m.fetch_series('COT3:132741_FO_TAM_S')
        self.assertEqual(result['id'],SID);fetch.assert_called_once_with(SID)
        m._doc_lookup.assert_not_called();m.closest_ids.assert_not_called()

    def test_error_cannot_fall_through_to_another_contract(self):
        m=self.module()
        with patch.object(cftc_series,'fetch',side_effect=ValueError('Exact provider denied')):
            for requested in [SID,'COT3:132741_FO_TAM_S']:
                with self.assertRaisesRegex(ValueError,'Exact provider denied'):m.fetch_series(requested)
        m.closest_ids.assert_not_called();m._cache_put.assert_not_called()

    def test_directories_work_without_producer_build_or_application_storage(self):
        m=self.module();found=m.explorer({'provider':'cftc','q':'132741','limit':'2','offset':'2'})
        self.assertEqual(len(found['rows']),2);self.assertEqual(found['offset'],2)
        for q in ['COT3:132741_FO_TAM_S',SID]:
            found=m.search(q);self.assertEqual(found['rows'][0]['id'],SID)
        self.assertEqual(m.search('132741',prov='cftc',kind='dataset')['rows'],[])
        m.load_index.assert_not_called()

    def test_legacy_alias_cache_cannot_supply_another_metric(self):
        m=load_lambda('cftc_alias_cache',SYMDIR,FakeS3())
        m.s3=Mock()
        for sid in ['COT3:132741_FO_TAM_S','COT:132741_FO_NCP_L','cot2:unknown']:
            self.assertEqual(m._cache_get(sid,86400),(None,None))
        m.s3.get_object.assert_not_called()

    def test_bundled_twin_client_and_official_schema_agree(self):
        root=Path(__file__).resolve().parents[4]
        native=SYMDIR.parent/'cftc-series.json';twin=SYMDIR.parent.parent/'config/cftc-series.json'
        self.assertEqual(native.read_bytes(),twin.read_bytes())
        self.assertEqual(hashlib.sha256(native.read_bytes()).hexdigest(),cftc_series.CATALOG_HASH)
        audit=json.loads((root/'docs/audit/2026-10-04/chart-cftc-schema-discovery.json').read_bytes())
        schemas={r['id']:r for r in audit['official_schemas']}
        for ds,report in cftc_series.CATALOG['reports'].items():
            schema=schemas[ds];fields={r['fieldName']:r for r in schema['columns']}
            self.assertEqual(report['schema_sha256'],schema['sha256'])
            for key,field in report['fields'].items():self.assertEqual(field['label'],fields[key]['name'])

if __name__=='__main__':unittest.main()
