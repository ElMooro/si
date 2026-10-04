import copy
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import reviewed_symbol_config as config
import warehouse_routing as routing


class ConfigTests(unittest.TestCase):
    def test_exact_packaged_bytes_and_no_equivalence_claim(self):
        raw=Path(config.__file__).with_name("tv-symbol-resolver.json").read_bytes()
        self.assertEqual(config.reviewed_resolver(),json.loads(raw))
        self.assertEqual(config.evidence()["sha256"],hashlib.sha256(raw).hexdigest())
        self.assertFalse(config.evidence()["equivalence_verified"])
        self.assertFalse(config.evidence()["history_verified"])

    def test_caller_mutation_cannot_change_next_request(self):
        d=config.reviewed_resolver();d["licensed_econ_skip"].clear();d["exact"].clear()
        self.assertTrue(config.reviewed_resolver()["licensed_econ_skip"])
        self.assertTrue(config.reviewed_resolver()["exact"])

    def test_unavailable_and_malformed_rules_fail_closed(self):
        original=config.reviewed_resolver()
        for field,value in [("schema","unknown"),("prefix",None),("exact",[]),("licensed_econ_skip",None),("computed_internals",None)]:
            d=copy.deepcopy(original);d[field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):config.validate(d)
        with self.assertRaises(ValueError):config.unique_object([("exact",{}),("exact",{})])

    def test_reviewed_rules_do_not_fetch_denied_public_configuration(self):
        def read(key):
            self.assertEqual(key,"data/symbol-map.json")
            return {"map":{}}
        self.assertEqual(routing.resolved_id("TVC:US10Y",read),"fred:DGS10")
        self.assertEqual(routing.resolved_id("NASDAQ:AAPL",read),"AAPL")
        self.assertEqual(routing.resolved_id("OANDA:EURUSD",read),"C:EURUSD")

    def test_all_existing_licensed_and_intraday_holds_remain(self):
        read=Mock(return_value={"map":{}})
        for key in config.reviewed_resolver()["licensed_econ_skip"]+["USI:TICKBA.DJ","USI:TICKBA.US"]:
            with self.subTest(key=key):self.assertEqual(routing.resolved_id(key,read),"SKIP")

    def test_runtime_map_unavailable_does_not_enable_vendor_fallback(self):
        for document in [None,[],{},{"map":[]},{"map":None}]:
            with self.subTest(document=document),self.assertRaisesRegex(ValueError,"refusing TV fallback"):
                routing.resolved_id("FTSE:FTUSPLUT",lambda _:document)

    def test_runtime_map_read_failure_propagates(self):
        with self.assertRaisesRegex(RuntimeError,"denied"):
            routing.resolved_id("FTSE:FTUSPLUT",Mock(side_effect=RuntimeError("denied")))

    def test_runtime_exact_mapping_and_unmapped_original_remain_distinct(self):
        read=lambda _:{"map":{"ECONOMICS:USM0":{"source":"FRED","id":"BOGMBASE"}}}
        self.assertEqual(routing.resolved_id("ECONOMICS:USM0",read),"fred:BOGMBASE")
        self.assertIsNone(routing.resolved_id("FTSE:FTUSPLUT",read))

    def test_packaged_configuration_failure_is_not_silently_ignored(self):
        with patch.object(config,"reviewed_resolver",side_effect=ValueError("invalid package")),self.assertRaisesRegex(ValueError,"invalid package"):
            routing.resolved_id("FTSE:FTUSPLUT",lambda _:{"map":{}})


if __name__=="__main__":
    unittest.main()
