import unittest
from provenance import wrap, derive, missing, coverage
from evidence_store import public_source_url


class ProvenanceTests(unittest.TestCase):
    def row(self, value=5, **extra):
        args = dict(field="ICSA", unit="Number", source="fred", series_id="ICSA",
                    url="https://fred.stlouisfed.org/graph/fredgraph.csv?id=ICSA", as_of="2026-09-12")
        args.update(extra)
        return wrap(value, **args)

    def test_unknown_dates_and_confidence_stay_unknown(self):
        row = self.row(as_of=None)
        self.assertIsNone(row["as_of"])
        self.assertIsNone(row["source"]["fetched_at"])
        self.assertIsNone(row["confidence"])
        self.assertIn("observation_period_unknown", row["quality"]["gaps"])

    def test_trace_covers_value_series_unit_and_observation_not_processing_clock(self):
        row = self.row()
        self.assertEqual(row["trace_id"], self.row(received_at="2026-09-18T00:00:00Z")["trace_id"])
        for alternate in (self.row(6), self.row(series_id="IC4WSA"), self.row(unit="Percent"), self.row(as_of="2026-09-05")):
            self.assertNotEqual(row["trace_id"], alternate["trace_id"])
        self.assertEqual(len(row["trace_id"]), 64)

    def test_zero_confidence_is_never_promoted(self):
        row = derive(10, "sum", "a+b", [self.row(confidence=0), self.row()], confidence=1)
        self.assertEqual(row["confidence"], 0)
        self.assertFalse(row["sizing_eligible"])
        self.assertFalse(row["derivation"]["formula_execution_verified"])

    def test_missing_parent_and_mixed_unknown_periods(self):
        row = derive(10, "sum", "a+b", [self.row(), missing("b", "absent")])
        self.assertIsNone(row["value"])
        self.assertIsNone(row["as_of"])
        self.assertEqual(row["derivation"]["missing_inputs"], ["b"])
        row = derive(10, "sum", "a+b", [self.row(), self.row(as_of=None)])
        self.assertIsNone(row["as_of"])

    def test_nonfinite_values_are_not_numbers(self):
        for value in (float("nan"), float("inf"), True, None):
            self.assertTrue(self.row(value)["data_unavailable"])

    def test_full_tree_coverage_is_not_replay_proof(self):
        proof = {"contract": "source-evidence.v1", "captured": True, "key": "test"}
        rows = [self.row(evidence=proof)] + list(range(600))
        result = coverage({"rows": rows})
        self.assertEqual(result["numeric_leaves"], 601)
        self.assertEqual(result["with_provenance"], 1)
        self.assertEqual(result["replay_verified"], 0)
        self.assertTrue(result["scan_complete"])

    def test_urls_preserve_identity_without_credentials(self):
        url = public_source_url("https://user:secret@fred.stlouisfed.org/graph/fredgraph.csv?id=ICSA&api_key=SECRET#x")
        self.assertEqual(url, "https://fred.stlouisfed.org/graph/fredgraph.csv?id=ICSA")
        url = public_source_url("https://apps.bea.gov/api/data/?datasetname=NIPA&TableName=T10101&UserID=SECRET")
        self.assertIn("TableName=T10101", url)
        self.assertNotIn("SECRET", url)

    def test_missing_reason_retained_and_evidence_never_self_certifies(self):
        row = missing("GDP", "upstream timeout")
        self.assertEqual(row["reason"], "upstream timeout")
        self.assertEqual(row["field"], "GDP")
        self.assertIsNone(row["as_of"])
        self.assertFalse(self.row(evidence={"captured": True})["quality"]["replay_verified"])
