"""Offline regression fixtures follow the repository's Symdir dataset reader.

Fixture values are synthetic; this suite does not claim live warehouse proof.
"""
import gzip
import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / "source"
spec = importlib.util.spec_from_file_location("ofr_funding", SOURCE / "ofr_funding.py")
funding = importlib.util.module_from_spec(spec)
spec.loader.exec_module(funding)
NOW = "2026-09-12T12:00:00+00:00"


def dataset():
    return {
        "as_of": "2026-09-11T16:00:00Z",
        "source_url": "https://example.test/ofr-fixture",
        "raw_snapshot_key": "raw/ofr/test-fixture.json",
        "payload": {"timeseries": {
            mnemonics[0]: {
                "metadata": {"unit": "USD billions" if field == "triparty_volume"
                             else "Percent"},
                "timeseries": {"aggregation": [["2026-09-09", 1.2],
                                                ["2026-09-11", 1.5],
                                                ["2026-09-10", 1.3]]},
            } for field, mnemonics in funding.FIELDS.items()
        }},
    }


def reader(documents):
    def get(key):
        if key not in documents:
            raise KeyError(key)
        return documents[key]
    return get


class FundingTests(unittest.TestCase):
    def build(self, doc=None, extra=None):
        docs = {funding.DATASET_KEYS[0]: dataset() if doc is None else doc}
        docs.update(extra or {})
        return funding.build_funding(reader(docs), NOW)

    def test_bulk_dataset_and_complete_provenance(self):
        out = self.build()
        self.assertEqual(out["available_fields"], 5)
        for field in funding.FIELDS:
            value = out[field]
            self.assertEqual(value["value"], 1.5)
            self.assertEqual(value["as_of"], "2026-09-11")
            self.assertEqual(value["source"]["fetched_at"], "2026-09-11T16:00:00Z")
            self.assertEqual(value["source"]["raw_snapshot_key"], funding.DATASET_KEYS[0])
            self.assertEqual(value["source"]["provider"], "ofr")
            self.assertFalse(value["data_unavailable"])
            self.assertFalse(value["stale"])

    def test_latest_dated_valid_number_not_last_row(self):
        point = funding.latest({"aggregation": [
            ["2026-09-11", "0"], ["2026-09-12", None],
            ["2026-09-12", "NaN"], ["2026-09-12", True],
            ["2026-09-12", "inf"], ["2027-01-01", 9],
            ["2026-09-09", 2], ["bad-date", 5],
        ]}, "2026-09-12")
        self.assertEqual(point, ("2026-09-11", 0.0))

    def test_known_compact_containers(self):
        for node in ({"last_observation": ["2026-09-11", "2.4"]},
                     {"2026-09-11": "2.4"},
                     {"observations": [{"date": "2026-09-11", "value": "2.4"}]}):
            with self.subTest(node=node):
                self.assertEqual(funding.latest(node, "2026-09-12"),
                                 ("2026-09-11", 2.4))

    def test_fresher_exact_series_wins_over_dataset(self):
        mnemonic = funding.FIELDS["sofr"][0]
        key = f"data/warm/ofr/series/{mnemonic}.json.gz"
        out = self.build(extra={key: {"mnemonic": mnemonic,
            "as_of": "2026-09-12T11:00:00Z",
            "payload": {"timeseries": {"aggregation": [["2026-09-12", 2.2]]}}}})
        self.assertEqual(out["sofr"]["value"], 2.2)
        self.assertEqual(out["sofr"]["raw_snapshot_key"], key)

    def test_no_ex_fed_or_overnight_substitution(self):
        doc = dataset()
        series = doc["payload"]["timeseries"]
        series["REPO-TRIV1_AR_TOT-P"] = series.pop("REPO-TRI_AR_TOT-P")
        series["REPO-DVP_AR_OO-P"] = series.pop("REPO-DVP_AR_TOT-P")
        out = self.build(doc)
        self.assertTrue(out["triparty_rate"]["data_unavailable"])
        self.assertTrue(out["dvp_rate"]["data_unavailable"])
        self.assertEqual(out["available_fields"], 3)

    def test_observed_ofr_unit_objects_and_keyed_single_series(self):
        doc = dataset()
        for field, mnemonics in funding.FIELDS.items():
            doc["payload"]["timeseries"][mnemonics[0]]["metadata"]["unit"] = {
                "type": "Volume" if field == "triparty_volume" else "Rate",
                "magnitude": 0, "display_magnitude": 0,
                "name": "USD" if field == "triparty_volume" else "Percent",
                "precision": 2}
        sofr = doc["payload"]["timeseries"].pop("FNYR-SOFR-A")
        sofr["timeseries"]["aggregation"] = [["2026-08-05", 1.5]]
        out = self.build(doc, extra={"data/warm/ofr/series/FNYR-SOFR-A.json.gz": {
            "mnemonic": "FNYR-SOFR-A", "as_of": "2026-08-06T03:06:03+00:00",
            "payload": {"FNYR-SOFR-A": sofr}}})
        self.assertEqual(out["available_fields"], 5)
        self.assertEqual(out["triparty_volume"]["unit"], "USD")
        self.assertEqual(out["sofr"]["unit"], "Percent")
        self.assertEqual(out["sofr"]["source"]["fetched_at"], "2026-08-06T03:06:03+00:00")
        self.assertEqual(out["stale_fields"], ["sofr"])
        self.assertEqual(out["fresh_fields"], 4)

    def test_unreviewed_unit_scale_fails_closed(self):
        doc = dataset()
        doc["payload"]["timeseries"]["REPO-TRI_TV_TOT-P"]["metadata"]["unit"] = {
            "name": "USD", "magnitude": 6}
        self.assertTrue(self.build(doc)["triparty_volume"]["data_unavailable"])

    def test_newer_primary_sofr_preserves_nyfed_provenance(self):
        doc = dataset()
        doc["payload"]["timeseries"]["FNYR-SOFR-A"]["timeseries"] = {
            "aggregation": [["2026-08-04", 3.66]]}
        nyfed = {"rate": "sofr", "observations": [
            {"date": "2026-09-10", "rate": 3.62}],
            "source_url": "https://example.test/nyfed-fixture",
            "raw_snapshot_key": "raw/nyfed/fixture.json",
            "_warehouse_last_modified": "2026-09-11T01:00:00+00:00"}
        out = self.build(doc, {funding.NYFED_SOFR_KEY: nyfed})
        self.assertEqual(out["sofr"]["value"], 3.62)
        self.assertEqual(out["sofr"]["as_of"], "2026-09-10")
        self.assertEqual(out["sofr"]["provider"], "nyfed")
        self.assertEqual(out["sofr"]["series"], "SOFR")
        self.assertEqual(out["sofr"]["source"]["raw_snapshot_key"], funding.NYFED_SOFR_KEY)
        self.assertEqual(out["sofr"]["source"]["fetched_at_basis"], "warehouse_last_modified")
        self.assertEqual(out["sofr"]["source"]["fetched_at"], nyfed["_warehouse_last_modified"])
        self.assertEqual(out["fresh_fields"], 5)

    def test_another_nyfed_rate_cannot_replace_sofr(self):
        out = self.build(extra={funding.NYFED_SOFR_KEY: {
            "rate": "effr", "observations": [{"date": "2026-09-12", "rate": 9.9}]}})
        self.assertEqual(out["sofr"]["provider"], "ofr")
        self.assertEqual(out["sofr"]["value"], 1.5)

    def test_older_nyfed_copy_does_not_replace_newer_ofr(self):
        out = self.build(extra={funding.NYFED_SOFR_KEY: {
            "rate": "sofr", "observations": [{"date": "2026-08-01", "rate": 9.9}]}})
        self.assertEqual(out["sofr"]["provider"], "ofr")
        self.assertEqual(out["sofr"]["as_of"], "2026-09-11")

    def test_volume_unit_must_be_known(self):
        doc = dataset()
        del doc["payload"]["timeseries"]["REPO-TRI_TV_TOT-P"]["metadata"]
        out = self.build(doc)
        self.assertIsNone(out["triparty_volume"]["value"])
        self.assertTrue(out["triparty_volume"]["data_unavailable"])

    def test_current_nested_metadata_units(self):
        doc = dataset()
        doc["payload"]["timeseries"]["REPO-TRI_TV_TOT-P"]["metadata"] = {
            "description": {"units": "Billions of U.S. Dollars"}}
        out = self.build(doc)
        self.assertEqual(out["triparty_volume"]["unit"], "Billions of U.S. Dollars")
        self.assertEqual(out["triparty_volume"]["value"], 1.5)

    def test_stale_observation_keeps_its_date(self):
        doc = dataset()
        doc["as_of"] = NOW
        doc["payload"]["timeseries"]["FNYR-SOFR-A"]["timeseries"] = {
            "aggregation": [["2026-07-01", 2.1]]}
        out = self.build(doc)
        self.assertEqual(out["sofr"]["as_of"], "2026-07-01")
        self.assertTrue(out["sofr"]["stale"])
        self.assertEqual(out["sofr"]["source"]["kind"], "cache-stale")

    def test_missing_values_are_not_counted_or_assigned_now(self):
        out = funding.build_funding(reader({}), NOW)
        self.assertEqual(out["available_fields"], 0)
        for field in funding.FIELDS:
            self.assertIsNone(out[field]["value"])
            self.assertIsNone(out[field]["as_of"])
            self.assertTrue(out[field]["data_unavailable"])
        self.assertLessEqual(len(out["warehouse_read_errors"]), 9)

    def test_series_identity_mismatch_is_rejected(self):
        key = "data/warm/ofr/series/FNYR-SOFR-A.json.gz"
        out = self.build(doc={}, extra={key: {
            "mnemonic": "FNYR-EFFR-A", "payload": [["2026-09-11", 9]]}})
        self.assertTrue(out["sofr"]["data_unavailable"])

    def test_ofr_only_handler_with_real_provenance_module(self):
        writes, reads = [], []
        compressed = gzip.compress(json.dumps(dataset()).encode())

        class FakeS3:
            def get_object(self, Bucket, Key):
                reads.append(Key)
                if Key == funding.DATASET_KEYS[0]:
                    return {"Body": io.BytesIO(compressed)}
                raise KeyError(Key)

            def put_object(self, **kwargs):
                writes.append(kwargs)

        fake_boto = types.SimpleNamespace(client=lambda *a, **k: FakeS3())
        shared = SOURCE.parents[2] / "shared"
        with patch.dict(sys.modules, {"boto3": fake_boto, "ofr_funding": funding}), \
                patch.object(sys, "path", [str(shared)] + sys.path):
            bridge_spec = importlib.util.spec_from_file_location("bridge_under_test",
                                                               SOURCE / "lambda_function.py")
            bridge = importlib.util.module_from_spec(bridge_spec)
            bridge_spec.loader.exec_module(bridge)
            self.assertIsNotNone(bridge._lib_wrap)
            response = bridge.lambda_handler({"feed": "ofr"}, None)
        self.assertEqual(json.loads(response["body"])["wrapped"]["ofr"], 5)
        self.assertEqual([write["Key"] for write in writes], ["data/ofr-funding.json"])
        self.assertLessEqual(len(reads), 9)
        self.assertEqual(len(reads), len(set(reads)))
        self.assertTrue(all(key.startswith("data/warm/ofr/") or key == funding.NYFED_SOFR_KEY
                            for key in reads))


if __name__ == "__main__":
    unittest.main()
