import importlib.util
import io
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "source"), str(ROOT.parents[1] / "shared")]
from canary_measurements import DEFINITIONS, csv_observations, diagnostics, curve_movement, observation_quality
from evidence_store import read_verified
from provenance import wrap

NOW = datetime(2026, 9, 18, 12, tzinfo=timezone.utc)


def row(sid, value, period="2026-09-17", prev=None, prev_date="2026-09-16"):
    definition = DEFINITIONS[sid]
    result = wrap(value, sid, unit=definition[0], source="fred", series_id=sid,
                  url="https://fred.stlouisfed.org/series/"+sid, as_of=period)
    result.update(prev=prev, prev_date=prev_date)
    return result


class MemoryS3:
    def __init__(self): self.objects = {}
    def put_object(self, **kw): self.objects[kw["Key"]] = kw
    def get_object(self, **kw):
        obj = self.objects[kw["Key"]]
        return {"Body": io.BytesIO(obj["Body"]), "Metadata": obj.get("Metadata", {})}


def module(s3):
    with patch.dict(sys.modules, {"boto3": types.SimpleNamespace(client=lambda *a, **k: s3)}):
        spec = importlib.util.spec_from_file_location("canary_fixture", ROOT / "source/lambda_function.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod


class CanaryTests(unittest.TestCase):
    def test_csv_order_missing_dates_and_conflicts(self):
        raw = b"observation_date,ICSA\n2026-09-12,196000\n2026-09-05,200000\n2026-09-17,.\n"
        self.assertEqual(csv_observations(raw, "ICSA", NOW.date())[0], ("2026-09-12", 196000))
        for bad in (b"2026-09-12,9\n", b"2026-09-19,9\n", b"2026-09-16,nan\n"):
            with self.assertRaises(ValueError): csv_observations(raw+bad, "ICSA", NOW.date())
        with self.assertRaises(ValueError): csv_observations(raw, "OTHER", NOW.date())

    def test_stale_future_and_unknown_definition_are_not_fresh(self):
        self.assertEqual(observation_quality("ICSA", "2026-09-12", NOW)["status"], "within_age_ceiling")
        self.assertEqual(observation_quality("ICSA", "2026-01-01", NOW)["status"], "stale")
        self.assertEqual(observation_quality("ICSA", "2026-09-19", NOW)["status"], "invalid")
        self.assertEqual(observation_quality("UNKNOWN", "2026-09-17", NOW)["status"], "unknown")

    def test_rrp_billion_scale_is_converted_and_named_as_proxy(self):
        hot = {sid: row(sid, value) for sid, value in {"WALCL": 7_000_000, "WTREGEN": 1_000_000, "WRESBAL": 3_000_000, "RRPONTSYD": 1000, "GDP": 30_000}.items()}
        flags = diagnostics(hot, NOW)
        self.assertEqual(flags["reserve_share_of_fed_liabilities"], 60)
        self.assertEqual(flags["reserve_scarcity_pct"], 10)
        self.assertFalse(flags["sizing_eligible"])
        self.assertIn("not a share", flags["notes"]["reserve_share_of_fed_liabilities"])
        hot["RRPONTSYD"]["unit"] = "Millions"
        self.assertIsNone(diagnostics(hot, NOW)["reserve_share_of_fed_liabilities"])

    def test_curve_uses_changes_even_when_inverted(self):
        hot = {"DGS2": row("DGS2", 4, prev=4.3), "DGS10": row("DGS10", 3.9, prev=4.0)}
        self.assertEqual(curve_movement(hot, NOW), "bull_steepener")
        hot["DGS2"] = row("DGS2", 4.1, prev=4.0)
        hot["DGS10"] = row("DGS10", 4.3, prev=4.0)
        self.assertEqual(curve_movement(hot, NOW), "bear_steepener")
        hot["DGS10"]["prev_date"] = "2026-09-15"
        self.assertIsNone(curve_movement(hot, NOW))

    def test_stale_flags_abstain_and_funding_requires_same_day(self):
        hot = {"SAHMREALTIME": row("SAHMREALTIME", .6, "2020-01-01"), "SOFR": row("SOFR", 4.2), "IORB": row("IORB", 4.0, "2026-09-16")}
        flags = diagnostics(hot, NOW)
        self.assertIsNone(flags["sahm_triggered"])
        self.assertIsNone(flags["floor_breach_bp"])
        self.assertEqual(flags["status"], "UNKNOWN")
        self.assertEqual(flags["_inputs"]["floor_breach_bp"], ["SOFR", "IORB"])

    def test_claims_rise_requires_adjacent_week(self):
        hot = {"IC4WSA": row("IC4WSA", 210000, "2026-09-12", prev=200000, prev_date="2026-09-05")}
        self.assertTrue(diagnostics(hot, NOW)["claims_4wk_rising"])
        hot["IC4WSA"]["prev_date"] = "2026-08-01"
        self.assertIsNone(diagnostics(hot, NOW)["claims_4wk_rising"])

    def test_mixed_frequency_fallback_archives_full_original_responses(self):
        s3 = MemoryS3(); mod = module(s3)
        originals = {"ICSA": b"observation_date,ICSA\n2026-09-12,196000\n", "IC4WSA": b"observation_date,IC4WSA\n2026-09-12,203250\n"}
        mod._fetch = lambda url, **kw: b"PKmixedfrequency" if "," in url else originals[url.split("=")[-1]]
        hot, status = {}, {}
        mod._fred_panel("claims", list(originals), hot, status)
        self.assertEqual(status["claims"]["with_data"], 2)
        self.assertNotEqual(hot["ICSA"]["trace_id"], hot["IC4WSA"]["trace_id"])
        for sid, raw in originals.items():
            self.assertEqual(read_verified(s3, mod.BUCKET, hot[sid]["evidence"]), raw)
            self.assertEqual(hot[sid]["field"], sid)
            self.assertEqual(hot[sid]["as_of"], "2026-09-12")
            self.assertIn("id="+sid, hot[sid]["source"]["url"])
            self.assertIsNone(hot[sid]["confidence"])

    def test_failed_archive_is_not_published_as_proven_value(self):
        s3 = MemoryS3(); mod = module(s3)
        mod._fetch = lambda *a, **kw: b"observation_date,ICSA\n2026-09-12,196000\n"
        mod.capture = lambda *a, **kw: (_ for _ in ()).throw(ValueError("archive denied"))
        hot, status = {}, {}
        mod._fred_panel("claims", ["ICSA"], hot, status)
        self.assertIsNone(hot["ICSA"]["value"])
        self.assertIn("archive denied", hot["ICSA"]["reason"])

    def test_bls_annual_average_and_request_year_limit(self):
        s3 = MemoryS3(); mod = module(s3); requests = []
        raw = json.dumps({"status": "REQUEST_SUCCEEDED", "Results": {"series": [{"seriesID": mod.BLS_EXTRA[0], "data": [
            {"year": "2026", "period": "M13", "value": "99"}, {"year": "2026", "period": "M08", "value": "4.1"}]}]}}).encode()
        def fetch(url, **kw): requests.append(json.loads(kw["data"])); return raw
        mod._fetch = fetch
        hot, status = {}, {}; mod._bls(hot, status)
        self.assertEqual(hot[mod.BLS_EXTRA[0]]["value"], 4.1)
        self.assertEqual(hot[mod.BLS_EXTRA[0]]["as_of"], "2026-M08")
        self.assertEqual(int(requests[0]["endyear"])-int(requests[0]["startyear"]), 9)
        self.assertEqual(read_verified(s3, mod.BUCKET, hot[mod.BLS_EXTRA[0]]["evidence"]), raw)
        self.assertIsNone(hot[mod.BLS_EXTRA[0]]["unit"])
        self.assertTrue(hot[mod.BLS_EXTRA[1]]["data_unavailable"])


if __name__ == "__main__": unittest.main()
