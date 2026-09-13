"""Real internals build with fake S3 and forbidden vendor HTTP."""
import copy
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[4]
SOURCE = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SOURCE))
import internals_warehouse as iw
import compile_jh_internals as compiler


class FakeS3:
    def __init__(self):
        self.stamp = datetime.now(timezone.utc)-timedelta(hours=2)
        self.rows = {
            iw.KEY: {"schema_version":1,"fields":{"twos_tens":.39,"liq_proxy_bn":5852.,"nfci":-.564,"custom":42},"unrelated":{"keep":True}},
            "data/fred-cache.json": {sid:{"observations":[{"date":"2026-09-04","value":v}]} for sid,v in
                {"DGS10":4.95,"DGS2":4.56,"WALCL":6740619,"WTREGEN":883335,"RRPONTSYD":5.255,"NFCI":-.564}.items()},
            "data/finviz-universe.json": {"n_tickers":1000,"by_ticker":{str(i):{"price":2 if i<700 else 1,"prev_close":1 if i<700 else 2} for i in range(1000)},"generated_at":self.stamp.isoformat()}}
        self.puts = []
    def get_object(self, Bucket, Key):
        return {"Body":io.BytesIO(json.dumps(self.rows[Key]).encode()),"LastModified":self.stamp,"ETag":"existing"}
    def put_object(self, **kw):
        assert kw["IfMatch"] == "existing"
        self.puts.append(kw)
        self.rows[kw["Key"]] = json.loads(kw["Body"])
    def get_paginator(self, *args):
        raise AssertionError("Fresh cache must not traverse scoped banks")


class InternalsTests(unittest.TestCase):
    def test_fresh_warehouse_no_http_and_uncapped_merge(self):
        s3 = FakeS3()
        with patch("urllib.request.urlopen", side_effect=AssertionError("vendor HTTP forbidden")):
            out, _ = iw.build(s3)
            receipt = iw.run(s3)
        self.assertTrue(receipt["ok"])
        self.assertEqual(out["fields"], {"twos_tens":.39,"liq_proxy_bn":5852.,"nfci":-.564,"custom":42,
                          "ad_breadth":.4,"n_up":700,"n_down":300,"n_univ":1000,"n_missing":0,
                          "n_above_50":0,"n_sma50":0,"n_above_200":0,"n_sma200":0,
                          "n_new_high":0,"n_new_low":0,"nh_nl":0,"n_range52":0})
        self.assertEqual(out["unrelated"], {"keep":True})
        self.assertEqual(out["warehouse"]["fresh_fred_legs"],6)
        self.assertEqual(out["warehouse"]["fred_http_requests"],0)
        self.assertTrue(all(x["fresh_36h"] for x in out["warehouse"]["fred"].values()))
    def test_sma_independent_exact_80_percent_gate_uncapped(self):
        s3 = FakeS3()
        universe = s3.rows["data/finviz-universe.json"]
        for i, row in enumerate(universe["by_ticker"].values()):
            if i < 800:
                row["sma50_pct"] = 1 if i < 700 else 0
            if i < 799:
                row["sma200_pct"] = 1 if i < 700 else -1
        before = copy.deepcopy(s3.rows[iw.KEY])
        before["fields"].update(ad_breadth=.3238,n_up=7421,n_down=3659,n_univ=11618)
        before["warehouse"] = {"fred_http_requests":0,"fred":{"DGS10":{"key":"data/fred-cache.json","fresh_36h":True}}}
        out = iw.merge_sma(before, universe, s3.stamp.isoformat())
        self.assertEqual(out["fields"]["n_above_50"],700)
        self.assertEqual(out["fields"]["n_sma50"],800)
        self.assertEqual(out["fields"]["pct_above_50"],.875)
        self.assertEqual(out["fields"]["n_above_200"],700)
        self.assertEqual(out["fields"]["n_sma200"],799)
        self.assertNotIn("pct_above_200",out["fields"])
        self.assertEqual(out["sma_breadth"]["windows"]["50"]["coverage"],.8)
        self.assertIn("minimum 80%",out["sma_breadth"]["windows"]["200"]["skip_reason"])
        for key, value in before.items():
            if key == "fields":
                self.assertTrue(all(out[key][k] == v for k,v in value.items()))
            else:
                self.assertEqual(out[key],value)
        self.assertNotIn("sma_breadth",before)
    def test_sma_signed_percentages_sufficient_without_prices(self):
        s3 = FakeS3()
        for row in s3.rows["data/finviz-universe.json"]["by_ticker"].values():
            row.clear()
            row.update(sma50_pct=12.5,sma200_pct=-3.2)
        before = s3.rows[iw.KEY]
        before["fields"].update(pct_above_50=.9,pct_above_200=.8)
        out = iw.merge_sma(before,s3.rows["data/finviz-universe.json"],s3.stamp.isoformat())
        self.assertEqual(out["fields"]["pct_above_50"],1)
        self.assertEqual(out["fields"]["pct_above_200"],0)
        for window in (50,200):
            self.assertEqual(out["fields"][f"n_sma{window}"],1000)
            evidence = out["sma_breadth"]["windows"][str(window)]
            self.assertEqual(evidence["coverage"],1)
    def test_sma_nonfinite_excluded_zero_and_negative_in_denominator(self):
        rows = [{"sma50_pct":v} for v in (1,-2,0,"3.5",None,True,float("nan"),float("inf"),"bad")]
        rows += [{"sma50":1,"price":2}, None]
        universe = {"n_tickers":len(rows),"by_ticker":dict(enumerate(rows))}
        out = iw.merge_sma({"schema_version":1,"fields":{}},universe,"stamp")
        self.assertEqual(out["fields"]["n_above_50"],2)
        self.assertEqual(out["fields"]["n_sma50"],4)
        evidence = out["sma_breadth"]["windows"]["50"]
        self.assertEqual(evidence["n_missing"],7)
        self.assertEqual(evidence["coverage"],4/11)
    def test_sma_low_coverage_keeps_existing_publication(self):
        s3 = FakeS3()
        before = s3.rows[iw.KEY]
        before["fields"]["pct_above_50"] = .5
        snapshot = copy.deepcopy(before)
        with self.assertRaisesRegex(ValueError,"preserving existing"):
            iw.merge_sma(before,s3.rows["data/finviz-universe.json"],"stamp")
        self.assertEqual(before,snapshot)
    def test_sma_incomplete_or_capped_universe_rejected(self):
        s3 = FakeS3()
        universe = s3.rows["data/finviz-universe.json"]
        universe["n_tickers"] += 1
        with self.assertRaisesRegex(ValueError,"Full declared"):
            iw.merge_sma(s3.rows[iw.KEY],universe,"stamp")
        universe["n_tickers"] -= 1
        universe["capped"] = True
        with self.assertRaisesRegex(ValueError,"flagged capped"):
            iw.merge_sma(s3.rows[iw.KEY],universe,"stamp")
    def test_range52_inclusive_boundaries_and_independent_sides(self):
        pairs = [(-1,1),(-1.0001,1.0001),(0,0),(-.5,None),(None,.5),(None,None)]
        universe = {"n_tickers":len(pairs),"by_ticker":{
            str(i):{"off_52w_high_pct":h,"off_52w_low_pct":l} for i,(h,l) in enumerate(pairs)}}
        out = iw.merge_range52({"schema_version":1,"fields":{}},universe,"stamp")
        self.assertEqual(out["fields"],{"n_new_high":3,"n_new_low":3,"nh_nl":0,"n_range52":3})
        self.assertEqual(out["range52_breadth"]["n_valid_high"],4)
        self.assertEqual(out["range52_breadth"]["n_valid_low"],4)
    def test_range52_uncapped_and_entire_existing_document_preserved(self):
        before = {"schema_version":1,"fields":{"twos_tens":.39,"liq_proxy_bn":5852.,"nfci":-.564,
            "ad_breadth":.3238,"n_up":7421,"n_down":3659,"n_univ":11618,
            "pct_above_50":4983/11621,"pct_above_200":6108/11621,"custom":42},
            "warehouse":{"fred_http_requests":0,"fred":{"DGS10":{"as_of":"2026-09-10"}}},
            "sma_breadth":{"custom_proof":[1,2]},"other":{"keep":True}}
        snapshot = copy.deepcopy(before)
        rows = {str(i):{"off_52w_high_pct":-.5 if i<700 else -3,
                         "off_52w_low_pct":.5 if i>=700 else 3} for i in range(1000)}
        out = iw.merge_range52(before,{"n_tickers":1000,"by_ticker":rows},"stamp")
        self.assertEqual({k:out["fields"][k] for k in ("n_new_high","n_new_low","nh_nl","n_range52")},
                         {"n_new_high":700,"n_new_low":300,"nh_nl":400,"n_range52":1000})
        for key, value in before.items():
            if key == "fields":
                self.assertTrue(all(out[key][k] == v for k,v in value.items()))
            else:
                self.assertEqual(out[key],value)
        self.assertEqual(before,snapshot)
    def test_range52_nonfinite_values_and_negative_net(self):
        values = [True,float("nan"),float("inf"),float("-inf"),None,"bad"]
        pairs = [(v,v) for v in values] + [("-1","1"),(-4,-.2),(-5,0)]
        universe = {"n_tickers":len(pairs),"by_ticker":{
            str(i):{"off_52w_high_pct":h,"off_52w_low_pct":l} for i,(h,l) in enumerate(pairs)}}
        out = iw.merge_range52({"schema_version":1,"fields":{}},universe,"stamp")
        self.assertEqual(out["fields"],{"n_new_high":1,"n_new_low":3,"nh_nl":-2,"n_range52":3})
        universe["n_tickers"] += 1
        with self.assertRaisesRegex(ValueError,"Full declared"):
            iw.merge_range52({"schema_version":1,"fields":{}},universe,"stamp")
    def test_coverage_denominator_includes_unchanged_and_missing(self):
        pairs = [(2,1)]*3 + [(1,2)]*2 + [(1,1)]*3 + [(None,1)]*2
        self.assertTrue(iw.count_pairs(pairs)["eligible"])
        self.assertEqual(iw.count_pairs(pairs)["n_univ"],8)
        self.assertFalse(iw.count_pairs(pairs+[(None,1)])["eligible"])
    def test_low_coverage_preserves_prior_publication(self):
        s3 = FakeS3(); s3.rows[iw.KEY]["fields"]["ad_breadth"] = .3238
        before = copy.deepcopy(s3.rows[iw.KEY])
        for row in list(s3.rows["data/finviz-universe.json"]["by_ticker"].values())[:201]:
            row["prev_close"] = None
        with self.assertRaisesRegex(ValueError,"preserving existing"):
            iw.run(s3)
        self.assertEqual(s3.rows[iw.KEY],before)
        self.assertEqual(s3.puts,[])
    def test_compute_matches_canonical_script(self):
        spec = importlib.util.spec_from_file_location("canonical",ROOT/"scripts/compile_jh_internals.py")
        canonical = importlib.util.module_from_spec(spec); spec.loader.exec_module(canonical)
        legs = dict(dgs10=4.95,dgs2=4.56,walcl=6740619,tga=883335,rrp=5255,nfci=-.564,
                    n_up=7421,n_down=3659,n_univ=11618,n_above_50=700,n_sma50=800,n_above_200=600,n_sma200=900,
                    n_new_high=700,n_new_low=300)
        self.assertEqual(compiler.compute(legs)["fields"],canonical.compute(legs)["fields"])
        self.assertEqual(compiler.FRED_LEGS,canonical.FRED_LEGS)


if __name__ == "__main__":
    unittest.main()
