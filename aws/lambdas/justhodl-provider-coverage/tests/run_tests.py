"""justhodl-provider-coverage offline tests (in-memory S3)."""
import io
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "source")]


class S3Stub:
    def __init__(self):
        self.store = {}

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body


def boot():
    import boto3
    s3 = S3Stub()
    boto3.client = lambda *a, **k: s3
    for m in list(sys.modules):
        if m == "lambda_function":
            del sys.modules[m]
    import lambda_function as lf
    lf.s3 = s3
    return lf, s3


def test_folds_manifests_catalog_and_readers():
    lf, s3 = boot()
    s3.store["data/warm/fmp-bank/manifest.json"] = json.dumps({"version": "1.1.0", "phase": "BACKFILL", "key_status": "ok", "updated_at": "2026-10-25T16:26:00+00:00", "objects": 145, "bytes": 1.7e9, "datasets": 140,
        "endpoints_served": ["rating-bulk", "eod-bulk"], "endpoints_not_served": [["financial-growth-bulk", "http404"]], "run": {"calls": 190, "http": {"ok": 80, "http429": 10}}}).encode()
    s3.store["data/warm/fmp/manifest.json"] = json.dumps({"version": "1.1.0", "phase": "BACKFILL", "updated_at": "2026-10-25T16:40:00+00:00", "objects": 1000, "bytes": 2e8, "datasets": 90, "datasets_declared": 160,
        "endpoints_served": ["price-target", "grades"], "endpoints_refused": [["esg-benchmark", "http404"]], "skipped": [{"endpoint": "technical-indicators/*", "why": "derived"}],
        "per_dataset": {"price-target": {"rotation_days": 12.5}}, "universe_counts": {"tierA": 11000}}).encode()
    s3.store["data/warm/provider-bank/manifest.json"] = json.dumps({"version": "1.0.0", "phase": "BACKFILL", "updated_at": "2026-10-25T16:50:00+00:00", "objects": 50, "bytes": 5e6, "datasets": 30, "datasets_declared": 58,
        "endpoints_served": ["te/countries"], "endpoints_refused": [["te/calendar", "http403"]], "skipped": [{"provider": "te", "path": "x", "why": "y"}],
        "by_provider": {"te": {"served": ["te/countries", "te/news"], "refused": [["te/calendar", "http403"]], "untested": ["te/comtrade-country"], "objects": 20, "bytes": 2e6, "calls_today": 40, "daily_call_cap": 1500, "key_present": True},
                        "cmc": {"served": ["cmc/map"], "refused": [["cmc/global-metrics-historical", "api_error:1006"]], "untested": [], "objects": 5, "bytes": 1e6},
                        "newsapi": {"served": [], "refused": [], "untested": ["newsapi/sources"], "objects": 0, "bytes": 0, "key_present": False},
                        "ndl": {"served": ["ndl/databases"], "refused": [], "untested": [], "objects": 2, "bytes": 1e5},
                        "cq": {"served": ["cq/exchange-split"], "refused": [["cq/hourly", "http403"]], "untested": [], "objects": 10, "bytes": 1e6}}}).encode()
    s3.store["data/provider-catalog.json"] = json.dumps({"providers": [{"slug": "fmp", "name": "FMP", "datasets": 8, "mb": 10.9, "freshest_h": 0.2}, {"slug": "polygon", "name": "Polygon", "datasets": 1644}]}).encode()
    s3.store["engine-manifest.json"] = json.dumps({"engines": {"justhodl-fmp-ratios": {"reads": ["data/warm/fmp-bank/manifest.json"]}, "justhodl-x": {"reads": ["data/other.json"]}}}).encode()
    out = lf.lambda_handler({}, None)
    doc = json.loads(s3.store[lf.OUT])
    assert doc["providers"]["fmp"]["served_n"] == 4 and doc["providers"]["fmp"]["refused"] == [["financial-growth-bulk", "http404"], ["esg-benchmark", "http404"]], doc["providers"]["fmp"]
    assert doc["providers"]["fmp"]["coverage"] is not None and 0 < doc["providers"]["fmp"]["coverage"] < 1
    assert doc["providers"]["te"]["served_n"] == 2 and doc["providers"]["te"]["untested"] == 1 and doc["providers"]["te"]["coverage"] == 0.5
    assert doc["providers"]["newsapi"]["coverage"] == 0.0
    assert doc["providers"]["polygon"]["banks"] == ["polygon-bank"] and doc["banks"]["polygon-bank"]["present"] is False
    assert "justhodl-fmp-ratios" in doc["banks"]["fmp-bank"]["used_by"]
    assert doc["totals"]["objects_banked"] == 1195
    assert doc["providers"]["fmp"]["catalog_cards"]["fmp"]["mb"] == 10.9
    assert any(k.startswith(lf.HISTORY) for k in s3.store)
    assert out["providers"]["cq"]["refused"] == 1


if __name__ == "__main__":
    test_folds_manifests_catalog_and_readers()
    print("ok test_folds_manifests_catalog_and_readers")
    print("1 tests passed")
