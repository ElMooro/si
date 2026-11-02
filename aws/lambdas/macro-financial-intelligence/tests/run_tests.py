"""macro-financial-intelligence v2.0.1 offline tests - urllib3 and boto3 stubbed; no network, no AWS.

2026-11-02: the brief carried two constants presented as data ('health': 7.5, 'liq': 'NORMAL') and the page called the
Fed's MBS holdings (FRED WSHOMCB) "Primary Dealer Positioning".  Every published field is now a FRED reading or computed
from one; with no data the alert says UNAVAILABLE, never NORMAL."""
from pathlib import Path
import json
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
PUTS, STORE = [], {}
FRED = {}


class _S3:
    def get_object(self, Bucket, Key):
        if Key not in STORE:
            raise KeyError(Key)
        return {"Body": types.SimpleNamespace(read=lambda: STORE[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        STORE[Key] = Body.encode() if isinstance(Body, str) else Body
        PUTS.append(Key)


class _Pool:
    def request(self, method, url, fields=None):
        obs = FRED.get(fields["series_id"])
        if obs is None:
            return types.SimpleNamespace(status=500, data=b"{}")
        return types.SimpleNamespace(status=200, data=json.dumps({"observations": obs}).encode())


sys.modules["boto3"] = types.SimpleNamespace(client=lambda *a, **k: _S3())
sys.modules["urllib3"] = types.SimpleNamespace(PoolManager=lambda: _Pool())
sys.path.insert(0, str(ROOT / "source"))
import lambda_function as L  # noqa: E402


def obs(values):
    return [{"date": "2026-10-%02d" % (29 - i), "value": str(v)} for i, v in enumerate(values)]


class Tests(unittest.TestCase):
    def setUp(self):
        PUTS.clear()
        STORE.clear()
        FRED.clear()

    def test_brief_is_fred_only_no_constants(self):
        FRED.update({"WSHOMCB": obs([1883520, 1890000, 1900000, 1910000, 1920000]), "DFF": obs([3.88]), "T10Y2Y": obs([0.38]),
                     "DGS10": obs([4.98]), "DGS2": obs([4.61])})
        out = L.lambda_handler({}, None)
        self.assertEqual(out["statusCode"], 200)
        brief = json.loads(STORE["latest_brief.json"])
        self.assertNotIn("health", brief)
        self.assertNotIn("liq", brief)
        self.assertEqual(brief["inds"]["t10y"]["val"], 4.98)
        self.assertEqual(brief["fails"]["current_b"], 1883.52)
        self.assertIn(brief["alert"], ("NORMAL", "WARNING", "CRITICAL"))
        self.assertIn("WSHOMCB", brief["series"]["fails"])
        self.assertEqual(brief["version"], "2.0.1")

    def test_no_data_is_unavailable_not_normal(self):
        L.lambda_handler({}, None)
        brief = json.loads(STORE["latest_brief.json"])
        self.assertEqual(brief["fails"], {"status": "no_data"})
        self.assertEqual(brief["alert"], "UNAVAILABLE")

    def test_page_shows_no_invented_score_and_names_the_series(self):
        FRED.update({"WSHOMCB": obs([1, 2, 3]), "DGS10": obs([4.98])})
        L.lambda_handler({}, None)
        out = L.lambda_handler({"requestContext": {"http": {"method": "GET"}}}, None)
        html = out["body"]
        self.assertEqual(out["statusCode"], 200)
        for gone in ("Market Health", "/10", "Liquidity", "Primary Dealer", "Real-time"):
            self.assertNotIn(gone, html)
        self.assertIn("Fed MBS holdings (FRED WSHOMCB", html)


if __name__ == "__main__":
    result = unittest.main(exit=False, verbosity=2).result
    sys.exit(0 if result.wasSuccessful() else 1)
