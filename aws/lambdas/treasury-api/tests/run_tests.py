"""treasury-api offline tests - urllib and boto3 stubbed; no network, no AWS.

2026-11-02 (ops 6712/6713): Treasury FiscalData answers HTTP 400 "Invalid Filter Format" to `bid_to_cover_ratio:not.is.null`,
which left the API and its history file empty from 2026-05-28.  The query now uses documented operators only and the
complete-auction filter runs here."""
from pathlib import Path
import io
import json
import re
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
PUTS, URLS = [], []


class _S3:
    def get_object(self, Bucket, Key):
        raise KeyError(Key)

    def put_object(self, Bucket, Key, Body, **kw):
        PUTS.append((Bucket, Key, Body))


sys.modules["boto3"] = types.SimpleNamespace(client=lambda *a, **k: _S3())
sys.path.insert(0, str(ROOT / "source"))
import lambda_function as L  # noqa: E402


def row(date, term, btc="2.5", typ="Note", **kw):
    r = {"auction_date": date, "security_term": term, "security_type": typ, "cusip": "91282C" + date[-2:] + term[:1],
         "bid_to_cover_ratio": btc, "total_accepted": "40000000000", "total_tendered": "100000000000", "high_yield": "4.10",
         "avg_med_yield": "4.05", "low_yield": "3.90", "indirect_bidder_accepted": "28000000000", "primary_dealer_accepted": "6000000000",
         "direct_bidder_accepted": "6000000000"}
    r.update(kw)
    return r


ROWS = [row("2026-10-22", "5-Year", btc="null", high_yield="null")]            # announced, results not in yet
ROWS += [row("2026-10-%02d" % d, t) for d in (20, 13, 6) for t in ("2-Year", "5-Year", "10-Year", "30-Year")]
ROWS += [row("2026-09-%02d" % d, t, btc="2.%d" % d) for d in (29, 22, 15) for t in ("2-Year", "5-Year", "10-Year", "30-Year")]


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def fake_urlopen(url, timeout=None):
    URLS.append(url)
    return _Resp(json.dumps({"data": ROWS, "meta": {"total-count": len(ROWS)}}).encode())


L.urllib.request.urlopen = fake_urlopen


class Tests(unittest.TestCase):
    def setUp(self):
        PUTS.clear()
        URLS.clear()

    def test_query_uses_documented_operators_only(self):
        L.get_comprehensive_auction_data()
        url = URLS[-1]
        self.assertNotIn("not.is.null", url)
        self.assertNotIn("limit=", url)
        filt = re.search(r"filter=([^&]+)", url).group(1)
        for clause in filt.split(","):
            self.assertIn(clause.split(":")[1], ("eq", "lt", "lte", "gt", "gte", "in"))
        self.assertIn("page%5Bsize%5D=10000", url)

    def test_incomplete_auctions_are_dropped_and_limit_applies(self):
        got = L.get_comprehensive_auction_data(limit=5)
        self.assertEqual(len(got), 5)
        self.assertTrue(all(L.has_all_required_data(a) for a in got))
        self.assertNotIn("2026-10-22", [a["auction_date"] for a in got])

    def test_scheduled_update_writes_the_history_with_instruments(self):
        out = L.lambda_handler({"source": "aws.events"}, None)
        self.assertEqual(out["statusCode"], 200)
        self.assertEqual([(b, k) for b, k, _ in PUTS], [("openbb-lambda-data", "treasury_historical_comprehensive.json")])
        doc = json.loads(PUTS[0][2])
        inst = doc["updates"][0]["instruments"]
        self.assertEqual(sorted(inst), ["10-Year", "2-Year", "30-Year", "5-Year"])
        self.assertEqual(inst["5-Year"]["latest_auction"].get("auction_date") or inst["5-Year"]["latest_auction"].get("date"), "2026-10-20")

    def test_api_current_auction_answers(self):
        out = L.lambda_handler({"rawPath": "/auctions/current"}, None)
        body = json.loads(out["body"])
        self.assertEqual(out["statusCode"], 200)
        self.assertNotIn("No complete auction data", json.dumps(body))


if __name__ == "__main__":
    result = unittest.main(exit=False, verbosity=2).result
    sys.exit(0 if result.wasSuccessful() else 1)
