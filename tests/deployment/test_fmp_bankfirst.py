"""fmp_bankfirst: FMP /stable URLs answered from the banks when fresh; everything else untouched (deploy gate)."""
import gzip
import io
import json
import os
import sys
import urllib.request
from datetime import date, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "aws", "shared"))

import fmp_bank  # noqa: E402
import fmp_bankfirst  # noqa: E402


class S3:
    def __init__(self):
        self.store, self.meta = {}, {}

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.store[Key]), "Metadata": self.meta.get(Key, {})}


def _put(s3, key, rows, **meta):
    s3.store[key] = gzip.compress(json.dumps(rows).encode())
    s3.meta[key] = meta


def test_bankfirst_hits_stale_and_passthrough():
    s3 = S3()
    fmp_bank._S3 = s3
    fmp_bank._cache.clear()
    today = date.today()
    _put(s3, fmp_bank.WALKER_ROOT + "price-target/AAPL.json.gz", [{"symbol": "AAPL", "priceTarget": 300}], pulled_at=today.isoformat())
    _put(s3, fmp_bank.WALKER_ROOT + "grades/OLD.json.gz", [{"symbol": "OLD"}], pulled_at=(today - timedelta(days=40)).isoformat())
    _put(s3, fmp_bank.WALKER_ROOT + "historical-price-eod-full/AAPL.json.gz", [{"date": (today - timedelta(days=1)).isoformat(), "close": 1}, {"date": "2020-01-02", "close": 2}], pulled_at=today.isoformat())
    _put(s3, fmp_bank.WALKER_ROOT + "historical-price-eod-full/STALE.json.gz", [{"date": (today - timedelta(days=30)).isoformat(), "close": 1}], pulled_at=today.isoformat())
    calls = []

    def fake_real(url, data=None, timeout=None, *a, **k):
        calls.append(url.full_url if isinstance(url, urllib.request.Request) else url)
        r = io.BytesIO(b'{"live": true}')
        r.__enter__ = lambda s=r: s
        r.__exit__ = lambda s=r, *x: None
        return r
    fmp_bankfirst.uninstall()
    urllib.request.urlopen = fake_real
    fmp_bankfirst.install()
    try:
        for k in fmp_bankfirst.STATS:
            fmp_bankfirst.STATS[k] = 0
        # hit: fresh walker object, same shape as the endpoint, apikey stripped before the lookup
        with urllib.request.urlopen("https://financialmodelingprep.com/stable/price-target?symbol=AAPL&apikey=SECRET") as r:
            assert json.loads(r.read()) == [{"symbol": "AAPL", "priceTarget": 300}]
            assert r.status == 200 and r.getcode() == 200 and r.headers["X-JH-Source"] == "fmp-bank"
        # stale fast-family object -> live
        assert json.loads(urllib.request.urlopen(urllib.request.Request("https://financialmodelingprep.com/stable/grades?symbol=OLD&apikey=S")).read()) == {"live": True}
        # price family: newest banked date within 5 days -> hit; 30 days -> live
        assert len(json.loads(urllib.request.urlopen("https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&apikey=S").read())) == 2
        assert json.loads(urllib.request.urlopen("https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=STALE&apikey=S").read()) == {"live": True}
        # never-bank family and non-FMP URLs pass straight through; POSTs pass through
        assert json.loads(urllib.request.urlopen("https://financialmodelingprep.com/stable/quote?symbol=AAPL&apikey=S").read()) == {"live": True}
        assert json.loads(urllib.request.urlopen("https://api.polygon.io/v2/aggs/ticker/AAPL/prev?apiKey=S").read()) == {"live": True}
        assert json.loads(urllib.request.urlopen("https://financialmodelingprep.com/stable/price-target?symbol=AAPL", data=b"x").read()) == {"live": True}
        # unbanked symbol -> live (miss)
        assert json.loads(urllib.request.urlopen("https://financialmodelingprep.com/stable/price-target?symbol=ZZZZ&apikey=S").read()) == {"live": True}
        assert fmp_bankfirst.STATS["hits"] == 2 and fmp_bankfirst.STATS["stale"] == 2 and fmp_bankfirst.STATS["misses"] == 1, fmp_bankfirst.STATS
        assert len(calls) == 6, calls
        assert all("SECRET" not in c or "price-target?symbol=AAPL&apikey=SECRET" in c for c in calls)
    finally:
        fmp_bankfirst.uninstall()


if __name__ == "__main__":
    test_bankfirst_hits_stale_and_passthrough()
    print("ok test_bankfirst_hits_stale_and_passthrough")
