"""bank_originals: the bank rung of the received-original research engines - FMP-shaped bytes from the warehouse bank with
the bank's provenance (object, pulled_at, as_of) and the vintage rule; None for everything the bank does not answer."""
import gzip
import io
import json
import os
import sys
import time
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "aws", "shared"))

import bank_originals  # noqa: E402
import fmp_bank  # noqa: E402
import fmp_bankfirst  # noqa: E402

NOW = datetime(2026, 10, 22, 23, 0, tzinfo=timezone.utc)


class _FixedDT(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz else NOW.replace(tzinfo=None)


class S3:
    def __init__(self):
        self.store, self.meta, self.gets = {}, {}, []

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        if Key not in self.store:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.store[Key]), "Metadata": self.meta.get(Key, {})}


def _put(s3, key, rows, **meta):
    s3.store[key] = gzip.compress(json.dumps(rows).encode())
    s3.meta[key] = meta


def _fresh_bank(s3):
    fmp_bank._S3 = s3
    fmp_bank._cache.clear()
    fmp_bank._TABLES.clear()
    fmp_bank._MISSING.clear()
    fmp_bank._EVICTED.clear()
    for k in fmp_bank.STATS:
        fmp_bank.STATS[k] = 0
    fmp_bankfirst._reset_ctl(time.time())
    for k in fmp_bankfirst.STATS:
        fmp_bankfirst.STATS[k] = 0


def test_bank_original_carries_the_banks_provenance_and_answers_only_what_the_bank_answers():
    s3 = S3()
    _fresh_bank(s3)
    fmp_bankfirst.datetime = _FixedDT
    W = fmp_bank.WALKER_ROOT
    fresh = "2026-10-22T20:00:00Z"
    _put(s3, W + "analyst-estimates-annual/AAPL.json.gz", [{"symbol": "AAPL", "date": "2026-12-31", "epsAvg": 7.5}], pulled_at=fresh)
    _put(s3, W + "historical-price-eod-full/AAPL.json.gz", [{"date": "2026-10-22", "close": 1}, {"date": "2026-10-21", "close": 2}, {"date": "2020-01-02", "close": 3}], pulled_at=fresh)
    out = bank_originals.bank_original("https://financialmodelingprep.com/stable/analyst-estimates?symbol=AAPL&period=annual&limit=5&apikey=SECRET", now=NOW)
    assert out is not None
    assert json.loads(out["raw"]) == [{"symbol": "AAPL", "date": "2026-12-31", "epsAvg": 7.5}]
    assert b"SECRET" not in out["raw"]
    assert out["transport"] == bank_originals.TRANSPORT == "warehouse_bank"
    assert out["received_at"] == NOW.isoformat()
    assert out["bank"]["object"] == W + "analyst-estimates-annual/AAPL.json.gz"
    assert out["bank"]["pulled_at"] == "2026-10-22T20:00:00+00:00"
    assert bank_originals.vintage(out["bank"]) == datetime(2026, 10, 22, 20, 0, tzinfo=timezone.utc)
    # a price window is answered from the walker file with from/to applied, in FMP's shape
    px = bank_originals.bank_original("https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&from=2026-10-01&to=2026-10-22&apikey=S", now=NOW)
    assert px is not None and [r["date"] for r in json.loads(px["raw"])] == ["2026-10-22", "2026-10-21"]
    # never-banked (quote), unknown symbol, non-FMP URL -> None, and nothing opened a connection
    assert bank_originals.bank_original("https://financialmodelingprep.com/stable/quote?symbol=AAPL&apikey=S", now=NOW) is None
    assert bank_originals.bank_original("https://financialmodelingprep.com/stable/analyst-estimates?symbol=ZZZZ&period=annual&limit=5", now=NOW) is None
    assert bank_originals.bank_original("https://api.polygon.io/benzinga/v1/earnings?ticker=AAPL", now=NOW) is None
    assert all(k.startswith(W) or k.endswith("manifest.json") for k in s3.gets)


def test_vintage_rule_admits_only_newer_bank_content():
    prov = {"object": "x", "pulled_at": "2026-10-22T20:00:00+00:00", "as_of": None}
    assert bank_originals.newer_than(prov, None)                                   # no earlier original: admitted
    assert bank_originals.newer_than(prov, "2026-10-21T23:59:00+00:00")             # pulled after the previous receipt
    assert not bank_originals.newer_than(prov, "2026-10-22T20:00:00+00:00")         # equal is not newer
    assert not bank_originals.newer_than(prov, "2026-10-23T01:00:00Z")              # older content than the live baseline
    assert not bank_originals.newer_than({"object": "x", "pulled_at": None, "as_of": None}, "2026-10-01T00:00:00+00:00")  # unknown vintage: refused
    assert bank_originals.newer_than({"object": "x", "pulled_at": None, "as_of": "2026-10-22"}, "2026-10-21T00:00:00+00:00")  # as_of fallback
    assert not bank_originals.newer_than(prov, "not a clock")


def test_bank_original_never_raises_when_the_bank_is_unavailable():
    class Broken:
        def get_object(self, Bucket, Key):
            raise RuntimeError("s3 down")
    _fresh_bank(Broken())
    assert bank_originals.bank_original("https://financialmodelingprep.com/stable/analyst-estimates?symbol=AAPL&period=annual&limit=5", now=NOW) is None


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("bank_originals tests passed")
