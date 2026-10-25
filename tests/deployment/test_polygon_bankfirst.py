"""polygon_bankfirst: Polygon daily aggregates answered from the per-ticker bank only when the answer means the same thing
and is complete; everything else untouched (deploy gate)."""
import gzip
import io
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "aws", "shared"))

import polygon_bankfirst as pb  # noqa: E402

NOW = datetime(2026, 10, 22, 23, 0, tzinfo=timezone.utc)      # Thursday after settle: Oct 22 is the last completed session


class _FixedDT(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz else NOW.replace(tzinfo=None)


class S3:
    def __init__(self):
        self.store, self.gets = {}, []

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        if Key not in self.store:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.store[Key])}


def ms(y, m, d):
    return int(datetime(y, m, d, 4, tzinfo=timezone.utc).timestamp() * 1000)     # Polygon stamps midnight ET


def _rows(days):
    return [[ms(2026, 10, d), 100 + d, 101 + d, 99 + d, 100.5 + d, 1000.0 * d, 100.2 + d, 10 * d] for d in days]


def _bank(s3, ticker, days, first="2026-10-01"):
    rows = _rows(days)
    doc = {"ticker": ticker, "adjusted": True, "fields": pb.FIELDS, "rows": rows, "n": len(rows), "first": first, "last": "2026-10-%02d" % days[-1]}
    s3.store[pb.ROOT + ticker + ".json.gz"] = gzip.compress(json.dumps(doc).encode())


def _install(calls):
    def fake_real(url, data=None, timeout=None, *a, **k):
        calls.append(url.full_url if isinstance(url, urllib.request.Request) else url)
        r = io.BytesIO(b'{"live": true}')
        r.__enter__ = lambda s=r: s
        r.__exit__ = lambda s=r, *x: None
        return r
    pb.uninstall()
    urllib.request.urlopen = fake_real
    pb.install()


def _get(url):
    return json.loads(urllib.request.urlopen(url).read())


def test_polygon_bankfirst_answers_complete_windows_in_polygons_envelope():
    s3 = S3()
    pb._S3 = s3
    pb._cache.clear()
    pb._reset_ctl()
    for k in pb.STATS:
        pb.STATS[k] = 0
    pb.datetime = _FixedDT
    weekdays = [d for d in range(1, 23) if datetime(2026, 10, d).weekday() < 5]
    _bank(s3, "AAPL", weekdays)                                             # through Oct 22 = last completed session
    _bank(s3, "LAG", [d for d in weekdays if d <= 21])                      # one session behind
    _bank(s3, "NEW", [d for d in weekdays if d >= 12], first="2026-10-12")   # listed Oct 12
    s3.store[pb.MANIFEST] = json.dumps({"gaps": ["2026-10-13"], "holidays_recorded": []}).encode()
    calls = []
    _install(calls)
    try:
        # a closed window after the gap: bank answers, Polygon's envelope, asc by default
        got = _get("https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2026-10-14/2026-10-16?adjusted=true&sort=asc&limit=120&apiKey=SECRET")
        assert got["ticker"] == "AAPL" and got["status"] == "OK" and got["adjusted"] is True and got["resultsCount"] == 3 == got["count"] == got["queryCount"], got
        assert [r["t"] for r in got["results"]] == [ms(2026, 10, 14), ms(2026, 10, 15), ms(2026, 10, 16)]
        assert set(got["results"][0]) == {"v", "vw", "o", "c", "h", "l", "t", "n"} and got["results"][0]["c"] == 114.5
        # desc honoured; a window reaching today is answered after settle when the bank is at the last completed session
        got = _get("https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2026-10-19/2026-10-30?sort=desc&apiKey=S")
        assert [r["t"] for r in got["results"]] == [ms(2026, 10, 22), ms(2026, 10, 21), ms(2026, 10, 20), ms(2026, 10, 19)]
        # /prev = the last completed session's bar
        got = _get("https://api.polygon.io/v2/aggs/ticker/AAPL/prev?adjusted=true&apiKey=S")
        assert got["resultsCount"] == 1 and got["results"][0]["t"] == ms(2026, 10, 22) and got["results"][0]["T"] == "AAPL"
        # millisecond window arguments work too
        got = _get("https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/%d/%d?apiKey=S" % (ms(2026, 10, 19), ms(2026, 10, 20)))
        assert got["resultsCount"] == 2
        # a window spanning the published gap (Oct 13) -> live
        assert _get("https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2026-10-12/2026-10-16?apiKey=S") == {"live": True}
        # the bank one session behind for a current window -> live; a past window it reaches -> bank
        assert _get("https://api.polygon.io/v2/aggs/ticker/LAG/range/1/day/2026-10-19/2026-10-30?apiKey=S") == {"live": True}
        assert _get("https://api.polygon.io/v2/aggs/ticker/LAG/range/1/day/2026-10-19/2026-10-21?apiKey=S")["resultsCount"] == 3
        # a window starting before the bank's history -> live; a ticker listed after the window start (inside the bank's era) -> bank
        assert _get("https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2020-01-01/2026-10-22?apiKey=S") == {"live": True}
        assert _get("https://api.polygon.io/v2/aggs/ticker/NEW/range/1/day/2026-10-14/2026-10-22?apiKey=S")["resultsCount"] == 7
        # shapes the bank does not reproduce: weekly bars, unadjusted, unknown params, a paginating limit, snapshots, grouped
        for u in ("v2/aggs/ticker/AAPL/range/1/week/2026-10-14/2026-10-16", "v2/aggs/ticker/AAPL/range/5/day/2026-10-14/2026-10-16",
                  "v2/aggs/ticker/AAPL/range/1/day/2026-10-14/2026-10-16?adjusted=false", "v2/aggs/ticker/AAPL/range/1/day/2026-10-14/2026-10-16?timespan=day",
                  "v2/aggs/ticker/AAPL/range/1/day/2026-10-14/2026-10-16?limit=2", "v2/snapshot/locale/us/markets/stocks/tickers/AAPL",
                  "v2/aggs/grouped/locale/us/market/stocks/2026-10-22"):
            assert _get("https://api.polygon.io/" + u + ("&" if "?" in u else "?") + "apiKey=S") == {"live": True}, u
        # unbanked ticker -> live (miss); POSTs and other hosts untouched
        assert _get("https://api.polygon.io/v2/aggs/ticker/ZZZZ/prev?apiKey=S") == {"live": True}
        assert json.loads(urllib.request.urlopen("https://api.polygon.io/v2/aggs/ticker/AAPL/prev", data=b"x").read()) == {"live": True}
        assert _get("https://financialmodelingprep.com/stable/profile?symbol=AAPL") == {"live": True}
        st = pb.STATS
        assert st["hits"] == 6 and st["gap"] == 1 and st["stale"] == 1 and st["misses"] == 1 and st["unshaped"] >= 6, st
        assert all("SECRET" not in c for c in calls)
    finally:
        pb.uninstall()
        pb.datetime = datetime


def test_polygon_bankfirst_session_window_and_never_inside_the_banks():
    s3 = S3()
    pb._S3 = s3
    pb._cache.clear()
    pb._reset_ctl()
    _bank(s3, "AAPL", [d for d in range(1, 24) if datetime(2026, 10, d).weekday() < 5])
    s3.store[pb.MANIFEST] = json.dumps({"gaps": []}).encode()
    # Monday 15:00 UTC inside the session: a current window is the provider's even though the bank is at Friday's close
    assert pb.answer("https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2026-10-19/2026-10-26?apiKey=S", now=datetime(2026, 10, 26, 15, 0, tzinfo=timezone.utc)) is None
    assert pb.answer("https://api.polygon.io/v2/aggs/ticker/AAPL/prev?apiKey=S", now=datetime(2026, 10, 26, 15, 0, tzinfo=timezone.utc)) is None
    # Saturday: Friday Oct 23 is the last completed session and the bank has it
    body = pb.answer("https://api.polygon.io/v2/aggs/ticker/AAPL/prev?apiKey=S", now=datetime(2026, 10, 24, 15, 0, tzinfo=timezone.utc))
    assert json.loads(body)["results"][0]["t"] == ms(2026, 10, 23)
    pb.uninstall()
    real = urllib.request.urlopen
    old = os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
    try:
        for fn in ("justhodl-polygon-full", "justhodl-polygon-eod", "justhodl-tv-bars"):
            os.environ["AWS_LAMBDA_FUNCTION_NAME"] = fn
            pb.install()
            assert urllib.request.urlopen is real and not pb._installed["done"], fn
    finally:
        if old is None:
            os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)
        else:
            os.environ["AWS_LAMBDA_FUNCTION_NAME"] = old
        pb.uninstall()
    for fn in ("justhodl-polygon-full", "justhodl-polygon-eod", "justhodl-tv-bars", "justhodl-polygon-bank"):
        assert "from polygon_bankfirst import install" not in open(os.path.join(ROOT, "aws", "lambdas", fn, "source", "lambda_function.py")).read(), fn


if __name__ == "__main__":
    test_polygon_bankfirst_answers_complete_windows_in_polygons_envelope()
    test_polygon_bankfirst_session_window_and_never_inside_the_banks()
    print("ok polygon_bankfirst")
