"""fmp_bankfirst: FMP /stable URLs answered from the banks only when the answer means the same thing and is fresh;
everything else untouched; the bank steps aside when it would cost more than the provider (deploy gate)."""
import gzip
import io
import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "aws", "shared"))

import fmp_bank  # noqa: E402
import fmp_bankfirst  # noqa: E402

NOW = datetime(2026, 10, 22, 23, 0, tzinfo=timezone.utc)          # Thursday 23:00 UTC: Oct 22's session is complete


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


def _csv(s3, key, text, **meta):
    s3.store[key] = gzip.compress(text.encode())
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


def _install(calls):
    def fake_real(url, data=None, timeout=None, *a, **k):
        calls.append(url.full_url if isinstance(url, urllib.request.Request) else url)
        r = io.BytesIO(b'{"live": true}')
        r.__enter__ = lambda s=r: s
        r.__exit__ = lambda s=r, *x: None
        return r
    fmp_bankfirst.uninstall()
    urllib.request.urlopen = fake_real
    fmp_bankfirst.install()


def _get(url):
    return json.loads(urllib.request.urlopen(url).read())


def test_bankfirst_meaning_and_freshness_guards():
    s3 = S3()
    _fresh_bank(s3)
    fmp_bankfirst.datetime = _FixedDT
    W = fmp_bank.WALKER_ROOT
    fresh, old, older = "2026-10-22T20:00:00Z", "2026-10-02T20:00:00Z", "2026-09-12T20:00:00Z"
    _put(s3, W + "price-target/AAPL.json.gz", [{"symbol": "AAPL", "priceTarget": 300}], pulled_at=fresh)
    _put(s3, W + "grades/OLD.json.gz", [{"symbol": "OLD"}], pulled_at=old)                               # 20 days: a fast family is live
    _put(s3, W + "key-executives/SLOW.json.gz", [{"symbol": "SLOW", "name": "x"}], pulled_at=old)       # 20 days: a slow family still answers
    _put(s3, W + "key-executives/OLDER.json.gz", [{"symbol": "OLDER"}], pulled_at=older)                # 40 days: live
    _put(s3, W + "historical-price-eod-full/AAPL.json.gz", [{"date": "2026-10-22", "close": 1}, {"date": "2026-10-21", "close": 2}, {"date": "2020-01-02", "close": 3}], pulled_at=fresh)
    _put(s3, W + "historical-price-eod-full/LAG.json.gz", [{"date": "2026-10-21", "close": 1}], pulled_at=fresh)       # one session behind
    _put(s3, W + "dividends/NODATE.json.gz", [{"symbol": "NODATE", "amount": 1}], pulled_at=fresh)
    calls = []
    _install(calls)
    try:
        # hit: fresh walker object, FMP's shape, apikey stripped before the lookup, response object complete
        with urllib.request.urlopen("https://financialmodelingprep.com/stable/price-target?symbol=AAPL&apikey=SECRET") as r:
            assert json.loads(r.read()) == [{"symbol": "AAPL", "priceTarget": 300}]
            assert r.status == 200 and r.getcode() == 200 and r.headers["X-JH-Source"] == "fmp-bank"
        r = urllib.request.urlopen("https://financialmodelingprep.com/stable/price-target?symbol=AAPL&apikey=S")
        assert r.headers.get_content_charset() == "utf-8" and r.info().get_content_type() == "application/json"
        assert r.headers.get("Content-Encoding") is None and r.headers.get("Content-Encoding", "identity") == "identity"
        assert r.headers.get_all("Content-Length") == [str(r.length)] and "Content-Type" in r.headers and r.msg is r.headers
        assert dict(r.getheaders())["X-JH-Source"] == "fmp-bank" and r.reason == "OK" and r.geturl().startswith("https://financialmodelingprep.com/stable/price-target")
        assert json.load(r) == [{"symbol": "AAPL", "priceTarget": 300}]
        r.close()
        assert r.isclosed()
        # fast family 20 days old -> live; slow family 20 days old -> bank; slow family 40 days old -> live
        assert _get("https://financialmodelingprep.com/stable/grades?symbol=OLD&apikey=S") == {"live": True}
        assert _get("https://financialmodelingprep.com/stable/key-executives?symbol=SLOW&apikey=S") == [{"symbol": "SLOW", "name": "x"}]
        assert _get("https://financialmodelingprep.com/stable/key-executives?symbol=OLDER&apikey=S") == {"live": True}
        # prices: newest banked date IS the last completed session -> hit, with from/to applied; one session behind -> live
        assert _get("https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&apikey=S") == [{"date": "2026-10-22", "close": 1}, {"date": "2026-10-21", "close": 2}, {"date": "2020-01-02", "close": 3}]
        assert _get("https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&from=2026-10-01&to=2026-10-21&apikey=S") == [{"date": "2026-10-21", "close": 2}]
        assert _get("https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=LAG&apikey=S") == {"live": True}
        # a `to` in the past wants that session's row as the newest: Oct 21 asked, bank newest Oct 21 -> hit
        assert _get("https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=LAG&to=2026-10-21&apikey=S") == [{"date": "2026-10-21", "close": 1}]
        # shapes the bank cannot reproduce go live: page, datatype, symbols=, unknown params, a window over undated rows
        for u in ("insider-trading/search?symbol=AAPL&page=0&limit=100", "price-target?symbol=AAPL&datatype=csv", "news/stock?symbols=AAPL",
                  "price-target?symbol=AAPL&structure=flat", "dividends?symbol=NODATE&from=2026-01-01"):
            assert _get("https://financialmodelingprep.com/stable/" + u + "&apikey=S") == {"live": True}, u
        # never-bank family, non-FMP URLs and POSTs pass straight through
        assert _get("https://financialmodelingprep.com/stable/quote?symbol=AAPL&apikey=S") == {"live": True}
        assert _get("https://api.polygon.io/v2/aggs/ticker/AAPL/prev?apiKey=S") == {"live": True}
        assert json.loads(urllib.request.urlopen("https://financialmodelingprep.com/stable/price-target?symbol=AAPL", data=b"x").read()) == {"live": True}
        # unbanked symbol -> live (miss)
        assert _get("https://financialmodelingprep.com/stable/price-target?symbol=ZZZZ&apikey=S") == {"live": True}
        st = fmp_bankfirst.STATS
        assert st["hits"] == 6 and st["stale"] == 3 and st["misses"] == 1 and st["unshaped"] == 5, st
        assert all("SECRET" not in c for c in calls)
    finally:
        fmp_bankfirst.uninstall()
        fmp_bankfirst.datetime = datetime


def test_bankfirst_session_window_and_clock():
    assert fmp_bankfirst._last_completed_session(datetime(2026, 10, 26, 15, 0, tzinfo=timezone.utc)).isoformat() == "2026-10-23"   # Monday mid-session -> Friday
    assert fmp_bankfirst._last_completed_session(datetime(2026, 10, 26, 22, 0, tzinfo=timezone.utc)).isoformat() == "2026-10-26"   # Monday after settle
    assert fmp_bankfirst._last_completed_session(datetime(2026, 10, 25, 12, 0, tzinfo=timezone.utc)).isoformat() == "2026-10-23"   # Sunday
    assert fmp_bankfirst._in_session_window(datetime(2026, 10, 26, 15, 0, tzinfo=timezone.utc))
    assert not fmp_bankfirst._in_session_window(datetime(2026, 10, 26, 22, 0, tzinfo=timezone.utc))
    assert not fmp_bankfirst._in_session_window(datetime(2026, 10, 25, 15, 0, tzinfo=timezone.utc))
    # during the session a current-dated price request is the provider's (partial bar) even when the bank is at the last close
    rows = [{"date": "2026-10-23", "close": 1}]
    fmp_bank._LAST["key"] = fmp_bank.WALKER_ROOT + "historical-price-eod-full/X.json.gz"
    assert not fmp_bankfirst._fresh_enough("historical-price-eod/full", rows, {}, now=datetime(2026, 10, 26, 15, 0, tzinfo=timezone.utc))
    assert fmp_bankfirst._fresh_enough("historical-price-eod/full", rows, {}, now=datetime(2026, 10, 25, 15, 0, tzinfo=timezone.utc))


def test_bankfirst_bulk_statements_indexed_and_bounded():
    s3 = S3()
    _fresh_bank(s3)
    fmp_bankfirst.datetime = _FixedDT
    R = fmp_bank.ROOT
    s3.store[R + "manifest.json"] = json.dumps({"updated_at": "2026-10-22T23:13:00+00:00", "endpoints_served": ["profile-bulk", "income-statement-bulk"],
                                               "dated_keys": ["income-statement-bulk/%d/annual" % y for y in range(2000, 2026)]}).encode()
    syms = ["S%05d" % i for i in range(20000)]
    _csv(s3, R + "profile-bulk/part-0/latest.csv.gz", "symbol,companyName,description\n" + "".join('%s,Co %s,"a, quoted ""desc""\nover two lines"\n' % (x, x) for x in syms[:10000]))
    _csv(s3, R + "profile-bulk/part-1/latest.csv.gz", "symbol,companyName,description\n" + "".join("%s,Co %s,plain\n" % (x, x) for x in syms[10000:]))
    for y in range(2000, 2026):
        _csv(s3, R + "income-statement-bulk/%d/annual.csv.gz" % y, "symbol,date,period,revenue\n" + "".join("%s,%d-12-31,FY,%d\n" % (x, y, y) for x in syms[:5000]))
    calls = []
    _install(calls)
    try:
        t0 = time.time()
        assert _get("https://financialmodelingprep.com/stable/profile?symbol=S00007&apikey=S") == [{"symbol": "S00007", "companyName": "Co S00007", "description": 'a, quoted "desc"\nover two lines'}]
        assert _get("https://financialmodelingprep.com/stable/profile?symbol=S15000&apikey=S") == [{"symbol": "S15000", "companyName": "Co S15000", "description": "plain"}]
        first = time.time() - t0
        t0 = time.time()
        for i in range(300):
            assert len(_get("https://financialmodelingprep.com/stable/profile?symbol=%s&apikey=S" % syms[(i * 67) % 20000])) == 1
        assert time.time() - t0 < first + 1.5, "indexed lookups must be O(1) after the first parse"
        assert fmp_bank.STATS["table_builds"] == 2 and fmp_bank.STATS["s3_gets"] <= 5, fmp_bank.STATS   # 2 parts + whole-file probe + part-2 probe + manifest
        # statements: explicit bounded limit -> bank, newest first, only the years that can hold it are read
        rows = _get("https://financialmodelingprep.com/stable/income-statement?symbol=S00001&limit=3&apikey=S")
        assert [r["date"] for r in rows] == ["2025-12-31", "2024-12-31", "2023-12-31"] and rows[0]["revenue"] == 2025
        assert fmp_bank.STATS["table_builds"] <= 2 + 5, fmp_bank.STATS
        # no limit, or a deep limit -> the provider defines the depth
        assert _get("https://financialmodelingprep.com/stable/income-statement?symbol=S00001&apikey=S") == {"live": True}
        assert _get("https://financialmodelingprep.com/stable/income-statement?symbol=S00001&limit=40&apikey=S") == {"live": True}
        # bulk freshness is the bank manifest's clock: 3 days old -> live
        fmp_bank._cache.clear()
        s3.store[R + "manifest.json"] = json.dumps({"updated_at": "2026-10-19T23:13:00+00:00"}).encode()
        assert _get("https://financialmodelingprep.com/stable/profile?symbol=S00007&apikey=S") == {"live": True}
        assert fmp_bankfirst.STATS["stale"] == 1
    finally:
        fmp_bankfirst.uninstall()
        fmp_bankfirst.datetime = datetime


def test_bankfirst_steps_aside_when_it_would_cost_more():
    s3 = S3()
    _fresh_bank(s3)
    calls = []
    _install(calls)
    try:
        # 25 consecutive misses on one dataset -> that dataset is live for the rest of the window, no more S3 reads
        for i in range(30):
            _get("https://financialmodelingprep.com/stable/price-target?symbol=Z%d&apikey=S" % i)
        assert "price-target" in fmp_bankfirst._ctl["off_heads"]
        gets = len(s3.gets)
        _get("https://financialmodelingprep.com/stable/price-target?symbol=Z99&apikey=S")
        assert len(s3.gets) == gets and fmp_bankfirst.STATS["bypassed"] >= 1
        # another dataset is unaffected
        _put(s3, fmp_bank.WALKER_ROOT + "key-executives/A.json.gz", [{"symbol": "A"}], pulled_at=datetime.now(timezone.utc).isoformat())
        assert _get("https://financialmodelingprep.com/stable/key-executives?symbol=A&apikey=S") == [{"symbol": "A"}]
        # thrashing tables -> everything live
        fmp_bank.STATS["table_refetches"] = 6
        _get("https://financialmodelingprep.com/stable/key-executives?symbol=A&apikey=S")
        assert fmp_bankfirst._ctl["off"] and "thrash" in fmp_bankfirst._ctl["off"]
        assert _get("https://financialmodelingprep.com/stable/key-executives?symbol=A&apikey=S") == {"live": True}
        # the control window resets after 15 minutes
        fmp_bank.STATS["table_refetches"] = 0
        fmp_bankfirst._ctl["since"] -= 1000
        assert _get("https://financialmodelingprep.com/stable/key-executives?symbol=A&apikey=S") == [{"symbol": "A"}]
        # the marginal rule: slow attempts that keep HITTING are a warm-up and stay; slow attempts with no hit step aside
        real_try_bank = fmp_bank.try_bank
        fmp_bankfirst.RECENT_SLOW_S = 0.01

        def slow_try_bank(path, qs=""):
            time.sleep(0.02)
            return real_try_bank(path, qs)
        fmp_bank.try_bank = slow_try_bank
        try:
            fmp_bankfirst._reset_ctl(time.time())
            for _ in range(12):
                assert _get("https://financialmodelingprep.com/stable/key-executives?symbol=A&apikey=S") == [{"symbol": "A"}]
            assert fmp_bankfirst._ctl["off"] is None, fmp_bankfirst._ctl["off"]
            fmp_bankfirst._reset_ctl(time.time())
            for i in range(12):
                _get("https://financialmodelingprep.com/stable/key-executives?symbol=NOPE%d&apikey=S" % i)
            assert fmp_bankfirst._ctl["off"] and "no hit" in fmp_bankfirst._ctl["off"], fmp_bankfirst._ctl["off"]
        finally:
            fmp_bank.try_bank = real_try_bank
            fmp_bankfirst.RECENT_SLOW_S = 0.5
    finally:
        fmp_bankfirst.uninstall()


def test_fmp_bank_table_budget_evicts_and_counts_refetches():
    s3 = S3()
    _fresh_bank(s3)
    os.environ["FMP_BANK_TEXT_BUDGET_MB"] = "1"
    try:
        R = fmp_bank.ROOT
        big = "symbol,v\n" + "".join("S%05d,%d\n" % (i, i) for i in range(50000))          # ~600 KB each
        for y in (2023, 2024, 2025):
            _csv(s3, R + "ratios-bulk/%d/annual.csv.gz" % y, big)
        assert fmp_bank._rows_for(R + "ratios-bulk/2025/annual.csv.gz", "S00005") == [{"symbol": "S00005", "v": 5}]
        assert fmp_bank._rows_for(R + "ratios-bulk/2024/annual.csv.gz", "S00005") == [{"symbol": "S00005", "v": 5}]
        assert fmp_bank.STATS["table_evictions"] == 1 and not fmp_bank.thrashing()
        for _ in range(3):
            fmp_bank._rows_for(R + "ratios-bulk/2025/annual.csv.gz", "S1")
            fmp_bank._rows_for(R + "ratios-bulk/2024/annual.csv.gz", "S1")
        assert fmp_bank.STATS["table_refetches"] >= 6 and fmp_bank.thrashing()
        # a missing object is remembered: one S3 read, not one per lookup
        n = len(s3.gets)
        for _ in range(5):
            assert fmp_bank._rows_for(R + "ratios-bulk/1999/annual.csv.gz", "S1") is None
        assert len(s3.gets) == n + 1
    finally:
        os.environ.pop("FMP_BANK_TEXT_BUDGET_MB", None)


def test_bankfirst_threads_never_mix_up_freshness():
    """Engines call FMP from thread pools: the freshness guard must judge each thread's own answer (bulk vs walker,
    its own pulled_at), never a neighbour's."""
    from concurrent.futures import ThreadPoolExecutor
    s3 = S3()
    _fresh_bank(s3)
    fmp_bankfirst.datetime = _FixedDT
    R, W = fmp_bank.ROOT, fmp_bank.WALKER_ROOT
    s3.store[R + "manifest.json"] = json.dumps({"updated_at": "2026-10-22T23:13:00+00:00"}).encode()
    _csv(s3, R + "profile-bulk/latest.csv.gz", "symbol,companyName\n" + "".join("P%03d,Co %d\n" % (i, i) for i in range(300)))
    for i in range(60):
        _put(s3, W + "grades/G%03d.json.gz" % i, [{"symbol": "G%03d" % i}], pulled_at="2026-10-02T20:00:00Z")          # 20 days: must go live
        _put(s3, W + "price-target/T%03d.json.gz" % i, [{"symbol": "T%03d" % i}], pulled_at="2026-10-22T20:00:00Z")    # fresh: bank
    calls = []
    lock = __import__("threading").Lock()

    def fake_real(url, data=None, timeout=None, *a, **k):
        with lock:
            calls.append(url)
        r = io.BytesIO(b'{"live": true}')
        r.__enter__ = lambda s=r: s
        r.__exit__ = lambda s=r, *x: None
        return r
    fmp_bankfirst.uninstall()
    urllib.request.urlopen = fake_real
    fmp_bankfirst.install()
    real_try_bank = fmp_bank.try_bank

    def preempted_try_bank(path, qs=""):
        rows = real_try_bank(path, qs)
        time.sleep(0.003)                       # a thread switch right after the answer, before the freshness check
        return rows
    fmp_bank.try_bank = preempted_try_bank
    try:
        urls = []
        for i in range(60):
            urls += ["https://financialmodelingprep.com/stable/profile?symbol=P%03d&apikey=S" % i,
                     "https://financialmodelingprep.com/stable/grades?symbol=G%03d&apikey=S" % i,
                     "https://financialmodelingprep.com/stable/price-target?symbol=T%03d&apikey=S" % i]
        with ThreadPoolExecutor(max_workers=12) as ex:
            results = list(ex.map(lambda u: (u, _get(u)), urls))
        for u, got in results:
            if "/grades?" in u:
                assert got == {"live": True}, (u, got)                       # a 20-day-old fast object never passes
            else:
                assert isinstance(got, list) and len(got) == 1 and got[0]["symbol"] == u.split("symbol=")[1][:4], (u, got)
        assert fmp_bankfirst.STATS["hits"] == 120 and fmp_bankfirst.STATS["stale"] == 60, fmp_bankfirst.STATS
        assert len(calls) == 60 and all("/grades?" in c for c in calls)
    finally:
        fmp_bank.try_bank = real_try_bank
        fmp_bankfirst.uninstall()
        fmp_bankfirst.datetime = datetime


def test_bankfirst_never_installs_inside_the_banks():
    fmp_bankfirst.uninstall()
    real = urllib.request.urlopen
    old = os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
    try:
        for fn in ("justhodl-fmp-bank", "justhodl-fmp-walker"):
            os.environ["AWS_LAMBDA_FUNCTION_NAME"] = fn
            fmp_bankfirst.install()
            assert urllib.request.urlopen is real and not fmp_bankfirst._installed["done"], fn
    finally:
        if old is None:
            os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)
        else:
            os.environ["AWS_LAMBDA_FUNCTION_NAME"] = old
        fmp_bankfirst.uninstall()
    r = subprocess.run([sys.executable, os.path.join(ROOT, "scripts", "fmp_bankfirst_apply.py"), "justhodl-fmp-bank"], capture_output=True, text=True)
    assert r.returncode != 0 and "refusing" in (r.stdout + r.stderr)
    for fn in ("justhodl-fmp-bank", "justhodl-fmp-walker"):
        assert "from fmp_bankfirst import install" not in open(os.path.join(ROOT, "aws", "lambdas", fn, "source", "lambda_function.py")).read()


if __name__ == "__main__":
    for t in (test_bankfirst_meaning_and_freshness_guards, test_bankfirst_session_window_and_clock, test_bankfirst_bulk_statements_indexed_and_bounded,
              test_bankfirst_steps_aside_when_it_would_cost_more, test_fmp_bank_table_budget_evicts_and_counts_refetches,
              test_bankfirst_threads_never_mix_up_freshness, test_bankfirst_never_installs_inside_the_banks):
        t()
        print("ok", t.__name__)
