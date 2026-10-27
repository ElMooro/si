"""justhodl-worldbank-full v1.1.0 offline tests - boto3, urllib and chain_guard stubbed; no network, no AWS.

v1.0.0's contract (catalog discovery, verbatim zips, no-data named, 404 3-strike, redrain, state/manifest keys) plus
the v1.1.0 fix for the +75 GB/day S3 growth: the state is written a bounded number of times per run (not twice per
indicator), the per-indicator crash record is a tiny marker, a crashed attempt still counts toward quarantine, a v1.0.0
plain-JSON state is read, and an unchanged indicator is not rewritten."""
from pathlib import Path
import gzip
import io
import json
import re
import sys
import time as _time
REAL_TIME = _time.time      # captured before any test patches the time module's clock
import types
import unittest
import urllib.error
import urllib.parse
import zipfile

ROOT = Path(__file__).resolve().parents[4]
S3MEM, PUTS = {}, []


class _Body:
    def __init__(self, b):
        self.b = b

    def read(self):
        return self.b


class _S3:
    def get_object(self, Bucket, Key):
        if Key not in S3MEM:
            raise KeyError(Key)
        return {"Body": _Body(S3MEM[Key])}

    def put_object(self, Body, Bucket, Key, **kw):
        S3MEM[Key] = Body
        PUTS.append((Key, len(Body)))

    def head_object(self, Bucket, Key):
        return {"ContentLength": len(S3MEM[Key])}

    def upload_fileobj(self, fobj, Bucket, Key, ExtraArgs=None):
        out = b""
        while True:
            c = fobj.read(65536)
            if not c:
                break
            out += c
        S3MEM[Key] = out
        PUTS.append((Key, len(out)))


sys.modules["boto3"] = types.SimpleNamespace(client=lambda *a, **k: _S3())
CHAINS = []
sys.modules["chain_guard"] = types.SimpleNamespace(begin=lambda event: None,
                                                   chain_invoke=lambda payload, context=None: CHAINS.append(payload))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-worldbank-full" / "source"))
import lambda_function as L  # noqa: E402


def zbytes(name, rows, stamp=(2026, 1, 1, 0, 0, 0)):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(zipfile.ZipInfo(name, date_time=stamp), "Country Name,1960,2024\n" + rows)
        z.writestr(zipfile.ZipInfo("Metadata_" + name, date_time=stamp), "Indicator,Source\nX,WDI\n")
    return buf.getvalue()


IDS = ["NY.GDP.MKTP.CD", "SP.POP.TOTL", "ZZ.NODATA.X", "ZZ.GONE.404", "ZZ.NOTZIP"]
ZIPS = {}
MANY = []                                  # extra indicators for the write-count tests


class Resp:
    def __init__(self, body=b"", ct="application/zip"):
        self._b = body
        self.headers = types.SimpleNamespace(get=lambda k, d=None: {"Content-Type": ct}.get(k, d))

    def read(self, amt=None):
        b, self._b = (self._b, b"") if amt is None else (self._b[:amt], self._b[amt:])
        return b

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def fake_urlopen(req, timeout=None):
    url = req.full_url
    if "/v2/indicator?" in url:
        page = int(re.search(r"[?&]page=(\d+)", url).group(1))
        ids = IDS + MANY
        half = (len(ids) + 1) // 2
        rows = [{"id": i, "name": "n-" + i} for i in (ids[:half] if page == 1 else ids[half:])]
        return Resp(json.dumps([{"pages": 2, "page": page}, rows]).encode(), ct="application/json")
    m = re.search(r"/v2/en/indicator/([^?]+)\?downloadformat=csv", url)
    if m:
        iid = urllib.parse.unquote(m.group(1))
        if iid == "ZZ.GONE.404":
            raise urllib.error.HTTPError(url, 404, "nf", {}, io.BytesIO(b""))
        if iid == "ZZ.NODATA.X":
            return Resp(b"<!DOCTYPE html><html>No data</html>", ct="text/html")
        if iid == "ZZ.NOTZIP":
            return Resp(b'{"message": "an error body that is neither html nor a zip"}', ct="application/octet-stream")
        return Resp(ZIPS[iid])
    raise AssertionError("unexpected URL " + url)


L.urllib.request.urlopen = fake_urlopen
L.time.sleep = lambda *_: None
L.SPACING = 0


def state():
    return json.loads(gzip.decompress(S3MEM[L.STATE_KEY]))


def reset():
    S3MEM.clear()
    PUTS.clear()
    CHAINS.clear()
    MANY.clear()
    ZIPS.clear()
    ZIPS.update({"NY.GDP.MKTP.CD": zbytes("gdp.csv", "USA,543,27000\n" * 50), "SP.POP.TOTL": zbytes("pop.csv", "USA,180,340\n" * 30)})


class Tests(unittest.TestCase):
    def setUp(self):
        reset()
        L.CHECKPOINT_S = 10 ** 6            # a run shorter than one checkpoint interval
        L.time.time = REAL_TIME

    def test_v100_contract_discovery_mirror_no_data_404_manifest(self):
        out = L.lambda_handler({}, None)
        st = state()
        self.assertEqual((st["phase"], st["n_indicators"], st["n_banked"], out["banked"]), ("COMPLETE", 5, 2, 2))
        self.assertEqual(st["have"]["ZZ.NODATA.X"]["status"], "no_data")
        self.assertEqual((st["failures"]["ZZ.GONE.404"]["err"], st["failures"]["ZZ.GONE.404"]["tries"]), ("HTTP 404", 3))
        self.assertIn("ZZ.NOTZIP", st["failures"])                       # a non-zip body is a failure, never banked
        self.assertNotIn(L.ROOT + "src/ZZ.NOTZIP.zip", S3MEM)
        self.assertEqual(S3MEM[L.ROOT + "src/NY.GDP.MKTP.CD.zip"], ZIPS["NY.GDP.MKTP.CD"])   # verbatim
        self.assertEqual(st["bytes_total"], sum(len(v) for v in ZIPS.values()))
        self.assertEqual(json.loads(gzip.decompress(S3MEM[L.CATALOG_KEY]))["n"], 5)
        man = json.loads(S3MEM[L.MANIFEST_KEY])
        self.assertEqual((man["banked"], man["no_data"], man["failures"], man["phase"], man["version"]), (2, 1, 2, "COMPLETE", "1.1.1"))
        self.assertFalse(out["chained"])
        self.assertTrue(all(k in st for k in ("phase", "queue", "have", "failures", "n_indicators", "n_banked", "bytes_total", "as_of", "version")))

    def test_the_state_is_written_a_bounded_number_of_times_not_twice_per_indicator(self):
        MANY.extend("XX.IND.%03d" % i for i in range(200))
        for i in MANY:
            ZIPS[i] = zbytes(i + ".csv", "USA,1,2\n")
        L.lambda_handler({}, None)
        state_writes = sum(1 for k, _ in PUTS if k == L.STATE_KEY)
        markers = [n for k, n in PUTS if k == L.INFLIGHT_KEY]
        self.assertLessEqual(state_writes, 4)                            # lease, discover, end - none per indicator
        self.assertEqual(markers, [])                                    # a run after a clean run writes no crash records
        self.assertEqual(state()["n_banked"], 202)
        self.assertFalse(state()["run_open"])

    def test_a_run_after_a_crash_is_careful_and_writes_one_marker_per_indicator(self):
        S3MEM[L.STATE_KEY] = gzip.compress(json.dumps({"version": "1.1.1", "phase": "DRAIN", "run_open": True,
                                                       "queue": [["NY.GDP.MKTP.CD", 0], ["SP.POP.TOTL", 0]], "have": {}, "failures": {}}).encode())
        out = L.lambda_handler({}, None)
        markers = [k for k, _ in PUTS if k == L.INFLIGHT_KEY]
        self.assertTrue(out["careful"])
        self.assertEqual(len(markers), 3)                                 # two indicators + the clean-exit clear
        self.assertIsNone(json.loads(S3MEM[L.INFLIGHT_KEY])["iid"])
        self.assertFalse(state()["run_open"])

    def test_checkpoints_follow_the_clock(self):
        MANY.extend("XX.IND.%03d" % i for i in range(30))
        for i in MANY:
            ZIPS[i] = zbytes(i + ".csv", "USA,1,2\n")
        clock = [1000.0]

        def tick():
            clock[0] += 7.0                                              # 7 s per clock read
            return clock[0]
        L.time.time = tick
        L.CHECKPOINT_S = 120
        L.lambda_handler({}, None)
        writes = sum(1 for k, _ in PUTS if k == L.STATE_KEY)
        self.assertGreater(writes, 3)
        self.assertLess(writes, 35)                                       # far fewer than 2 x 35 indicators

    def test_a_v100_plain_json_state_is_read_and_continued(self):
        S3MEM[L.STATE_KEY] = json.dumps({"version": "1.0.0", "phase": "DRAIN", "queue": [["SP.POP.TOTL", 1]],
                                         "have": {"NY.GDP.MKTP.CD": {"status": "fresh", "bytes": 10}}, "failures": {}}, indent=1).encode()
        out = L.lambda_handler({}, None)
        st = state()
        self.assertEqual((out["phase"], st["version"], st["have"]["SP.POP.TOTL"]["status"]), ("COMPLETE", "1.1.1", "fresh"))
        self.assertIn("NY.GDP.MKTP.CD", st["have"])

    def test_a_crash_mid_indicator_still_counts_toward_the_quarantine(self):
        S3MEM[L.STATE_KEY] = gzip.compress(json.dumps({"version": "1.1.0", "phase": "DRAIN", "queue": [["SP.POP.TOTL", 2]],
                                                       "run_open": True, "have": {}, "failures": {}}).encode())
        S3MEM[L.INFLIGHT_KEY] = json.dumps({"iid": "SP.POP.TOTL", "attempt": 4}).encode()   # the run that died
        out = L.lambda_handler({}, None)
        st = state()
        self.assertEqual(out["recovered_inflight"], "SP.POP.TOTL")
        self.assertEqual(st["failures"]["SP.POP.TOTL"]["err"], "quarantined after 4 dead attempts")
        self.assertIsNone(json.loads(S3MEM[L.INFLIGHT_KEY])["iid"])                       # clean exit clears it

    def test_a_redrain_rewrites_only_what_changed(self):
        L.lambda_handler({}, None)
        first = {k: v for k, v in S3MEM.items() if k.startswith(L.ROOT + "src/")}
        PUTS.clear()
        ZIPS["NY.GDP.MKTP.CD"] = zbytes("gdp.csv", "USA,543,27000\n" * 50, stamp=(2026, 10, 27, 9, 0, 0))   # same data, new zip stamps
        ZIPS["SP.POP.TOTL"] = zbytes("pop.csv", "USA,180,341\n" * 40)                                      # new data
        out = L.lambda_handler({"redrain": True}, None)
        uploaded = [k for k, _ in PUTS if k.startswith(L.ROOT + "src/")]
        self.assertEqual(uploaded, [L.ROOT + "src/SP.POP.TOTL.zip"])
        self.assertEqual(S3MEM[L.ROOT + "src/NY.GDP.MKTP.CD.zip"], first[L.ROOT + "src/NY.GDP.MKTP.CD.zip"])
        self.assertEqual((out["run"]["uploaded"], out["run"]["unchanged"]), (1, 1))
        self.assertEqual(state()["bytes_total"], len(first[L.ROOT + "src/NY.GDP.MKTP.CD.zip"]) + len(ZIPS["SP.POP.TOTL"]))


if __name__ == "__main__":
    result = unittest.main(exit=False, verbosity=2).result
    sys.exit(0 if result.wasSuccessful() else 1)
