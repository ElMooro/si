"""justhodl-eia-bank offline tests: a fake EIA v2 server (tree + paged data) and in-memory S3."""
import gzip
import io
import json
import os
import sys
import urllib.error
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "source"), os.path.join(HERE, "..", "..", "..", "shared")]
os.environ["EIA_API_KEY"] = "k-eia"
os.environ["EIA_BANK_BUDGET_S"] = "60"
os.environ["EIA_BANK_RPS"] = "100000"
os.environ["EIA_BANK_PAGE"] = "3"
os.environ["EIA_BANK_PAGES_PER_LEAF"] = "2"


class S3Stub:
    def __init__(self):
        self.store, self.meta, self.puts = {}, {}, 0

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.store[Key])}

    def head_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Metadata": self.meta.get(Key, {})}

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body.read() if hasattr(Body, "read") else Body
        self.meta[Key] = kw.get("Metadata") or {}
        self.puts += 1


def J(x):
    return json.dumps(x).encode()


TREE = {"": ["petroleum", "natural-gas", "steo"], "/petroleum": ["pri"], "/petroleum/pri": ["spt"], "/natural-gas": ["stor"]}
LEAVES = {"/petroleum/pri/spt": {"frequency": [{"id": "daily"}, {"id": "weekly"}], "defaultFrequency": "daily", "data": {"value": {}}, "facets": [{"id": "series"}], "startPeriod": "1986-01-02", "endPeriod": "2026-10-24", "name": "Spot Prices"},
          "/natural-gas/stor": {"frequency": [{"id": "weekly"}], "data": {"value": {}}, "facets": [], "startPeriod": "2010-01-01", "endPeriod": "2026-10-17"},
          "/steo": {"frequency": [{"id": "monthly"}], "data": {"value": {}}, "facets": [], "startPeriod": "1997-01", "endPeriod": "2027-12"}}
ROWS = {"/petroleum/pri/spt": 7, "/natural-gas/stor": 2, "/steo": 0}


class FakeEIA:
    def __init__(self):
        self.calls, self.by_path = [], {}
        self.refuse = set()

    def __call__(self, req, timeout=0):
        url = req.full_url if hasattr(req, "full_url") else req
        self.calls.append(url)
        u = urllib.parse.urlparse(url)
        path = u.path.replace("/v2", "", 1).rstrip("/")
        q = dict(urllib.parse.parse_qsl(u.query))
        assert q.get("api_key") == "k-eia"
        self.by_path[path] = self.by_path.get(path, 0) + 1
        if path in self.refuse:
            raise urllib.error.HTTPError(url, 403, "Forbidden", {}, io.BytesIO(J({"error": "nope", "code": 403})))
        if path.endswith("/data"):
            leaf = path[:-5]
            n = ROWS.get(leaf, 0)
            rows = [{"period": "2026-%02d-01" % (i + 1), "value": i} for i in range(n)]
            if q.get("sort[0][direction]") == "desc":
                rows = rows[::-1]
            off, length = int(q.get("offset", 0)), int(q["length"])
            page = rows[off:off + length]
            return self._resp(J({"response": {"total": n, "data": page}}))
        if path in TREE:
            return self._resp(J({"response": {"id": path.strip("/") or "root", "routes": [{"id": r} for r in TREE[path]]}}))
        if path in LEAVES:
            return self._resp(J({"response": LEAVES[path]}))
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, io.BytesIO(J({"error": "x", "code": 404})))

    @staticmethod
    def _resp(body):
        resp = io.BytesIO(body)
        resp.__enter__ = lambda s=resp: s
        resp.__exit__ = lambda s=resp, *a: None
        return resp


def boot():
    import boto3
    s3 = S3Stub()
    boto3.client = lambda *a, **k: s3
    import urllib.request as ur
    fake = FakeEIA()
    ur.urlopen = fake
    for m in list(sys.modules):
        if m == "lambda_function":
            del sys.modules[m]
    import lambda_function as lf
    lf.s3 = s3
    return lf, s3, fake


def gz(s3, key):
    return json.loads(gzip.decompress(s3.store[key]))


def test_tree_leaves_latest_and_paged_stream():
    lf, s3, fake = boot()
    out = lf.lambda_handler({}, None)
    assert out["leaves"] == 3 and out["key_status"] == "ok", out
    assert lf.ROOT + "_catalog/leaves.json.gz" in s3.store
    assert lf.ROOT + "petroleum_pri_spt/_meta.json.gz" in s3.store
    # latest file per leaf (desc) + ascending pages from the cursor, PAGES_PER_LEAF per run
    assert any(k.startswith(lf.ROOT + "petroleum_pri_spt/daily/latest/") for k in s3.store)
    pages = sorted(k for k in s3.store if k.startswith(lf.ROOT + "petroleum_pri_spt/daily/pages/"))
    assert pages == [lf.ROOT + "petroleum_pri_spt/daily/pages/p000000.json.gz", lf.ROOT + "petroleum_pri_spt/daily/pages/p000001.json.gz"], pages
    assert gz(s3, pages[0])["data"][0]["period"] == "2026-01-01"
    st = json.loads(s3.store[lf.ROOT + "_state/state.json"])
    assert st["leaf_state"]["/petroleum/pri/spt"] == {"offset": 6, "pages": 2, "complete": False, "total": 7, "last_run": st["leaf_state"]["/petroleum/pri/spt"]["last_run"]}
    assert st["leaf_state"]["/natural-gas/stor"]["complete"] is True
    assert st["leaf_state"]["/steo"]["complete"] is True and st["leaf_state"]["/steo"]["pages"] == 0
    man = json.loads(s3.store[lf.ROOT + "manifest.json"])
    assert man["leaves"] == 3 and man["leaves_complete"] == 2 and man["phase"] == "BACKFILL"
    assert man["objects"] == s3.puts - 4, (man["objects"], s3.puts)   # state x2 + manifest + catalog are not bank objects
    # second run: finishes the stream (page 2), re-pulls only the last page of complete leaves, tree not re-walked (weekly)
    import urllib.request as ur
    fake2 = FakeEIA()
    ur.urlopen = fake2
    out2 = lf.lambda_handler({}, None)
    assert fake2.by_path.get("") is None, "tree must not be re-walked within a week"
    st = json.loads(s3.store[lf.ROOT + "_state/state.json"])
    assert st["leaf_state"]["/petroleum/pri/spt"]["complete"] is True and st["leaf_state"]["/petroleum/pri/spt"]["offset"] == 7
    assert out2["unchanged"] >= 2
    man = json.loads(s3.store[lf.ROOT + "manifest.json"])
    assert man["leaves_complete"] == 3 and man["phase"] == "STEADY"


def test_refused_leaf_is_recorded_and_the_rest_proceeds():
    lf, s3, fake = boot()
    fake.refuse.add("/steo/data")
    out = lf.lambda_handler({}, None)
    cat = json.loads(s3.store[lf.ROOT + "catalog.json"])
    assert cat["leaves"]["/steo"] == "http403", cat["leaves"]
    assert cat["leaves"]["/natural-gas/stor"] in ("ok", "empty")
    man = json.loads(s3.store[lf.ROOT + "manifest.json"])
    assert ["/steo", "http403"] in [list(x) for x in man["endpoints_refused"]]


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print("ok", t.__name__)
    print("%d tests passed" % len(tests))
