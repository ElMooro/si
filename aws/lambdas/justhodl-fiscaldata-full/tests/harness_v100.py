"""Harness justhodl-fiscaldata-full v1.0.0 (boto3+urllib stubbed).
Proves: docs-harvest+seed union, probe validation (invalid named),
paginated drain -> JSONL.gz rows, phase COMPLETE, delta refresh
requeues moved endpoint, redrain. Exit 0 = OK."""
import gzip, io, json, sys, types

S3MEM = {}
class _Body:
    def __init__(s, b): s.b = b
    def read(s): return s.b
class _S3:
    def get_object(s, Bucket, Key):
        if Key not in S3MEM: raise KeyError(Key)
        return {"Body": _Body(S3MEM[Key])}
    def put_object(s, Body, Bucket, Key, **kw): S3MEM[Key] = Body
    def upload_file(s, fn, Bucket, Key, ExtraArgs=None):
        S3MEM[Key] = open(fn, "rb").read()
boto3 = types.ModuleType("boto3"); boto3.client = lambda *a, **k: _S3()
sys.modules["boto3"] = boto3

sys.path.insert(0, "aws/lambdas/justhodl-fiscaldata-full/source")
import lambda_function as L
L.SEEDS = ["v1/x/good_a", "v1/x/gone"]
L.PAGE_SIZE = 2
L.time.sleep = lambda *_: None
L.SPACING = 0

DATA = {"v1/x/good_a": [{"record_date": "1979-11-15", "v": 1},
                        {"record_date": "1990-01-01", "v": 2},
                        {"record_date": "2026-08-01", "v": 3}],
        "v1/y/good_b": [{"record_date": "2005-10-03", "v": 9}]}

class Resp:
    def __init__(s, b): s._b = b
    def read(s, amt=None):
        b, s._b = (s._b, b"") if amt is None else \
            (s._b[:amt], s._b[amt:])
        return b
    def __enter__(s): return s
    def __exit__(s, *a): return False

def fake_urlopen(req, timeout=None):
    import urllib.error, re
    url = req.full_url
    if url.startswith(L.DOCS):
        return Resp(b'href="/datasets/x" /v1/y/good_b more '
                    b'/v1/x/good_a dup')
    m = re.search(r"fiscal_service/(v1/[a-z_/]+)\?", url)
    assert m, url
    ep = m.group(1)
    if ep == "v1/x/gone":
        raise urllib.error.HTTPError(url, 404, "nf", {},
                                     io.BytesIO(b""))
    rows = DATA[ep]
    ps = int(re.search(r"page\[size\]=(\d+)", url).group(1))
    pn = int((re.search(r"page\[number\]=(\d+)", url) or
              [None, "1"])[1])
    if "sort=-record_date" in url:
        newest = sorted(rows, key=lambda r: r["record_date"])[-1]
        return Resp(json.dumps({
            "meta": {"total-count": len(rows)},
            "data": [newest]}).encode())
    tp = (len(rows) + ps - 1) // ps
    chunk = rows[(pn - 1) * ps: pn * ps]
    return Resp(json.dumps({
        "meta": {"total-count": len(rows), "total-pages": tp},
        "data": chunk}).encode())

L.urllib.request.urlopen = fake_urlopen

fails = []
def ck(n, c, d=""):
    print(("PASS " if c else "FAIL ") + n + (" " + str(d) if d else ""))
    if not c: fails.append(n)

out1 = L.lambda_handler({}, None)
st = json.loads(S3MEM[L.STATE_KEY])
ck("phase COMPLETE", st["phase"] == "COMPLETE", st["phase"])
ck("universe 2 valid", set(st["universe"]) ==
   {"v1/x/good_a", "v1/y/good_b"}, list(st["universe"]))
ck("invalid named", st["invalid"].get("v1/x/gone") == "HTTP 404",
   st["invalid"])
ck("banked 2", st["n_banked"] == 2 and out1["banked"] == 2, None)
ck("rows 4", st["rows_total"] == 4, st["rows_total"])
raw = gzip.decompress(S3MEM[L.ROOT + "src/v1_x_good_a.jsonl.gz"])
lines = [json.loads(x) for x in raw.decode().splitlines()]
ck("jsonl paginated 3 rows", len(lines) == 3 and
   lines[0]["record_date"] == "1979-11-15", len(lines))
ck("pages tracked", st["have"]["v1/x/good_a"]["pages"] == 2,
   st["have"]["v1/x/good_a"])
man = json.loads(S3MEM[L.MANIFEST_KEY])
ck("manifest", man["endpoints_banked"] == 2 and man["rows"] == 4
   and man["invalid_named"] == 1, man)

# delta refresh: good_b gains a row -> requeued + redrained ---------
DATA["v1/y/good_b"].append({"record_date": "2026-08-24", "v": 10})
out2 = L.lambda_handler({}, None)
st2 = json.loads(S3MEM[L.STATE_KEY])
ck("refresh requeued+drained", st2["phase"] == "COMPLETE" and
   st2["have"]["v1/y/good_b"]["rows"] == 2 and
   st2["last_refresh_check"]["requeued"] == 1,
   st2.get("last_refresh_check"))
ck("rows now 5", st2["rows_total"] == 5, st2["rows_total"])

out3 = L.lambda_handler({"redrain": True}, None)
st3 = json.loads(S3MEM[L.STATE_KEY])
ck("redrain full", st3["phase"] == "COMPLETE" and
   st3["rows_total"] == 5, st3["rows_total"])
need = ["phase", "queue", "have", "failures", "universe",
        "invalid", "n_banked", "rows_total", "as_of"]
ck("state contract", all(k in st3 for k in need),
   [k for k in need if k not in st3])
print("HARNESS " + ("GREEN" if not fails else "RED: " + ",".join(fails)))
sys.exit(1 if fails else 0)
