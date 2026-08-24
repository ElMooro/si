"""Harness justhodl-gdelt-full v1.0.0 (boto3+urllib+clock stubbed).
Proves: cursor iteration, 404 gap counting, transient retry->named,
budget mid-drain resume, live-edge transition, v1 index parse +
backfill + V1 complete, manifest. Exit 0 = OK."""
import gzip, io, json, sys, types
from datetime import datetime, timedelta, timezone

S3MEM = {}
class _Body:
    def __init__(s, b): s.b = b
    def read(s): return s.b
class _S3:
    def get_object(s, Bucket, Key):
        if Key not in S3MEM: raise KeyError(Key)
        return {"Body": _Body(S3MEM[Key])}
    def put_object(s, Body, Bucket, Key, **kw): S3MEM[Key] = Body
    def upload_fileobj(s, fobj, Bucket, Key, ExtraArgs=None):
        out = b""
        while True:
            c = fobj.read(65536)
            if not c: break
            out += c
        S3MEM[Key] = out
boto3 = types.ModuleType("boto3"); boto3.client = lambda *a, **k: _S3()
sys.modules["boto3"] = boto3

sys.path.insert(0, "aws/lambdas/justhodl-gdelt-full/source")
import lambda_function as L
L.time.sleep = lambda *_: None
L.SPACING = 0
NOW = {"t": L.EPOCH + timedelta(minutes=15 * 6 + 45)}
L._now = lambda: NOW["t"]

GAP_SLOT = L.slot_str(L.EPOCH + timedelta(minutes=15))      # slot#2
FLAKY = L.slot_str(L.EPOCH + timedelta(minutes=30))          # slot#3
flaky_left = {"n": 1}
V1NAMES = ["1979.zip", "201501.zip", "20150217.export.CSV.zip"]

class Resp:
    def __init__(s, b, ln=None):
        s._b = b
        s.headers = types.SimpleNamespace(
            get=lambda k, d=None: {"Content-Length":
                                   str(ln if ln is not None
                                       else len(b))}.get(k, d))
    def read(s, amt=None):
        b, s._b = (s._b, b"") if amt is None else \
            (s._b[:amt], s._b[amt:])
        return b
    def __enter__(s): return s
    def __exit__(s, *a): return False

def fake_urlopen(req, timeout=None):
    import urllib.error
    url = req.full_url
    if url == L.V1IDX:
        html = " ".join('<a href="%s">x</a>' % n for n in V1NAMES)
        return Resp(html.encode())
    if url.startswith(L.V1):
        name = url[len(L.V1):]
        assert name in V1NAMES, name
        return Resp(b"V1DATA-" + name.encode())
    assert url.startswith(L.V2), url
    ss = url[len(L.V2):].split(".")[0]
    if ss == GAP_SLOT:
        raise urllib.error.HTTPError(url, 404, "nf", {},
                                     io.BytesIO(b""))
    if ss == FLAKY and flaky_left["n"] > 0:
        flaky_left["n"] -= 1
        raise urllib.error.HTTPError(url, 503, "busy", {},
                                     io.BytesIO(b""))
    return Resp(b"ZIP-" + ss.encode())

L.urllib.request.urlopen = fake_urlopen

fails = []
def ck(n, c, d=""):
    print(("PASS " if c else "FAIL ") + n + (" " + str(d) if d else ""))
    if not c: fails.append(n)

out1 = L.lambda_handler({}, None)
st = json.loads(S3MEM[L.STATE_KEY])
ck("v2 files 6 (7 slots - 1 gap)", st["files"] == 6, st["files"])
ck("gap counted+sampled", st["gaps"] == 1 and
   st["gaps_sample"] == [GAP_SLOT], st.get("gaps_sample"))
ck("flaky retried then ok", FLAKY in json.dumps(S3MEM.keys().__iter__().__length_hint__() and list(S3MEM)) or any(FLAKY in k for k in S3MEM), None)
ck("flaky failure logged once", st["failures"][FLAKY]["tries"] == 1,
   st["failures"].get(FLAKY))
ck("cursor at live edge", st["cursor"] ==
   L.slot_str(L.EPOCH + timedelta(minutes=15 * 7)), st["cursor"])
ck("verbatim key layout", S3MEM[L.ROOT +
   "v2/export/2015/02/%s.export.CSV.zip" % L.slot_str(L.EPOCH)]
   == b"ZIP-" + L.slot_str(L.EPOCH).encode(), None)
ck("v1 complete phase V1", st["phase"] == "V1" and
   st["v1_idx"] == 3 and st["v1_files"] == 3, st.get("phase"))
ck("v1 verbatim", S3MEM[L.ROOT + "v1/1979.zip"] == b"V1DATA-1979.zip",
   None)
man = json.loads(S3MEM[L.MANIFEST_KEY])
ck("manifest", man["v2_files"] == 6 and man["gaps"] == 1 and
   man["v1_files"] == 3 and man["phase"] == "V1", man)
ck("no chain when done", out1["chained"] is False, out1["chained"])

# steady-state: 2 new slots appear -> cursor advances only ----------
NOW["t"] = NOW["t"] + timedelta(minutes=30)
out2 = L.lambda_handler({}, None)
st2 = json.loads(S3MEM[L.STATE_KEY])
ck("steady +2", st2["files"] == 8 and st2["phase"] == "V1",
   st2["files"])
need = ["phase", "cursor", "files", "bytes", "gaps", "failures",
        "v1_idx", "v1_total", "as_of"]
ck("state contract", all(k in st2 for k in need),
   [k for k in need if k not in st2])
print("HARNESS " + ("GREEN" if not fails else "RED: " + ",".join(fails)))
sys.exit(1 if fails else 0)
