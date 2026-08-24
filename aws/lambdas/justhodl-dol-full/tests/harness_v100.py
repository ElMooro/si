"""Harness justhodl-dol-full v1.0.0. Proves: harvest+resolve, fresh
mirror w/ metadata, unchanged skip on rerun, changed refetch, HEAD-
less fallback, 3-strike named failure, harvest-fail fallback to known
universe, manifest. Exit 0 = OK."""
import json, sys, types, io

S3MEM, S3META = {}, {}
class _Body:
    def __init__(s, b): s.b = b
    def read(s): return s.b
class _S3:
    def get_object(s, Bucket, Key):
        if Key not in S3MEM: raise KeyError(Key)
        return {"Body": _Body(S3MEM[Key])}
    def put_object(s, Body, Bucket, Key, **kw): S3MEM[Key] = Body
    def head_object(s, Bucket, Key):
        if Key not in S3MEM: raise KeyError(Key)
        return {"ContentLength": len(S3MEM[Key]),
                "Metadata": S3META.get(Key, {})}
    def upload_fileobj(s, f, Bucket, Key, ExtraArgs=None):
        out = b""
        while True:
            c = f.read(65536)
            if not c: break
            out += c
        S3MEM[Key] = out
        S3META[Key] = (ExtraArgs or {}).get("Metadata", {})
boto3 = types.ModuleType("boto3"); boto3.client = lambda *a, **k: _S3()
sys.modules["boto3"] = boto3
sys.path.insert(0, "aws/lambdas/justhodl-dol-full/source")
import lambda_function as L
L.time.sleep = lambda *_: None
L.SPACING = 0

FILES = {"ar539.csv": ("Mon, 01 Jan 2024 00:00:00 GMT",
                       b"st,c1\nAL,5\n" * 20),
         "ac207.csv": ("Tue, 02 Jan 2024 00:00:00 GMT",
                       b"a,b\n1,2\n")}
NOHEAD = "ae207.csv"
GONE = "zz999.csv"
FILES[NOHEAD] = (None, b"x,y\n9,9\n")
harvest_ok = {"v": True}

class Resp:
    def __init__(s, b, hdr):
        s._b = b
        s.headers = types.SimpleNamespace(
            get=lambda k, d=None: hdr.get(k, d))
    def read(s, amt=None):
        b, s._b = (s._b, b"") if amt is None else \
            (s._b[:amt], s._b[amt:])
        return b
    def __enter__(s): return s
    def __exit__(s, *a): return False

def fake_urlopen(req, timeout=None):
    import urllib.error
    url, meth = req.full_url, req.get_method()
    if url == L.PAGE:
        if not harvest_ok["v"]:
            raise urllib.error.HTTPError(url, 503, "b", {},
                                         io.BytesIO(b""))
        links = "".join('<a href="csv/%s">x</a>' % n
                        for n in list(FILES) + [GONE])
        return Resp(links.encode(), {})
    name = url.rsplit("/", 1)[-1]
    if name == GONE:
        raise urllib.error.HTTPError(url, 404, "nf", {},
                                     io.BytesIO(b""))
    lm, body = FILES[name]
    if meth == "HEAD":
        if lm is None:
            raise urllib.error.HTTPError(url, 405, "no", {},
                                         io.BytesIO(b""))
        return Resp(b"", {"Last-Modified": lm,
                          "Content-Length": str(len(body))})
    hdr = {"Content-Length": str(len(body))}
    if lm:
        hdr["Last-Modified"] = lm
    return Resp(body, hdr)

L.urllib.request.urlopen = fake_urlopen
fails = []
def ck(n, c, d=""):
    print(("PASS " if c else "FAIL ") + n + (" " + str(d) if d else ""))
    if not c: fails.append(n)

o1 = L.lambda_handler({}, None)
st = json.loads(S3MEM[L.STATE_KEY])
ck("universe 4", st["universe_n"] == 4, st["universe_n"])
ck("3 banked fresh", st["n_files"] == 3 and all(
    v["status"] == "fresh" for v in st["have"].values()), st["n_files"])
ck("headless fallback banked", st["have"][NOHEAD]["bytes"] == 8,
   st["have"].get(NOHEAD))
ck("gone 1-strike logged", st["failures"][GONE]["tries"] == 1,
   st["failures"].get(GONE))
ck("verbatim", S3MEM[L.ROOT + "src/ar539.csv"]
   == FILES["ar539.csv"][1], None)

o2 = L.lambda_handler({}, None)
st2 = json.loads(S3MEM[L.STATE_KEY])
ck("rerun unchanged", st2["have"]["ar539.csv"]["status"]
   == "unchanged", st2["have"]["ar539.csv"]["status"])
ck("gone 2-strike", st2["failures"][GONE]["tries"] == 2, None)

FILES["ac207.csv"] = ("Wed, 03 Jan 2024 00:00:00 GMT",
                      b"a,b\n1,2\n3,4\n")
o3 = L.lambda_handler({}, None)
st3 = json.loads(S3MEM[L.STATE_KEY])
ck("changed refetched", st3["have"]["ac207.csv"]["status"] == "fresh"
   and st3["have"]["ac207.csv"]["bytes"] == 12,
   st3["have"]["ac207.csv"])
ck("gone 3-strike named", st3["failures"][GONE]["tries"] >= 3, None)

harvest_ok["v"] = False
o4 = L.lambda_handler({}, None)
st4 = json.loads(S3MEM[L.STATE_KEY])
ck("harvest-fail fallback", "_harvest" in st4["failures"] and
   st4["n_files"] == 3, st4["failures"].get("_harvest"))
man = json.loads(S3MEM[L.MANIFEST_KEY])
ck("manifest", man["files"] == 3 and man["failures"] >= 2, man)
print("HARNESS " + ("GREEN" if not fails else "RED: " + ",".join(fails)))
sys.exit(1 if fails else 0)
