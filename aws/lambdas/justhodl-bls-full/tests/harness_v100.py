"""Harness for justhodl-bls-full v1.0.0 -- boto3+urllib stubbed.
Proves: discovery, per-survey listing, streaming mirror w/ metadata,
HEAD-conditional skip (unchanged), changed-file refetch on relist,
named HEAD-404 failure, state/manifest contract. Exit 0 = OK."""
import io, json, sys, types, re

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
    def upload_fileobj(s, fobj, Bucket, Key, ExtraArgs=None):
        S3MEM[Key] = fobj.read()
        S3META[Key] = (ExtraArgs or {}).get("Metadata", {})
boto3 = types.ModuleType("boto3"); boto3.client = lambda *a, **k: _S3()
sys.modules["boto3"] = boto3

sys.path.insert(0, "aws/lambdas/justhodl-bls-full/source")
import lambda_function as L

FILES = {
 "ap/ap.series":   ("Mon, 01 Jan 2024 00:00:00 GMT", b"sid\tname\n" * 40),
 "ap/ap.data.0.Current": ("Tue, 02 Jan 2024 00:00:00 GMT",
                          b"sid\t1913\tM01\t9.9\n" * 200),
 "ap/ap.footnote": ("Mon, 01 Jan 2024 00:00:00 GMT", b"f\tnote\n"),
 "jt/jt.series":   ("Wed, 03 Jan 2024 00:00:00 GMT", b"sid\tx\n" * 10),
 "jt/jt.data.1.AllItems": ("Wed, 03 Jan 2024 00:00:00 GMT",
                           b"sid\t2000\tM12\t5.5\n" * 300),
}
GONE = "jt/jt.contacts"          # HEAD will 404 -> named failure

class Resp:
    def __init__(s, body=b"", headers=None, st=200):
        s._b = body; s.status = st
        s.headers = {k: v for k, v in (headers or {}).items()}
    def read(s, amt=None):
        b, s._b = (s._b, b"") if amt is None else (s._b[:amt], s._b[amt:])
        return b
    def __enter__(s): return s
    def __exit__(s, *a): return False
    class H(dict):
        def get(s2, k, d=None): return dict.get(s2, k, d)

def fake_urlopen(req, timeout=None):
    import urllib.error
    url = req.full_url
    meth = req.get_method()
    if url == L.BASE:
        return Resp(b'<A HREF="/pub/time.series/AP/">AP</A> '
                    b'<A HREF="jt/">jt</A> <A HREF="pub/">x</A>')
    m = re.match(re.escape(L.BASE) + r"([a-z]{2,3})/$", url)
    if m:
        sv = m.group(1)
        links = "".join('<A HREF="%s">x</A>' % f.split("/")[1]
                        for f in list(FILES) + [GONE]
                        if f.startswith(sv + "/"))
        return Resp(links.encode())
    rel = url[len(L.BASE):]
    if rel == GONE:
        raise urllib.error.HTTPError(url, 404, "nf", {}, io.BytesIO(b""))
    if rel in FILES:
        lm, body = FILES[rel]
        h = {"Last-Modified": lm, "Content-Length": str(len(body))}
        r = Resp(body if meth == "GET" else b"", headers=h)
        r.headers = types.SimpleNamespace(get=lambda k, d=None: h.get(k, d))
        return r
    raise AssertionError("unexpected URL " + url)

L.urllib.request.urlopen = fake_urlopen
L.time.sleep = lambda *_: None
L.SPACING = 0

fails = []
def ck(n, c, d=""):
    print(("PASS " if c else "FAIL ") + n + (" " + str(d) if d else ""))
    if not c: fails.append(n)

out1 = L.lambda_handler({}, None)
st = json.loads(S3MEM[L.STATE_KEY])
ck("phase COMPLETE", st["phase"] == "COMPLETE", st["phase"])
ck("surveys discovered", st["n_surveys"] == 2 and
   set(st["surveys"]) == {"ap", "jt"}, st.get("n_surveys"))
ck("files banked", st["n_files"] == 5 and out1["files"] == 5,
   st.get("n_files"))
ck("all fresh", all(v["status"] == "fresh"
                    for v in st["have"].values()), None)
ck("bytes accounted", st["bytes_total"] ==
   sum(len(b) for _, b in FILES.values()), st["bytes_total"])
ck("404 named", "HEAD HTTP 404" in str(st["failures"].get(GONE)),
   st["failures"].get(GONE))
ck("verbatim banked", S3MEM[L.ROOT + "src/ap/ap.data.0.Current"]
   == FILES["ap/ap.data.0.Current"][1], None)
ck("metadata stamped", S3META[L.ROOT + "src/jt/jt.series"]
   .get("src_lm") == FILES["jt/jt.series"][0], None)
man = json.loads(S3MEM[L.MANIFEST_KEY])
ck("manifest", man["files"] == 5 and man["phase"] == "COMPLETE"
   and man["gb"] == round(st["bytes_total"] / 1e9, 2), man["files"])
ck("no chain when drained", out1["chained"] is False, out1["chained"])

# run 2: relist; one file changed upstream -> only it refetches ------
FILES["ap/ap.data.0.Current"] = ("Thu, 04 Jan 2024 00:00:00 GMT",
                                 b"sid\t1913\tM01\t9.9\n" * 250)
out2 = L.lambda_handler({"relist": True}, None)
st2 = json.loads(S3MEM[L.STATE_KEY])
hv = st2["have"]
ck("changed refetched", hv["ap/ap.data.0.Current"]["status"] == "fresh"
   and hv["ap/ap.data.0.Current"]["lm"].startswith("Thu"),
   hv["ap/ap.data.0.Current"])
ck("unchanged skipped", hv["jt/jt.data.1.AllItems"]["status"]
   == "unchanged", hv["jt/jt.data.1.AllItems"]["status"])
ck("phase back to COMPLETE", st2["phase"] == "COMPLETE"
   and out2["queue_left"] == 0, st2["phase"])
ck("bytes updated", st2["bytes_total"] ==
   sum(len(b) for _, b in FILES.values()), st2["bytes_total"])

need = ["phase", "surveys", "queue", "have", "failures", "n_files",
        "bytes_total", "as_of", "version"]
ck("state contract", all(k in st2 for k in need),
   [k for k in need if k not in st2])
print("HARNESS " + ("GREEN" if not fails else "RED: " + ",".join(fails)))
sys.exit(1 if fails else 0)
