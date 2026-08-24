"""Harness justhodl-worldbank-full v1.0.0 -- boto3+urllib stubbed.
Proves: paginated catalog discovery, zip streaming w/ head-chaining,
no-data HTML detection, 404 3-strike named failure, redrain refresh,
state/manifest contract. Exit 0 = OK."""
import io, json, sys, types, gzip, zipfile

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
        return {"ContentLength": len(S3MEM[Key])}
    def upload_fileobj(s, fobj, Bucket, Key, ExtraArgs=None):
        out = b""
        while True:
            c = fobj.read(65536)
            if not c: break
            out += c
        S3MEM[Key] = out
        S3META[Key] = (ExtraArgs or {}).get("Metadata", {})
boto3 = types.ModuleType("boto3"); boto3.client = lambda *a, **k: _S3()
sys.modules["boto3"] = boto3

sys.path.insert(0, "aws/lambdas/justhodl-worldbank-full/source")
import lambda_function as L

def zbytes(name, rows):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(name, "Country Name,1960,2024\n" + rows)
    return buf.getvalue()

IDS = ["NY.GDP.MKTP.CD", "SP.POP.TOTL", "ZZ.NODATA.X", "ZZ.GONE.404"]
ZIPS = {"NY.GDP.MKTP.CD": zbytes("gdp.csv", "USA,543,27000\n" * 50),
        "SP.POP.TOTL": zbytes("pop.csv", "USA,180,340\n" * 30)}

class Resp:
    def __init__(s, body=b"", ct="application/zip", st=200):
        s._b = body; s.status = st
        s.headers = types.SimpleNamespace(
            get=lambda k, d=None: {"Content-Type": ct}.get(k, d))
    def read(s, amt=None):
        b, s._b = (s._b, b"") if amt is None else (s._b[:amt], s._b[amt:])
        return b
    def __enter__(s): return s
    def __exit__(s, *a): return False

def fake_urlopen(req, timeout=None):
    import urllib.error, re
    url = req.full_url
    if "/v2/indicator?" in url:
        page = int(re.search(r"[?&]page=(\d+)", url).group(1))
        rows = [{"id": i, "name": "n-" + i} for i in
                (IDS[:2] if page == 1 else IDS[2:])]
        return Resp(json.dumps([
            {"pages": 2, "page": page}, rows]).encode(),
            ct="application/json")
    m = re.search(r"/v2/en/indicator/([^?]+)\?downloadformat=csv", url)
    if m:
        iid = urllib_unq(m.group(1))
        if iid == "ZZ.GONE.404":
            raise urllib.error.HTTPError(url, 404, "nf", {},
                                         io.BytesIO(b""))
        if iid == "ZZ.NODATA.X":
            return Resp(b"<!DOCTYPE html><html>No data</html>",
                        ct="text/html")
        return Resp(ZIPS[iid])
    raise AssertionError("unexpected URL " + url)

import urllib.parse as _up
urllib_unq = _up.unquote
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
ck("catalog n", st["n_indicators"] == 4, st.get("n_indicators"))
ck("banked 2 fresh", st["n_banked"] == 2 and out1["banked"] == 2,
   st.get("n_banked"))
ck("no_data named", st["have"]["ZZ.NODATA.X"]["status"] == "no_data",
   st["have"].get("ZZ.NODATA.X"))
ck("404 3-strike named",
   "HTTP 404" in str(st["failures"].get("ZZ.GONE.404", {}).get("err"))
   and st["failures"]["ZZ.GONE.404"]["tries"] == 3,
   st["failures"].get("ZZ.GONE.404"))
ck("zip verbatim", S3MEM[L.ROOT + "src/NY.GDP.MKTP.CD.zip"]
   == ZIPS["NY.GDP.MKTP.CD"], None)
ck("zip readable", b"Country Name" in zipfile.ZipFile(io.BytesIO(
   S3MEM[L.ROOT + "src/SP.POP.TOTL.zip"])).read("pop.csv"), None)
ck("bytes", st["bytes_total"] == sum(len(v) for v in ZIPS.values()),
   st["bytes_total"])
cat = json.loads(gzip.decompress(S3MEM[L.CATALOG_KEY]))
ck("catalog artifact", cat["n"] == 4, cat["n"])
man = json.loads(S3MEM[L.MANIFEST_KEY])
ck("manifest", man["banked"] == 2 and man["no_data"] == 1
   and man["failures"] == 1 and man["phase"] == "COMPLETE",
   (man["banked"], man["no_data"], man["failures"]))
ck("no chain when drained", out1["chained"] is False, out1["chained"])

ZIPS["SP.POP.TOTL"] = zbytes("pop.csv", "USA,180,341\n" * 40)
out2 = L.lambda_handler({"redrain": True}, None)
st2 = json.loads(S3MEM[L.STATE_KEY])
ck("redrain re-banked", st2["phase"] == "COMPLETE"
   and S3MEM[L.ROOT + "src/SP.POP.TOTL.zip"] == ZIPS["SP.POP.TOTL"],
   st2["phase"])
ck("redrain bytes updated", st2["bytes_total"] ==
   sum(len(v) for v in ZIPS.values()), st2["bytes_total"])

need = ["phase", "queue", "have", "failures", "n_indicators",
        "n_banked", "bytes_total", "as_of", "version"]
ck("state contract", all(k in st2 for k in need),
   [k for k in need if k not in st2])
print("HARNESS " + ("GREEN" if not fails else "RED: " + ",".join(fails)))
sys.exit(1 if fails else 0)
