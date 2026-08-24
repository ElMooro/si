"""Harness justhodl-imf-full v1.0.0. Proves: catalog XML parse
(vintage + plain), full-pull verbatim gz, lastN fallback tagged,
3-strike named failure, COMPLETE, rediscovery queues NEW vintage
only, redrain skips vintages, manifest. Exit 0 = OK."""
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
        S3MEM[Key + "::meta"] = json.dumps(
            (ExtraArgs or {}).get("Metadata", {})).encode()
boto3 = types.ModuleType("boto3"); boto3.client = lambda *a, **k: _S3()
sys.modules["boto3"] = boto3
sys.path.insert(0, "aws/lambdas/justhodl-imf-full/source")
import lambda_function as L
L.time.sleep = lambda *_: None
L.SPACING = 0

CAT = {"BOP": "Balance of Payments",
       "CPI_WCA_2026_MAY_VINTAGE": "CPI world vintage May",
       "FLAKY": "needs lastN", "DEADFLOW": "always 500"}
PAD = b"<!--" + b"x" * 240 + b"-->"
BODIES = {"BOP": PAD + b"<Series><Obs v='1'/><Obs v='2'/></Series>",
          "CPI_WCA_2026_MAY_VINTAGE": PAD + b"<Series><Obs/></Series>"}

def cat_xml():
    pad = "<!-- " + "c" * 5200 + " -->"
    return (pad + "".join(
        '<str:Dataflow id="%s" agencyID="IMF">'
        '<com:Name xml:lang="en">%s</com:Name></str:Dataflow>'
        % (k, v) for k, v in CAT.items())).encode()

class Resp:
    def __init__(s, b): s._b = b
    def read(s, amt=None):
        b, s._b = (s._b, b"") if amt is None else \
            (s._b[:amt], s._b[amt:])
        return b
    def __enter__(s): return s
    def __exit__(s, *a): return False

def fake_urlopen(req, timeout=None):
    import urllib.error
    url = req.full_url
    if "/dataflow" in url:
        if url.endswith("/dataflow/all"):
            return Resp(cat_xml())          # ladder cand #1 answers
        raise urllib.error.HTTPError(url, 204, "empty", {},
                                     io.BytesIO(b""))
    fid = url.split("/data/")[1].split("?")[0]
    if fid == "DEADFLOW":
        raise urllib.error.HTTPError(url, 500, "x", {},
                                     io.BytesIO(b""))
    if fid == "FLAKY":
        if "lastNObservations" not in url:
            raise urllib.error.HTTPError(url, 413, "big", {},
                                         io.BytesIO(b""))
        return Resp(PAD + b"<Series>" + b"<Obs/>" * 40 + b"</Series>")
    return Resp(BODIES[fid])

L.urllib.request.urlopen = fake_urlopen
fails = []
def ck(n, c, d=""):
    print(("PASS " if c else "FAIL ") + n + (" " + str(d) if d else ""))
    if not c: fails.append(n)

o1 = L.lambda_handler({}, None)
o2 = L.lambda_handler({}, None)   # chain link 2 clears retries
o3 = L.lambda_handler({}, None)
st = json.loads(S3MEM[L.STATE_KEY])
ck("catalog 4 flows", len(st["universe"]) == 4,
   list(st["universe"]))
ck("phase COMPLETE", st["phase"] == "COMPLETE", st["phase"])
ck("banked 3", st["n_banked"] == 3, st["n_banked"])
ck("dead named 3-strike", st["failures"]["DEADFLOW"]["tries"] >= 3,
   st["failures"].get("DEADFLOW"))
ck("bop verbatim gz", gzip.decompress(
    S3MEM[L.ROOT + "src/BOP.xml.gz"]).endswith(
    b"<Obs v='2'/></Series>"), None)
ck("flaky lastN tagged", st["have"]["FLAKY"]["mode"] == "lastN" and
   st["have"]["FLAKY"]["obs_hint"] == 40, st["have"]["FLAKY"])
ck("vintage flagged", st["have"]["CPI_WCA_2026_MAY_VINTAGE"
                         ]["vintage"] is True, None)
man = json.loads(S3MEM[L.MANIFEST_KEY])
ck("manifest", man["flows_banked"] == 3 and
   man["vintages_banked"] == 1 and man["lastN_partial"] == 1 and
   man["flows_catalog"] == 4, man)

# rediscovery: NEW vintage appears -> queued + drained ------------
CAT["CPI_WCA_2026_AUG_VINTAGE"] = "CPI world vintage Aug"
BODIES["CPI_WCA_2026_AUG_VINTAGE"] = PAD + b"<Series><Obs/><Obs/></Series>"
o4 = L.lambda_handler({"rediscover": True}, None)
st4 = json.loads(S3MEM[L.STATE_KEY])
ck("new vintage banked", "CPI_WCA_2026_AUG_VINTAGE" in st4["have"]
   and st4["phase"] == "COMPLETE", st4["n_banked"])
ck("old vintage retained", "CPI_WCA_2026_MAY_VINTAGE" in
   st4["have"], None)

# redrain skips vintages ------------------------------------------
BODIES["BOP"] = PAD + b"<Series><Obs v='1'/><Obs v='2'/><Obs v='3'/></Series>"
may_at = st4["have"]["CPI_WCA_2026_MAY_VINTAGE"]["at"]
o5 = L.lambda_handler({"redrain": True}, None)
st5 = json.loads(S3MEM[L.STATE_KEY])
ck("redrain refreshed BOP", st5["have"]["BOP"]["obs_hint"] == 3,
   st5["have"]["BOP"]["obs_hint"])
ck("redrain skipped vintages",
   st5["have"]["CPI_WCA_2026_MAY_VINTAGE"]["at"] == may_at, None)
need = ["phase", "queue", "have", "failures", "universe",
        "n_banked", "as_of", "last_discover"]
ck("state contract", all(k in st5 for k in need),
   [k for k in need if k not in st5])
print("HARNESS " + ("GREEN" if not fails else "RED: " + ",".join(fails)))
sys.exit(1 if fails else 0)
