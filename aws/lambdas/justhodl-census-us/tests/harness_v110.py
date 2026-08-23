"""Local harness for census-us v1.1.0 -- boto3 + urllib stubbed.
Proves: universe widening + exclusions, family slugs, EITS slug
stability, YEAR grammar (BDS-lag safe), geo-variant chain to state:*,
header-based year fix, recatalog event, state key contract. Exit 0 = OK.
"""
import io, json, sys, types, gzip

# ── boto3 stub (must land before engine import) ───────────────────────
S3MEM = {}
class _Body:
    def __init__(s, b): s.b = b
    def read(s): return s.b
class _S3:
    def get_object(s, Bucket, Key):
        if Key not in S3MEM: raise KeyError(Key)
        return {"Body": _Body(S3MEM[Key])}
    def put_object(s, Body, Bucket, Key, **kw): S3MEM[Key] = Body
boto3 = types.ModuleType("boto3"); boto3.client = lambda *a, **k: _S3()
sys.modules["boto3"] = boto3

sys.path.insert(0, "aws/lambdas/justhodl-census-us/source")
import lambda_function as L

# ── HTTP fixtures ─────────────────────────────────────────────────────
CUR = 2026
DATA_JSON = {"dataset": [
  {"c_dataset": ["timeseries","eits","marts"], "c_isTimeseries": True,
   "title": "Adv Monthly Retail", "modified": "2026-08",
   "distribution": [{"accessURL": "http://api.census.gov/data/timeseries/eits/marts"}]},
  {"c_dataset": ["timeseries","bds"], "c_isTimeseries": True,
   "title": "Business Dynamics", "modified": "2026-05",
   "distribution": [{"accessURL": "https://api.census.gov/data/timeseries/bds"}]},
  {"c_dataset": ["timeseries","qwi","sa"], "c_isTimeseries": True,
   "title": "QWI Sex by Age", "modified": "2026-07",
   "distribution": [{"accessURL": "https://api.census.gov/data/timeseries/qwi/sa"}]},
  {"c_dataset": ["timeseries","intltrade","imports","hs"],
   "c_isTimeseries": True, "title": "Imports HS",
   "distribution": [{"accessURL": "https://api.census.gov/data/timeseries/intltrade/imports/hs"}]},
  {"c_dataset": ["timeseries","idb","1year"], "c_isTimeseries": True,
   "title": "IDB", "distribution": [{"accessURL": "https://api.census.gov/data/timeseries/idb/1year"}]},
  {"c_dataset": ["2023","acs","acs1"], "c_isTimeseries": False,
   "title": "ACS ignored", "distribution": [{"accessURL": "https://x"}]},
]}
V_MARTS = {"variables": {"cell_value": {}, "data_type_code": {},
  "category_code": {}, "seasonally_adj": {}, "time": {}, "for": {}, "us": {}}}
V_BDS = {"variables": {"YEAR": {}, "ESTAB": {}, "FIRM": {},
  "JOB_CREATION": {}, "for": {}, "us": {}}}
V_QWI = {"variables": {"time": {}, "Emp": {}, "HirA": {}, "Sep": {},
  "EarnS": {}, "for": {}, "state": {}}}

def rows_marts():
    h = [["cell_value","data_type_code","category_code","seasonally_adj","time","us"]]
    return h + [["%d" % (100+i), "SM","44X00","yes","%d-%02d" % (yr,m),"1"]
                for i,(yr,m) in enumerate((y,mm) for y in range(2019,2022)
                                          for mm in (1,6))]
def rows_bds(y):
    return [["ESTAB","FIRM","JOB_CREATION","YEAR"],["500","300","70",str(y)]]
def rows_qwi():
    h = [["Emp","HirA","Sep","EarnS","time","state"]]
    return h + [["10","1","1","900","%d-Q1" % y, "48"] for y in range(1990,2026)]

class Resp:
    def __init__(s, obj, st=200):
        s.status = st; s._b = json.dumps(obj).encode()
    def read(s): return s._b
    def __enter__(s): return s
    def __exit__(s, *a): return False

def fake_urlopen(req, timeout=None):
    url = req.full_url if hasattr(req, "full_url") else str(req)
    R = Resp
    if url.startswith("https://api.census.gov/data.json"): return R(DATA_JSON)
    if "eits/marts/variables.json" in url: return R(V_MARTS)
    if "timeseries/bds/variables.json" in url: return R(V_BDS)
    if "qwi/sa/variables.json" in url: return R(V_QWI)
    if "eits/marts?" in url:
        if "for=us%3A%2A" in url and "time=from+1900" in url:
            return R(rows_marts())
        return R([])                       # base variant: nothing matched
    if "timeseries/bds?" in url:
        import re
        m = re.search(r"YEAR=(\d{4})", url)
        if m and 1978 <= int(m.group(1)) <= 2022 and "for=" not in url:
            return R(rows_bds(int(m.group(1))))
        return R([])
    if "qwi/sa?" in url:
        if "time=from+1900" in url:
            if "for=state%3A%2A" in url: return R(rows_qwi())
            if "for=us%3A%2A" in url:
                import urllib.error
                raise urllib.error.HTTPError(url, 400, "bad", {}, io.BytesIO(b"unknown geo"))
            return R([])
        return R([])
    raise AssertionError("unexpected URL: " + url)

L.urllib.request.urlopen = fake_urlopen
L.SPACING = 0.0; L.time.sleep = lambda *_: None

class Ctx:
    def __init__(s, ms=10_000_000): s.ms = ms
    def get_remaining_time_in_millis(s): return s.ms

fails = []
def ck(name, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + name + (" " + str(detail) if detail else ""))
    if not cond: fails.append(name)

# seed a v1.0-style COMPLETE state (marts done, no tp key) -------------
seed = {"version":"1.0.0","phase":"COMPLETE","queue":[],
 "datasets":{"marts":{"mode":"full_for","rows":18,"y0":"1992","y1":"2026",
   "calls":3,"ok":True,"resume_year":None,"seen_data":True,"empty_run":0,
   "vars":["cell_value","data_type_code","category_code","seasonally_adj"]}},
 "catalog":{"marts":{"slug":"marts","url":"https://api.census.gov/data/timeseries/eits/marts","title":"m"}},
 "failures":{}, "n_total":1,"n_done":1,"rows_total":18,
 "n_timeseries_universe":5,"last_catalog_check":"2026-08-23T14:00:00",
 "last_refresh_date":"2026-08-23"}
S3MEM[L.STATE_KEY] = json.dumps(seed).encode()

out = L.lambda_handler({"recatalog": True}, Ctx())
st = json.loads(S3MEM[L.STATE_KEY])

ck("universe counted", st["n_timeseries_universe"] == 5, st["n_timeseries_universe"])
ck("exclusions named", st.get("excluded_families") == {"intltrade":1,"idb":1}, st.get("excluded_families"))
ck("n_total widened", st["n_total"] == 3, st["n_total"])
ck("slugs+families", set(st["catalog"]) == {"marts","bds","qwi-sa"}
   and st["catalog"]["bds"]["family"] == "bds", sorted(st["catalog"]))
ck("families key", st.get("families") == ["bds","eits","qwi"], st.get("families"))
ck("phase drained to COMPLETE", st["phase"] == "COMPLETE", st["phase"])

m = st["datasets"]["marts"]
ck("EITS untouched", m["rows"] == 18 and m["mode"] == "full_for", m)

b = st["datasets"]["bds"]
ck("bds YEAR grammar", b.get("tp") == "YEAR" and b["mode"] == "year", (b.get("tp"), b["mode"]))
ck("bds inception 1978..2022", b["y0"] == "1978" and b["y1"] == "2022" and b["ok"], (b["y0"], b["y1"]))
ck("bds rows", b["rows"] == 45, b["rows"])
ck("bds lag-safe (no flips burned)", b.get("flips", 0) == 0, b.get("flips"))

qw = st["datasets"]["qwi-sa"]
ck("qwi state variant", qw["mode"] == "full_state" and qw["ok"], qw["mode"])
ck("qwi header year fix", qw["y0"] == "1990" and qw["y1"] == "2025", (qw["y0"], qw["y1"]))
ck("qwi full banked", "data/warm/census-us/qwi-sa/full.json.gz" in S3MEM, None)

need = ["phase","n_total","n_done","rows_total","queue","datasets",
        "failures","n_timeseries_universe","updated_at","families",
        "excluded_families"]
ck("state key contract", all(k in st for k in need),
   [k for k in need if k not in st])
ck("no failures", st["failures"] == {}, st["failures"])
ck("handler return shape", out["phase"] == "COMPLETE" and out["n_done"] == 3, out)

cat = json.loads(gzip.decompress(S3MEM[L.CATALOG_KEY]))
ck("catalog artifact", cat["n_total"] == 3 and cat["excluded_families"]["intltrade"] == 1, cat["n_total"])

print("HARNESS " + ("GREEN" if not fails else "RED: " + ",".join(fails)))
sys.exit(1 if fails else 0)
