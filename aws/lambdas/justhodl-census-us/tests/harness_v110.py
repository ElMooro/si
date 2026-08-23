"""Local harness for census-us v1.1.3 -- boto3 + urllib stubbed.
Proves v1.1.0 behaviors PLUS the 4948 fixes: chunked READ_CAP -> 413
ladder signal (no OOM), save-first crash quarantine, annual-set
single-year time answer redone via YEAR (bds), and the redo event.
Exit 0 = OK.
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
  {"c_dataset": ["timeseries","healthins","sahie"], "c_isTimeseries": True,
   "title": "SAHIE", "modified": "2026-06",
   "distribution": [{"accessURL": "https://api.census.gov/data/timeseries/healthins/sahie"}]},
  {"c_dataset": ["timeseries","soma"], "c_isTimeseries": True,
   "title": "Mkt Absorption", "modified": "2026-06",
   "distribution": [{"accessURL": "https://api.census.gov/data/timeseries/soma"}]},
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
V_BDS = {"variables": {"time": {}, "YEAR": {}, "ESTAB": {}, "FIRM": {},
  "JOB_CREATION": {}, "for": {}, "us": {}}}            # BOTH grammars
V_QWI = {"variables": {"time": {}, "Emp": {}, "HirA": {}, "Sep": {},
  "EarnS": {}, "sEmp": {}, "sHirA": {}, "sSep": {}, "sEarnS": {},
  "Payroll": {}, "FrmJbGn": {}, "for": {}, "state": {}}}
V_SAHIE = {"variables": {"time": {}, "NUI_PT": {}, "NIC_PT": {},
  "PCTUI_PT": {}, "for": {}, "us": {}, "state": {}}}

def rows_marts():
    h = [["cell_value","data_type_code","category_code","seasonally_adj","time","us"]]
    return h + [["%d" % (100+i), "SM","44X00","yes","%d-%02d" % (yr,m),"1"]
                for i,(yr,m) in enumerate((y,mm) for y in range(2019,2022)
                                          for mm in (1,6))]
def rows_bds_time_single():
    return [["ESTAB","FIRM","JOB_CREATION","time","us"],
            ["500","300","70","2023","1"]]
def rows_bds_year(y):
    return [["ESTAB","FIRM","JOB_CREATION","YEAR"],["500","300","70",str(y)]]
def rows_qwi(code):
    h = [["Emp","HirA","Sep","EarnS","time","state"]]
    return h + [["10","1","1","900","%d-Q1" % y, code] for y in (1990, 2025)]
def rows_sahie():
    h = [["NUI_PT","NIC_PT","PCTUI_PT","time","us"]]
    return h + [["27000","250000","9.7", str(y), "1"] for y in range(2008,2025)]

class Resp:
    def __init__(s, obj=None, st=200, big=False):
        s.status = st; s.big = big; s.sent = 0
        s._b = b"" if obj is None else json.dumps(obj).encode()
    def read(s, amt=None):
        if s.big:                       # endless body -> must hit cap
            s.sent += (amt or 8_000_000)
            return b"x" * (amt or 8_000_000) if s.sent < 200_000_000 else b""
        if amt is None:
            b, s._b = s._b, b""
            return b
        b, s._b = s._b[:amt], s._b[amt:]
        return b
    def __enter__(s): return s
    def __exit__(s, *a): return False

def fake_urlopen(req, timeout=None):
    url = req.full_url if hasattr(req, "full_url") else str(req)
    R = Resp
    if url.startswith("https://api.census.gov/data.json"): return R(DATA_JSON)
    if "eits/marts/variables.json" in url: return R(V_MARTS)
    if "timeseries/bds/variables.json" in url: return R(V_BDS)
    if "qwi/sa/variables.json" in url: return R(V_QWI)
    if "healthins/sahie/variables.json" in url: return R(V_SAHIE)
    if "timeseries/soma/variables.json" in url:
        raise AssertionError("soma must be quarantined before any HTTP")
    if "eits/marts?" in url:
        if "for=us%3A%2A" in url and "time=from+1900" in url:
            return R(rows_marts())
        return R([])
    if "timeseries/bds?" in url:
        import re
        if "time=from+1900" in url and "for=" not in url:
            return R(rows_bds_time_single())     # the 2022-class trap
        m = re.search(r"YEAR=(\d{4})", url)
        if m and 1978 <= int(m.group(1)) <= 2023 and "for=" not in url:
            return R(rows_bds_year(int(m.group(1))))
        return R([])
    if "qwi/sa?" in url:
        import urllib.error, re as _re
        core = "get=Emp%2CHirA%2CSep%2CEarnS" in url or \
               "get=Emp,HirA,Sep,EarnS" in url
        m = _re.search(r"for=state%3A(\d\d)", url)
        if core and "time=from+1990-Q1" in url and m:
            return R(rows_qwi(m.group(1)))       # specific state only
        raise urllib.error.HTTPError(url, 400, "bad", {}, io.BytesIO(
            b"error: wildcard not supported in 'for' clause for this "
            b"hierarchy. Please select a specific state."))
    if "healthins/sahie?" in url:
        if "time=from+1900" in url:
            if "for=" not in url:
                return R(big=True)               # county mega-dump
            if "for=us%3A%2A" in url:
                return R(rows_sahie())
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

# seed: v1.0-style marts done + soma pre-poisoned (attempts=4) ---------
seed = {"version":"1.0.0","phase":"COMPLETE","queue":["soma"],
 "datasets":{"marts":{"mode":"full_for","rows":18,"y0":"1992","y1":"2026",
   "calls":3,"ok":True,"resume_year":None,"seen_data":True,"empty_run":0,
   "vars":["cell_value","data_type_code","category_code","seasonally_adj"]},
  "soma":{"mode":None,"rows":0,"y0":None,"y1":None,"calls":0,"ok":False,
   "resume_year":None,"seen_data":False,"empty_run":0,"attempts":4}},
 "catalog":{"marts":{"slug":"marts","url":"https://api.census.gov/data/timeseries/eits/marts","title":"m"}},
 "failures":{}, "n_total":1,"n_done":1,"rows_total":18,
 "n_timeseries_universe":8,"last_catalog_check":"2026-08-23T14:00:00",
 "last_refresh_date":"2026-08-23"}
S3MEM[L.STATE_KEY] = json.dumps(seed).encode()
S3MEM[L.GRAM_KEY] = json.dumps({"qwi-sa": {
    "vars": ["Emp", "HirA", "Sep", "EarnS"],
    "geo_iter": "state",
    "full_time": "from 1990-Q1"}}).encode()

out = L.lambda_handler({"recatalog": True}, Ctx())
st = json.loads(S3MEM[L.STATE_KEY])

ck("exclusions named", st.get("excluded_families") == {"intltrade":1,"idb":1}, st.get("excluded_families"))
ck("n_total widened", st["n_total"] == 5, st["n_total"])
ck("phase COMPLETE", st["phase"] == "COMPLETE", st["phase"])
m = st["datasets"]["marts"]
ck("EITS untouched", m["rows"] == 18 and m["mode"] == "full_for", (m["rows"], m["mode"]))

b = st["datasets"]["bds"]
ck("bds redone via YEAR", b.get("tp") == "YEAR" and b.get("tp_flipped")
   and b["mode"] == "year", (b.get("tp"), b["mode"]))
ck("bds inception 1978..2023", b["y0"] == "1978" and b["y1"] == "2023"
   and b["ok"], (b["y0"], b["y1"]))
ck("bds rows 46", b["rows"] == 46, b["rows"])

qw = st["datasets"]["qwi-sa"]
ck("qwi geo_state mode", qw["mode"] == "geo_state" and qw["ok"], qw["mode"])
ck("qwi 51 geo banks", len(qw.get("geo_rows") or {}) == 51
   and qw["rows"] == 51 * 2, (len(qw.get("geo_rows") or {}), qw["rows"]))
ck("qwi geo artifacts", "data/warm/census-us/qwi-sa/geo/48.json.gz" in S3MEM
   and "data/warm/census-us/qwi-sa/geo/01.json.gz" in S3MEM, None)
ck("qwi header year fix", qw["y0"] == "1990" and qw["y1"] == "2025", (qw["y0"], qw["y1"]))
ck("qwi override vars applied", qw.get("vars") == ["Emp","HirA","Sep","EarnS"], qw.get("vars"))

sa = st["datasets"]["healthins-sahie"]
ck("sahie 413 -> us variant", sa["mode"] == "full_for" and sa["ok"],
   (sa["mode"], sa.get("ok")))
ck("sahie span", sa["y0"] == "2008" and sa["y1"] == "2024", (sa["y0"], sa["y1"]))
ck("sahie no OOM artifacts", "data/warm/census-us/healthins-sahie/full.json.gz" in S3MEM, None)

ck("soma quarantined pre-HTTP",
   "quarantined" in str(st["failures"].get("soma", "")),
   st["failures"].get("soma"))
ck("failures ledger size", len(st["failures"]) == 1, st["failures"])
ck("identity", st["n_done"] + len(st["failures"]) == st["n_total"],
   (st["n_done"], len(st["failures"]), st["n_total"]))
ck("attempts cleared on done", "attempts" not in st["datasets"]["bds"]
   and "attempts" not in st["datasets"]["qwi-sa"], None)

# redo event: surgical re-import of marts ------------------------------
out2 = L.lambda_handler({"redo": ["marts"]}, Ctx())
st2 = json.loads(S3MEM[L.STATE_KEY])
m2 = st2["datasets"]["marts"]
ck("redo re-imported marts", m2["rows"] == 6 and m2["ok"]
   and st2["phase"] == "COMPLETE", (m2["rows"], st2["phase"]))

need = ["phase","n_total","n_done","rows_total","queue","datasets",
        "failures","n_timeseries_universe","updated_at","families",
        "excluded_families"]
ck("state key contract", all(k in st2 for k in need),
   [k for k in need if k not in st2])
ck("handler return shape", out["phase"] == "COMPLETE" and out["n_done"] == 4, out)
ck("version 1.1.3", st2.get("version") == "1.1.3", st2.get("version"))

print("HARNESS " + ("GREEN" if not fails else "RED: " + ",".join(fails)))
sys.exit(1 if fails else 0)
