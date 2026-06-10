# ops 1535 — definitive HICP-2026 hunt (wildcard ref-area probe) + fix balance_sheet W-dates
import json, ssl, urllib.request, re, datetime, boto3
from botocore.config import Config
cfg = Config(read_timeout=120, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
HEADERS = {"User-Agent": "Mozilla/5.0 (JustHodl research)", "Accept-Encoding": "gzip"}
_ctx = ssl.create_default_context()
out = {"ops": 1535}

# 1) wildcard ref-area: which area code has 2026 HICP?
try:
    u = "https://data-api.ecb.europa.eu/service/data/ICP/M..N.000000.4.ANR?format=csvdata&lastNObservations=1"
    raw = urllib.request.urlopen(urllib.request.Request(u, headers=HEADERS), timeout=90, context=_ctx).read()
    if raw[:2] == b"\x1f\x8b":
        import gzip; raw = gzip.decompress(raw)
    lines = raw.decode("utf-8", "replace").strip().split("\n")
    hdr = lines[0].split(",")
    ki = hdr.index("KEY") if "KEY" in hdr else None
    ti = hdr.index("TIME_PERIOD"); vi = hdr.index("OBS_VALUE")
    ra = hdr.index("REF_AREA") if "REF_AREA" in hdr else None
    rows = []
    for ln in lines[1:]:
        c = ln.split(",")
        if len(c) <= max(ti, vi): continue
        area = c[ra] if ra is not None else (c[ki].split(".")[1] if ki is not None else "?")
        rows.append((area, c[ti], c[vi]))
    rows.sort(key=lambda r: r[1], reverse=True)
    out["areas_latest"] = rows[:25]
    out["areas_2026"] = [r for r in rows if r[1] >= "2026"][:15]
except Exception as e:
    out["areas_latest"] = str(e)[:120]

# 2) Eurostat: probe flash dataset for EA 2026
try:
    u = ("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_midx"
         "?format=JSON&lang=EN&geo=EA&coicop=CP00&unit=I15&lastTimePeriod=4")
    j = json.loads(urllib.request.urlopen(urllib.request.Request(u, headers=HEADERS), timeout=45, context=_ctx).read())
    idx = j.get("dimension", {}).get("time", {}).get("category", {}).get("index", {})
    vals = j.get("value", {})
    out["eurostat_midx_EA"] = sorted((t, vals.get(str(i))) for t, i in idx.items())
except Exception as e:
    out["eurostat_midx_EA"] = str(e)[:120]

# 3) fix balance_sheet ISO-week dates → Monday dates
try:
    doc = json.loads(s3.get_object(Bucket=B, Key="data/ecb-hist/balance_sheet.json")["Body"].read())
    def wfix(d):
        m = re.match(r"^(\d{4})-W(\d{2})$", d)
        if not m: return d
        return datetime.date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1).isoformat()
    pts = sorted([[wfix(d), v] for d, v in doc["points"]])
    doc.update({"points": pts, "first_date": pts[0][0], "latest_date": pts[-1][0]})
    s3.put_object(Bucket=B, Key="data/ecb-hist/balance_sheet.json", Body=json.dumps(doc).encode(),
                  ContentType="application/json", CacheControl="public, max-age=21600")
    out["balance_sheet_fixed"] = {"first": pts[0][0], "last": pts[-1][0], "n": len(pts)}
    man = json.loads(s3.get_object(Bucket=B, Key="data/ecb-hist/_manifest.json")["Body"].read())
    for sx in man["series"]:
        if sx["id"] == "balance_sheet":
            sx["first_date"], sx["latest_date"] = pts[0][0], pts[-1][0]
    s3.put_object(Bucket=B, Key="data/ecb-hist/_manifest.json", Body=json.dumps(man).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
except Exception as e:
    out["balance_sheet_fixed"] = str(e)[:100]

open("aws/ops/reports/1535_hunt.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"areas_2026": out.get("areas_2026"), "midx": out.get("eurostat_midx_EA"),
                  "bs": out.get("balance_sheet_fixed")}, default=str))
