# ops 1532 — ECB hub history backfill: 21 new chartable series into data/ecb-hist/ + manifest merge
import json, ssl, time, urllib.request, re
import boto3
from botocore.config import Config

cfg = Config(read_timeout=120, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
ECB = "https://data-api.ecb.europa.eu/service/data/"
HEADERS = {"User-Agent": "Mozilla/5.0 (JustHodl research)", "Accept-Encoding": "gzip"}
_ctx = ssl.create_default_context()
out = {"ops": 1532, "written": [], "failed": [], "probes": {}}


def ecb_csv(key, start=None, last_n=None):
    q = "?format=csvdata"
    if start: q += f"&startPeriod={start}"
    if last_n: q += f"&lastNObservations={last_n}"
    try:
        raw = urllib.request.urlopen(urllib.request.Request(ECB + key + q, headers=HEADERS), timeout=60, context=_ctx).read()
        if raw[:2] == b"\x1f\x8b":
            import gzip; raw = gzip.decompress(raw)
        lines = raw.decode("utf-8", "replace").strip().split("\n")
        hdr = lines[0].split(","); ti = hdr.index("TIME_PERIOD"); vi = hdr.index("OBS_VALUE")
        pts = []
        for ln in lines[1:]:
            c = ln.split(",")
            if len(c) <= max(ti, vi): continue
            d, v = c[ti].strip(), c[vi].strip()
            if d and v:
                try: pts.append([d, float(v)])
                except ValueError: pass
        pts.sort()
        return pts
    except Exception as e:
        out["failed"].append(f"{key}: {str(e)[:70]}")
        return []


def norm_date(d):
    """Normalize SDMX periods to ISO date for the chart lib."""
    if re.match(r"^\d{4}-\d{2}-\d{2}$", d): return d
    m = re.match(r"^(\d{4})-(\d{2})$", d)
    if m: return f"{d}-01"
    m = re.match(r"^(\d{4})-?Q([1-4])$", d)
    if m: return f"{m.group(1)}-{['01','04','07','10'][int(m.group(2))-1]}-01"
    return d


def write_hist(sid, label, freq, pts, unit=""):
    if not pts:
        out["failed"].append(f"{sid}: no points"); return None
    pts = [[norm_date(d), round(v, 4)] for d, v in pts]
    doc = {"id": sid, "label": label, "freq": freq, "unit": unit, "source": "ECB SDMX",
           "first_date": pts[0][0], "latest_date": pts[-1][0], "n_points": len(pts), "points": pts}
    s3.put_object(Bucket=B, Key=f"data/ecb-hist/{sid}.json", Body=json.dumps(doc).encode(),
                  ContentType="application/json", CacheControl="public, max-age=21600")
    out["written"].append({"id": sid, "n": len(pts), "range": f"{pts[0][0]}→{pts[-1][0]}"})
    return {"id": sid, "label": label, "freq": freq, "first_date": pts[0][0], "latest_date": pts[-1][0], "n_points": len(pts)}


entries = []
# ── HICP family (1997+) ──
for code, sid, lbl in [("000000", "hicp_headline", "HICP — Headline YoY%"), ("XEF000", "hicp_core", "HICP — Core (ex energy/food) YoY%"),
                       ("SERV00", "hicp_services", "HICP — Services YoY%"), ("NRGY00", "hicp_energy", "HICP — Energy YoY%")]:
    e = write_hist(sid, lbl, "monthly", ecb_csv(f"ICP/M.U2.N.{code}.4.ANR", start="1997-01"), "%")
    if e: entries.append(e)

# ── rates / curve ──
e = write_hist("estr", "€STR — Euro Short-Term Rate", "daily", ecb_csv("EST/B.EU000A2X2A25.WT", start="2019-10"), "%")
if e: entries.append(e)
e = write_hist("euribor_3m", "Euribor 3M (monthly avg)", "monthly", ecb_csv("FM/M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA", start="1994-01"), "%")
if e: entries.append(e)
# Euribor−OIS spread (2019-11+): monthly euribor − monthly mean of 3M compounded €STR
eur = {d[:7]: v for d, v in ecb_csv("FM/M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA", start="2019-11")}
comp = ecb_csv("EST/B.EU000A2QQF32.CR", start="2019-11")
mm = {}
for d, v in comp: mm.setdefault(d[:7], []).append(v)
spr = [[f"{m}-01", round((eur[m] - sum(vs)/len(vs)) * 100, 1)] for m, vs in sorted(mm.items()) if m in eur]
e = write_hist("euribor_ois_bp", "Euribor−OIS 3M Spread (bp)", "monthly", spr, "bp")
if e: entries.append(e)
# YC 1y + 1y1y daily since 2004
y1 = ecb_csv("YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_1Y", start="2004-09")
y2 = ecb_csv("YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y", start="2004-09")
e = write_hist("yc_1y", "EA AAA 1Y Spot Yield", "daily", y1, "%")
if e: entries.append(e)
y2m = dict(y2)
f11 = [[d, round(((1 + r2/100)**2 / (1 + v/100) - 1) * 100, 3)] for d, v in y1 if (r2 := y2m.get(d)) is not None]
e = write_hist("yc_1y1y", "EA AAA 1y1y Forward Rate", "daily", f11, "%")
if e: entries.append(e)

# ── wages / credit / SPF / FX / T2 ──
e = write_hist("wages_negotiated", "Negotiated Wages YoY% (official)", "quarterly", ecb_csv("STS/Q.U2.N.INWR.000000.3.ANR", start="1992"), "%")
if e: entries.append(e)
e = write_hist("nfc_loans_yoy", "NFC Loans YoY% (BSI)", "monthly", ecb_csv("BSI/M.U2.Y.U.A20T.A.I.U2.2240.Z01.A", start="2003-01"), "%")
if e: entries.append(e)
e = write_hist("hh_loans_yoy", "Household Loans YoY% (BSI)", "monthly", ecb_csv("BSI/M.U2.Y.U.A20T.A.I.U2.2250.Z01.A", start="2003-01"), "%")
if e: entries.append(e)
e = write_hist("spf_longterm", "SPF Long-Term Inflation Expectations", "quarterly", ecb_csv("SPF/Q.U2.HICP.POINT.LT.Q.AVG", start="1999"), "%")
if e: entries.append(e)
e = write_hist("eurusd", "EUR/USD Spot", "daily", ecb_csv("EXR/D.USD.EUR.SP00.A", start="1999-01"), "")
if e: entries.append(e)
de = ecb_csv("TGB/M.DE.N.A094T.U2.EUR.E", start="2008-01")
it = dict(ecb_csv("TGB/M.IT.N.A094T.U2.EUR.E", start="2008-01"))
t2 = [[d, round((v - it[d]) / 1000, 1)] for d, v in de if d in it]
e = write_hist("t2_de_minus_it", "TARGET2 DE−IT Gap (€bn)", "monthly", t2, "€bn")
if e: entries.append(e)

# ── M3 (probe + write) ──
m3 = ecb_csv("BSI/M.U2.Y.V.M30.X.I.U2.2300.Z01.A", start="1981-01")
out["probes"]["m3_key"] = "BSI/M.U2.Y.V.M30.X.I.U2.2300.Z01.A" if m3 else "FAILED"
e = write_hist("m3_yoy", "M3 Money Supply YoY%", "monthly", m3, "%")
if e: entries.append(e)

# ── Fragmentation: IRS long-term convergence yields, monthly ──
irs = {}
for cc in ("DE", "IT", "FR", "ES", "PT", "GR"):
    irs[cc] = ecb_csv(f"IRS/M.{cc}.L.L40.CI.0000.EUR.N.Z", start="1995-01")
out["probes"]["irs_counts"] = {cc: len(v) for cc, v in irs.items()}
dem = dict(irs["DE"])
for cc, sid in [("IT", "it_de_10y_bp"), ("FR", "fr_de_10y_bp"), ("ES", "es_de_10y_bp"), ("PT", "pt_de_10y_bp"), ("GR", "gr_de_10y_bp")]:
    sp = [[d, round((v - dem[d]) * 100, 0)] for d, v in irs[cc] if d in dem]
    e = write_hist(sid, f"{cc}−DE 10Y Spread (bp, monthly)", "monthly", sp, "bp")
    if e: entries.append(e)

# ── APP/PEPP probe: grep dataflow catalog for asset-purchase flows ──
try:
    raw = urllib.request.urlopen(urllib.request.Request(
        "https://data-api.ecb.europa.eu/service/dataflow/ECB?format=structurespecificdata", headers=HEADERS),
        timeout=60, context=_ctx).read()
    if raw[:2] == b"\x1f\x8b":
        import gzip; raw = gzip.decompress(raw)
    txt = raw.decode("utf-8", "replace")
    hits = re.findall(r'id="([A-Z0-9]+)"[^>]*>\s*<[^>]*Name[^>]*>([^<]*(?:urchase|APP|PEPP)[^<]*)<', txt)
    out["probes"]["app_flows"] = hits[:10]
except Exception as e:
    out["probes"]["app_flows"] = str(e)[:80]

# ── manifest merge ──
try:
    man = json.loads(s3.get_object(Bucket=B, Key="data/ecb-hist/_manifest.json")["Body"].read())
except Exception:
    man = {"series": []}
out["manifest_before"] = len(man.get("series", []))
have = {s["id"] for s in man["series"]}
sample = man["series"][0] if man["series"] else None
out["manifest_sample_keys"] = sorted(sample.keys()) if sample else None
for e in entries:
    if e["id"] not in have:
        man["series"].append(e)
man["series"].sort(key=lambda s: s["id"])
man["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
s3.put_object(Bucket=B, Key="data/ecb-hist/_manifest.json", Body=json.dumps(man).encode(),
              ContentType="application/json", CacheControl="public, max-age=3600")
out["manifest_after"] = len(man["series"])

open("aws/ops/reports/1532_backfill.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"written": len(out["written"]), "failed": out["failed"][:4],
                  "manifest": f"{out['manifest_before']}→{out['manifest_after']}",
                  "irs": out["probes"].get("irs_counts")}, default=str))
