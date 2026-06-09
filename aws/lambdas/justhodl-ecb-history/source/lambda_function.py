"""justhodl-ecb-history — full 1997→now history for the key ECB series, so tiles
become clickable charts (the audit's one real, valuable gap).

VERIFIED from AWS: ECB is NOT WAF-blocking (Mozilla UA returns 200), and
csvdata + startPeriod=1997-01-01 returns full history (CISS ~7,685 daily rows
back to ~1999, ILM ~1,432 weekly rows back to 1998). The existing ecb-detail
engine only stores today's point values (2KB, no history). This adds the history.

Per series → data/ecb-hist/<id>.json: {id, label, freq, points:[[date,value]...],
latest, min, max, percentile, z}. SCHEDULE: weekly Sat 06:00 UTC.
"""
import json, time, ssl, statistics
import urllib.request
from io import StringIO
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
BASE = "https://data-api.ecb.europa.eu/service/data/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/vnd.sdmx.data+json;version=1.0.0-wd, text/csv;q=0.9, */*;q=0.5",
    "Accept-Language": "en-US,en;q=0.9", "Accept-Encoding": "gzip, deflate",
}
s3 = boto3.client("s3", region_name=REGION)
_ctx = ssl.create_default_context(); _ctx.check_hostname = False; _ctx.verify_mode = ssl.CERT_NONE

# (flow/series_key, id, human label) — the high-signal liquidity/stress series
SERIES = [
    ("ILM/W.U2.C.A030000.U2.Z06", "ilm_usd_claims", "USD claims on EA residents (€bn) — dollar-shortage signal"),
    ("ILM/W.U2.C.L060000.U4.EUR", "ilm_eur_to_nonres", "EUR liabilities to non-residents (€bn) — foreign parking"),
    ("ILM/W.U2.C.A050000.U2.EUR", "ilm_mp_lending", "Monetary policy lending to banks (€bn)"),
    ("ILM/W.U2.C.L010000.U2.EUR", "ilm_banknotes", "Banknotes in circulation (€bn) — bank-run signal"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX", "ciss_ea", "CISS — Euro Area systemic stress composite"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_FI.CON", "ciss_fi", "CISS — financial intermediaries sub-index"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_BO.CON", "ciss_bo", "CISS — bond market sub-index"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_FX.CON", "ciss_fx", "CISS — FX market sub-index"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_EQ.CON", "ciss_eq", "CISS — equity market sub-index"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_MM.CON", "ciss_mm", "CISS — money market sub-index"),
]


def fetch_csv(flow_key):
    url = BASE + flow_key + "?format=csvdata&startPeriod=1997-01-01"
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            raw = urllib.request.urlopen(req, timeout=45, context=_ctx).read()
            # handle gzip
            if raw[:2] == b"\x1f\x8b":
                import gzip; raw = gzip.decompress(raw)
            text = raw.decode("utf-8", "replace")
            return text
        except Exception as e:
            if attempt < 2: time.sleep(1.5)
            else: print(f"[ecb-hist] {flow_key} err: {str(e)[:70]}")
    return None


def parse(text):
    # CSV: TIME_PERIOD + OBS_VALUE columns
    try:
        lines = text.strip().split("\n")
        hdr = lines[0].split(",")
        ti = hdr.index("TIME_PERIOD"); vi = hdr.index("OBS_VALUE")
        pts = []
        for ln in lines[1:]:
            cols = ln.split(",")
            if len(cols) <= max(ti, vi): continue
            d = cols[ti].strip(); v = cols[vi].strip()
            if not d or not v: continue
            # weekly "2026-W23" → approx date; daily "2026-06-05" as-is
            if "-W" in d:
                yr, wk = d.split("-W"); 
                try: dt = datetime.fromisocalendar(int(yr), int(wk), 5).date().isoformat()
                except Exception: continue
            else:
                dt = d
            try: pts.append([dt, float(v)])
            except ValueError: continue
        pts.sort()
        return pts
    except Exception as e:
        print(f"[ecb-hist] parse err: {str(e)[:60]}")
        return []


def lambda_handler(event=None, context=None):
    t0 = time.time(); written = []; manifest = []
    for flow_key, sid, label in SERIES:
        text = fetch_csv(flow_key)
        if not text: continue
        pts = parse(text)
        if len(pts) < 20: continue
        vals = [p[1] for p in pts]
        latest = vals[-1]
        below = sum(1 for v in vals if v <= latest)
        pctl = round(100 * below / len(vals), 1)
        try:
            mu = statistics.mean(vals[-260:] if len(vals) >= 260 else vals)
            sd = statistics.pstdev(vals[-260:] if len(vals) >= 260 else vals)
            z = round((latest - mu) / sd, 2) if sd else None
        except Exception: z = None
        freq = "weekly" if "-W" in text[:200] or flow_key.startswith("ILM") else "daily"
        out = {"id": sid, "label": label, "freq": freq, "flow_key": flow_key,
               "generated_at": datetime.now(timezone.utc).isoformat(),
               "n_points": len(pts), "first_date": pts[0][0], "latest_date": pts[-1][0],
               "latest": round(latest, 2), "min": round(min(vals), 2), "max": round(max(vals), 2),
               "percentile": pctl, "z_score": z, "points": pts}
        s3.put_object(Bucket=BUCKET, Key=f"data/ecb-hist/{sid}.json",
                      Body=json.dumps(out, default=str).encode(),
                      ContentType="application/json", CacheControl="public, max-age=43200")
        written.append(sid)
        manifest.append({"id": sid, "label": label, "freq": freq,
                         "latest": round(latest, 2), "percentile": pctl, "z_score": z,
                         "first_date": pts[0][0], "latest_date": pts[-1][0], "n_points": len(pts)})
        time.sleep(0.4)
    s3.put_object(Bucket=BUCKET, Key="data/ecb-hist/_manifest.json",
                  Body=json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(),
                                   "series": manifest, "n": len(manifest)}, default=str).encode(),
                  ContentType="application/json")
    print(f"[ecb-hist] wrote {len(written)} series in {round(time.time()-t0,1)}s: {written}")
    return {"statusCode": 200, "body": json.dumps({"written": len(written)})}
