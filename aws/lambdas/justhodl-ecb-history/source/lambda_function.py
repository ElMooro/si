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
        # Smart rounding: small-range indices (CISS 0-1) need more decimals than
        # large ones (balance sheet €bn). Round to keep ~4 significant figures.
        def _r(v):
            if v is None: return None
            a = abs(v)
            if a == 0: return 0.0
            if a < 1: return round(v, 5)
            if a < 100: return round(v, 3)
            return round(v, 1)
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
               "latest": _r(latest), "min": _r(min(vals)), "max": _r(max(vals)),
               "percentile": pctl, "z_score": z, "points": pts}
        s3.put_object(Bucket=BUCKET, Key=f"data/ecb-hist/{sid}.json",
                      Body=json.dumps(out, default=str).encode(),
                      ContentType="application/json", CacheControl="public, max-age=43200")
        written.append(sid)
        # flag series ECB has stopped updating (e.g. CISS sub-contributions ended ~2025-05)
        _stale_days = None
        try:
            from datetime import date as _d
            _stale_days = (datetime.now().date() - _d.fromisoformat(pts[-1][0])).days
        except Exception:
            pass
        # staleness vs frequency-appropriate SLA
        try:
            ld = pts[-1][0]
            ld10 = (ld + "-01-01")[:10] if len(ld) == 4 else (ld + "-01")[:10] if len(ld) == 7 else ld[:10]
            stale_days = (datetime.now(timezone.utc).date()
                           - datetime.strptime(ld10[:10], "%Y-%m-%d").date()).days
        except Exception:
            stale_days = None
        sla = {"daily": 7, "weekly": 14, "monthly": 45, "quarterly": 120, "annual": 430}.get(freq, 60)
        discontinued = bool(stale_days is not None and stale_days > sla * 3)
        manifest.append({"id": sid, "label": label, "freq": freq,
                         "latest": _r(latest), "percentile": pctl, "z_score": z,
                         "stale_days": stale_days, "discontinued": discontinued,
                         "first_date": pts[0][0], "latest_date": pts[-1][0], "n_points": len(pts),
                         "discontinued": bool(_stale_days and _stale_days > 120), "stale_days": _stale_days})
        time.sleep(0.4)
    s3.put_object(Bucket=BUCKET, Key="data/ecb-hist/_manifest.json",
                  Body=json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(),
                                   "series": manifest, "n": len(manifest)}, default=str).encode(),
                  ContentType="application/json")
    print(f"[ecb-hist] wrote {len(written)} series in {round(time.time()-t0,1)}s: {written}")
    return {"statusCode": 200, "body": json.dumps({"written": len(written)})}
