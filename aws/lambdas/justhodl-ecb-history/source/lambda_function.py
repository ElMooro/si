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
    ("ILM/W.U2.C.A030000.U2.Z06", "ilm_usd_claims", "Claims on EA residents in foreign currency (€mn, weekly) — dollar-shortage"),
    ("ILM/W.U2.C.A020000.U4.Z06", "fx_claims_nonea", "Claims on non-EA residents in foreign currency (€mn, weekly) — DXY / dollar-shortage lead"),
    ("ILM/W.U2.C.L060000.U4.EUR", "ilm_eur_to_nonres", "EUR liabilities to non-residents (€bn) — foreign parking"),
    ("ILM/W.U2.C.A050000.U2.EUR", "ilm_mp_lending", "Monetary policy lending to banks (€bn)"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX", "ciss_ea", "CISS — Euro Area systemic stress composite"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_FIN.CON", "ciss_fi", "CISS — financial intermediaries sub-index"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_BMN.CON", "ciss_bo", "CISS — bond market sub-index"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_FXN.CON", "ciss_fx", "CISS — FX market sub-index"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_EMN.CON", "ciss_eq", "CISS — equity market sub-index"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_MMN.CON", "ciss_mm", "CISS — money market sub-index"),
    ("LFSI/M.I9.S.UNEHRT.TOTAL0.15_74.T", "unemployment_ea", "Unemployment rate — Euro Area (%, ages 15-74)"),
    ("STS/M.I9.Y.PROD.NS0010.4.000", "indprod_total", "Industrial production — total incl. construction (index, WDA+SA)"),
    ("STS/M.I9.Y.PROD.NS0020.4.000", "indprod_core", "Industrial production — excl. construction / core industry (index, WDA+SA)"),
    ("EXR/D.USD.EUR.SP00.A", "eurusd", "EUR/USD reference rate — dollar-strength / funding gauge"),
    # ── Unemployment (LFSI): euro area total + youth + member states ──
    ("LFSI/M.I9.S.UNEHRT.TOTAL0.15_24.T", "unemp_ea_youth", "Unemployment rate — Euro Area youth <25 (%)"),
    ("LFSI/M.DE.S.UNEHRT.TOTAL0.15_74.T", "unemp_de", "Unemployment rate — Germany (%)"),
    ("LFSI/M.FR.S.UNEHRT.TOTAL0.15_74.T", "unemp_fr", "Unemployment rate — France (%)"),
    ("LFSI/M.IT.S.UNEHRT.TOTAL0.15_74.T", "unemp_it", "Unemployment rate — Italy (%)"),
    ("LFSI/M.ES.S.UNEHRT.TOTAL0.15_74.T", "unemp_es", "Unemployment rate — Spain (%)"),
    ("LFSI/M.NL.S.UNEHRT.TOTAL0.15_74.T", "unemp_nl", "Unemployment rate — Netherlands (%)"),
    ("LFSI/M.GR.S.UNEHRT.TOTAL0.15_74.T", "unemp_gr", "Unemployment rate — Greece (%)"),
    ("LFSI/M.PT.S.UNEHRT.TOTAL0.15_74.T", "unemp_pt", "Unemployment rate — Portugal (%)"),
    ("LFSI/M.IE.S.UNEHRT.TOTAL0.15_74.T", "unemp_ie", "Unemployment rate — Ireland (%)"),
    ("LFSI/M.AT.S.UNEHRT.TOTAL0.15_74.T", "unemp_at", "Unemployment rate — Austria (%)"),
    ("LFSI/M.BE.S.UNEHRT.TOTAL0.15_74.T", "unemp_be", "Unemployment rate — Belgium (%)"),
    ("LFSI/M.FI.S.UNEHRT.TOTAL0.15_74.T", "unemp_fi", "Unemployment rate — Finland (%)"),
    ("LFSI/M.BG.S.UNEHRT.TOTAL0.15_74.T", "unemp_bg", "Unemployment rate — Bulgaria (%)"),
    ("LFSI/M.HR.S.UNEHRT.TOTAL0.15_74.T", "unemp_hr", "Unemployment rate — Croatia (%)"),
    ("LFSI/M.CY.S.UNEHRT.TOTAL0.15_74.T", "unemp_cy", "Unemployment rate — Cyprus (%)"),
    ("LFSI/M.CZ.S.UNEHRT.TOTAL0.15_74.T", "unemp_cz", "Unemployment rate — Czechia (%)"),
    ("LFSI/M.DK.S.UNEHRT.TOTAL0.15_74.T", "unemp_dk", "Unemployment rate — Denmark (%)"),
    ("LFSI/M.EE.S.UNEHRT.TOTAL0.15_74.T", "unemp_ee", "Unemployment rate — Estonia (%)"),
    ("LFSI/M.HU.S.UNEHRT.TOTAL0.15_74.T", "unemp_hu", "Unemployment rate — Hungary (%)"),
    ("LFSI/M.LV.S.UNEHRT.TOTAL0.15_74.T", "unemp_lv", "Unemployment rate — Latvia (%)"),
    ("LFSI/M.LT.S.UNEHRT.TOTAL0.15_74.T", "unemp_lt", "Unemployment rate — Lithuania (%)"),
    ("LFSI/M.LU.S.UNEHRT.TOTAL0.15_74.T", "unemp_lu", "Unemployment rate — Luxembourg (%)"),
    ("LFSI/M.MT.S.UNEHRT.TOTAL0.15_74.T", "unemp_mt", "Unemployment rate — Malta (%)"),
    ("LFSI/M.PL.S.UNEHRT.TOTAL0.15_74.T", "unemp_pl", "Unemployment rate — Poland (%)"),
    ("LFSI/M.RO.S.UNEHRT.TOTAL0.15_74.T", "unemp_ro", "Unemployment rate — Romania (%)"),
    ("LFSI/M.SK.S.UNEHRT.TOTAL0.15_74.T", "unemp_sk", "Unemployment rate — Slovakia (%)"),
    ("LFSI/M.SI.S.UNEHRT.TOTAL0.15_74.T", "unemp_si", "Unemployment rate — Slovenia (%)"),
    ("LFSI/M.SE.S.UNEHRT.TOTAL0.15_74.T", "unemp_se", "Unemployment rate — Sweden (%)"),
    # ── Industrial production by Main Industrial Grouping (STS PROD) + turnover ──
    ("STS/M.I9.Y.PROD.NS0040.4.000", "indprod_intermediate", "Industrial production — intermediate goods (index)"),
    ("STS/M.I9.Y.PROD.NS0050.4.000", "indprod_capital", "Industrial production — capital goods (index)"),
    ("STS/M.I9.Y.PROD.NS0060.4.000", "indprod_durable", "Industrial production — durable consumer goods (index)"),
    ("STS/M.I9.Y.PROD.NS0070.4.000", "indprod_nondurable", "Industrial production — non-durable consumer goods (index)"),
    ("STS/M.I9.Y.PROD.NS0080.4.000", "indprod_energy", "Industrial production — energy (index)"),
    ("STS/M.I9.Y.TOVT.NS0020.4.000", "manuf_turnover", "Industry turnover — manufacturing/total ex-construction (index)"),
    ("STS/M.I9.Y.TOVT.NS0040.4.000", "retail_turnover", "Retail trade turnover (index) — consumer demand"),
    # ── Money, growth, credit cost, FX crosses ──
    ("BSI/M.U2.Y.V.M10.X.I.U2.2300.Z01.A", "m1_growth", "M1 narrow money — annual growth (%) — real-economy LEAD"),
    ("MNA/Q.Y.I9.W2.S1.S1.B.B1GQ._Z._Z._Z.EUR.LR.GY", "gdp_yoy", "Euro-area real GDP — annual growth (%)"),
    ("MIR/M.U2.B.A2A.A.R.A.2240.EUR.N", "bank_rate_nfc", "Bank lending rate to corporations (%, new business) — policy pass-through"),
    ("EXR/D.CNY.EUR.SP00.A", "eurcny", "EUR/CNY reference rate"),
    ("EXR/D.JPY.EUR.SP00.A", "eurjpy", "EUR/JPY reference rate"),
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


# ── External + derived history: Eurostat EA confidence suite, production YoY, real M1 ──
EUROSTAT = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/ei_bssi_m_r2"
_UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
EUROSTAT_CONF = [
    ("conf_esi",          "Euro-area Economic Sentiment Indicator (ESI, long-run avg=100)", "BS-ESI-I"),
    ("conf_industrial",   "Euro-area industrial confidence (balance, %)",                   "BS-ICI-BAL"),
    ("conf_services",     "Euro-area services confidence (balance, %)",                     "BS-SCI-BAL"),
    ("conf_consumer",     "Euro-area consumer confidence (balance, %)",                     "BS-CSMCI-BAL"),
    ("conf_retail",       "Euro-area retail trade confidence (balance, %)",                 "BS-RCI-BAL"),
    ("conf_construction", "Euro-area construction confidence (balance, %)",                 "BS-CCI-BAL"),
]
PROD_YOY = {  # base index id -> YoY label
    "indprod_total":        "Industrial production — total incl. construction · YoY (%)",
    "indprod_core":         "Manufacturing production (excl. construction) · YoY (%)",
    "indprod_intermediate": "Industrial production — intermediate goods · YoY (%)",
    "indprod_capital":      "Industrial production — capital goods · YoY (%)",
    "indprod_durable":      "Industrial production — consumer durables · YoY (%)",
    "indprod_nondurable":   "Industrial production — consumer non-durables · YoY (%)",
    "indprod_energy":       "Industrial production — energy · YoY (%)",
}
CAPTURE = set(PROD_YOY) | {"m1_growth", "hicp_headline"}


def fetch_eurostat(indic, geo="EA20"):
    url = "%s?format=JSON&lang=EN&geo=%s&indic=%s&s_adj=SA" % (EUROSTAT, geo, indic)
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=_UA), timeout=45) as r:
            j = json.loads(r.read().decode("utf-8", "ignore"))
        idx = j["dimension"]["time"]["category"]["index"]; vals = j["value"]
        inv = {p: per for per, p in idx.items()}
        out = []
        for p in sorted(int(k) for k in vals.keys()):
            v = vals.get(str(p))
            if v is not None:
                out.append([inv[p], round(float(v), 2)])
        return out
    except Exception as e:
        print("eurostat %s: %s" % (indic, e)); return []


def yoy_series(pts):
    out = []
    for i in range(12, len(pts)):
        prev = pts[i - 12][1]
        if prev:
            out.append([pts[i][0], round((pts[i][1] / prev - 1) * 100, 2)])
    return out


def _round(v):
    if v is None: return None
    a = abs(v)
    if a == 0: return 0.0
    if a < 1: return round(v, 5)
    if a < 100: return round(v, 3)
    return round(v, 1)


def _stats_write(sid, label, freq, pts, source):
    """Stats + S3 write + manifest entry for a derived/external series (mirrors main loop)."""
    vals = [p[1] for p in pts]; latest = vals[-1]; n = len(vals)
    pctl = round(100 * sum(1 for v in vals if v <= latest) / n, 1)
    try:
        seg = vals[-260:] if n >= 260 else vals
        sd = statistics.pstdev(seg)
        z = round((latest - statistics.mean(seg)) / sd, 2) if sd else None
    except Exception:
        z = None
    try:
        ld = pts[-1][0]; ld10 = (ld + "-01")[:10] if len(ld) == 7 else ld[:10]
        stale = (datetime.now(timezone.utc).date() - datetime.strptime(ld10, "%Y-%m-%d").date()).days
    except Exception:
        stale = None
    out = {"id": sid, "label": label, "freq": freq, "flow_key": source,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "n_points": n, "first_date": pts[0][0], "latest_date": pts[-1][0],
           "latest": _round(latest), "min": _round(min(vals)), "max": _round(max(vals)),
           "percentile": pctl, "z_score": z, "points": pts}
    s3.put_object(Bucket=BUCKET, Key="data/ecb-hist/%s.json" % sid,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=43200")
    return {"id": sid, "label": label, "freq": freq, "latest": _round(latest),
            "percentile": pctl, "z_score": z, "first_date": pts[0][0],
            "latest_date": pts[-1][0], "n_points": n,
            "stale_days": stale, "discontinued": bool(stale and stale > 120)}


def lambda_handler(event=None, context=None):
    t0 = time.time(); written = []; manifest = []; captured = {}
    for flow_key, sid, label in SERIES:
        text = fetch_csv(flow_key)
        if not text: continue
        pts = parse(text)
        if len(pts) < 20: continue
        if sid in CAPTURE: captured[sid] = pts
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
        _lp = pts[-1][0]
        if "-W" in text[:200] or flow_key.startswith("ILM"):
            freq = "weekly"
        elif len(_lp) == 4:
            freq = "annual"
        elif "Q" in _lp:
            freq = "quarterly"
        elif len(_lp) == 7:
            freq = "monthly"
        else:
            freq = "daily"
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
    # ── Eurostat: euro-area Business & Consumer Survey confidence suite (history to 1980) ──
    for sid, label, ic in EUROSTAT_CONF:
        ep = fetch_eurostat(ic)
        if len(ep) >= 20:
            manifest.append(_stats_write(sid, label, "monthly", ep, "Eurostat ei_bssi_m_r2"))
            written.append(sid)
        time.sleep(0.3)
    # ── computed YoY for every production breakdown (full history from the index) ──
    for base, ylabel in PROD_YOY.items():
        bp = captured.get(base)
        if bp:
            yp = yoy_series(bp)
            if len(yp) >= 20:
                manifest.append(_stats_write(base + "_yoy", ylabel, "monthly", yp,
                                             "computed YoY from ECB STS index"))
                written.append(base + "_yoy")
    # ── real M1 growth = nominal M1 YoY − HICP YoY (money-supply lead, inflation-adjusted) ──
    def _pts(sid):
        if sid in captured:
            return captured[sid]
        try:
            return json.loads(s3.get_object(Bucket=BUCKET, Key="data/ecb-hist/%s.json" % sid)["Body"].read()).get("points")
        except Exception:
            return None
    m1 = _pts("m1_growth"); hp = _pts("hicp_headline")
    if m1 and hp:
        hy = {d: v for d, v in yoy_series(hp)}
        rp = [[d, round(v - hy[d], 2)] for d, v in m1 if d in hy]
        if len(rp) >= 20:
            manifest.append(_stats_write("real_m1_growth",
                                         "Real M1 growth (nominal M1 YoY − HICP YoY, %)",
                                         "monthly", rp, "computed real M1"))
            written.append("real_m1_growth")

    # HEAL-FROM-FILES: the manifest is rebuilt from EVERY data/ecb-hist/*.json on S3,
    # not just this builder's own list. Files written by any past or sibling writer
    # (hub v3 series, esi accumulator, ...) stay visible with full stats; nothing a
    # narrower builder deploy can ever erase from the page again.
    seen = {m["id"] for m in manifest}
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": "data/ecb-hist/", "MaxKeys": 200}
        if tok:
            kw["ContinuationToken"] = tok
        rr = s3.list_objects_v2(**kw)
        for o in rr.get("Contents", []):
            key = o["Key"]
            sid = key.split("/")[-1].replace(".json", "")
            if sid.startswith("_") or sid in seen:
                continue
            try:
                doc = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
                pts = [(p_[0], float(p_[1])) for p_ in (doc.get("points") or [])
                       if p_[1] is not None]
                if len(pts) < 5:
                    continue
                vals = [v for _, v in pts]
                latest = vals[-1]
                below = sum(1 for v in vals if v <= latest)
                pctl = round(100.0 * below / len(vals), 1)
                mu = statistics.mean(vals)
                sd = statistics.pstdev(vals)
                z = round((latest - mu) / sd, 2) if sd else None
                freq = doc.get("freq") or ("daily" if len(pts) > 3000 else
                                            "weekly" if len(pts) > 800 else
                                            "monthly" if len(pts) > 100 else "quarterly")
                ld = pts[-1][0]
                ld10 = (ld + "-01-01")[:10] if len(ld) == 4 else                        (ld + "-01")[:10] if len(ld) == 7 else ld[:10]
                try:
                    stale_days = (datetime.now(timezone.utc).date()
                                   - datetime.strptime(ld10, "%Y-%m-%d").date()).days
                except Exception:
                    stale_days = None
                sla = {"daily": 7, "weekly": 14, "monthly": 45,
                       "quarterly": 120, "annual": 430}.get(freq, 60)
                manifest.append({"id": sid, "label": doc.get("label") or sid, "freq": freq,
                                  "latest": round(latest, 5), "percentile": pctl, "z_score": z,
                                  "stale_days": stale_days,
                                  "discontinued": bool(stale_days is not None
                                                        and stale_days > sla * 3),
                                  "first_date": pts[0][0], "latest_date": pts[-1][0],
                                  "n_points": len(pts), "healed_from_file": True})
                seen.add(sid)
            except Exception as _e:
                print(f"[heal] {sid}: {str(_e)[:60]}")
        tok = rr.get("NextContinuationToken")
        if not tok:
            break
    manifest.sort(key=lambda m: m["id"])
    s3.put_object(Bucket=BUCKET, Key="data/ecb-hist/_manifest.json",
                  Body=json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(),
                                   "series": manifest, "n": len(manifest)}, default=str).encode(),
                  ContentType="application/json")
    print(f"[ecb-hist] wrote {len(written)} series in {round(time.time()-t0,1)}s: {written}")
    return {"statusCode": 200, "body": json.dumps({"written": len(written)})}
