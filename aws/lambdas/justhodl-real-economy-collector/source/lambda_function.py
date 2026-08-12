"""justhodl-real-economy-collector v1.3.1 (ops 4614)

Institutional ingestion layer for the Physical/Real Economy signal —
the Google/Microsoft separation: this Lambda ONLY fetches and lands
raw evidence; the signal engine (physical-econ v2) only computes.

Every leg is an isolated fetcher behind its own error boundary and
lands a uniform envelope at data/warm/real-economy/{leg}.json:
  {leg_id, tier, status: OK|DEGRADED|FAILED, source, fetched_at,
   series:[{date,value}] | metrics:{...}, detail}
One leg's failure can never block another. Tiers are evidence classes:
  tier1 = keyed/keyless official APIs (hard SLO)
  tier2 = structured public files/pages (soft SLO)
  tier3 = best-effort scrapes (observed, never load-bearing)
Keys via env only (EIA_API_KEY, FRED_API_KEY) — never hardcoded.
Nothing is ever faked: a failed fetch lands status FAILED with the
verbatim reason.
v1.1.0 (per the 4614 truth table): EIA-930 on the daily route,
chokepoints with date-format negotiation, copper with a FRED
fallback, NOAA parser records its miss reason.
v1.2.0: link-follow resolution for AAR (news listing -> release
page, FRED BTS fallback) and Destatis (CSV href harvested off the
page); stooq mirror for copper; ACC CAB URL candidates.
v1.3.0 (Wave 4): DTS withheld taxes + customs (daily wage/import
reads), EIA Cushing/NG-storage/exports/gas-burn, NOAA upgraded to a
full daily degree-day series, WEI + four FRED cycle legs, FBX
observed.
"""
import csv
import io
import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
PREFIX = "data/warm/real-economy/"
EIA_KEY = os.environ.get("EIA_API_KEY", "")
FRED_KEY = os.environ.get("FRED_API_KEY",
                          "2f057499936072679d8843d7fce99989")
ARC = ("https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/"
       "services")
s3 = boto3.client("s3")


def http_get(url, timeout=25, headers=None):
    h = {"User-Agent": "Mozilla/5.0 (JustHodl real-economy; "
                       "ops@justhodl.ai)"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def land(leg_id, tier, source, status, series=None, metrics=None,
         detail=""):
    env = {"leg_id": leg_id, "tier": tier, "source": source,
           "status": status,
           "fetched_at": datetime.now(timezone.utc).isoformat(
               timespec="seconds"),
           "detail": str(detail)[:300]}
    if series:
        env["series"] = series[-600:]
        env["n_obs"] = len(series)
    if metrics:
        env["metrics"] = metrics
    s3.put_object(Bucket=BUCKET, Key=PREFIX + leg_id + ".json",
                  Body=json.dumps(env).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")
    return env["status"]


# ── tier-1: EIA v2 ───────────────────────────────────────────────────
def eia_seriesid(sid, length=260):
    qs = urllib.parse.urlencode({"api_key": EIA_KEY, "length": length})
    d = json.loads(http_get(
        "https://api.eia.gov/v2/seriesid/%s?%s" % (sid, qs), 30))
    rows = ((d.get("response") or {}).get("data")) or []
    out = []
    for x in rows:
        p, v = x.get("period"), x.get("value")
        if p and isinstance(v, (int, float)):
            out.append({"date": str(p)[:10], "value": float(v)})
    out.sort(key=lambda o: o["date"])
    return out


def leg_eia_weekly(leg_id, sid, label):
    if not EIA_KEY:
        return land(leg_id, "tier1", "EIA " + sid, "FAILED",
                    detail="EIA_API_KEY not set")
    try:
        se = eia_seriesid(sid)
        if not se:
            raise RuntimeError("0 rows")
        return land(leg_id, "tier1", "EIA " + sid, "OK", series=se,
                    detail=label)
    except Exception as e:
        return land(leg_id, "tier1", "EIA " + sid, "FAILED", detail=e)


def leg_eia930(leg_id, respondent):
    if not EIA_KEY:
        return land(leg_id, "tier1", "EIA-930", "FAILED",
                    detail="EIA_API_KEY not set")
    url = ("https://api.eia.gov/v2/electricity/rto/daily-region-data/"
           "data/?api_key=%s&frequency=daily&data[0]=value"
           "&facets[respondent][]=%s&facets[type][]=D"
           "&facets[timezone][]=Eastern"
           "&sort[0][column]=period&sort[0][direction]=desc"
           "&length=60" % (EIA_KEY, respondent))
    try:
        d = json.loads(http_get(url, 40))
        resp = d.get("response") or {}
        rows = resp.get("data") or []
        se = []
        for x in rows:
            p, v = str(x.get("period", ""))[:10], x.get("value")
            if v is None:
                continue
            try:
                se.append({"date": p, "value": round(
                    float(v) / 1000.0, 2)})
            except Exception:
                continue
        se.sort(key=lambda o: o["date"])
        if len(se) >= 2:
            se = se[:-1]  # partial current day off
        if not se:
            # fallback: hourly route, no start filter, aggregate
            qs2 = ("api_key=%s&frequency=hourly&data[0]=value"
                   "&facets[respondent][]=%s&facets[type][]=D"
                   "&sort[0][column]=period&sort[0][direction]=desc"
                   "&length=5000" % (EIA_KEY, respondent))
            d2 = json.loads(http_get(
                "https://api.eia.gov/v2/electricity/rto/region-data/"
                "data/?" + qs2, 40))
            daily = {}
            for x in ((d2.get("response") or {}).get("data") or []):
                p, v = str(x.get("period", "")), x.get("value")
                if len(p) >= 10 and v is not None:
                    try:
                        daily.setdefault(p[:10], 0.0)
                        daily[p[:10]] += float(v)
                    except Exception:
                        continue
            days = sorted(daily)[:-1]
            se = [{"date": dd, "value": round(daily[dd] / 1000.0, 2)}
                  for dd in days]
        if not se:
            raise RuntimeError(
                "0 rows (api total=%s, err=%s)"
                % (resp.get("total"),
                   str(d.get("error") or resp.get(
                       "warnings"))[:80]))
        return land(leg_id, "tier1", "EIA-930 %s demand" % respondent,
                    "OK", series=se,
                    detail="daily GWh (native daily route)")
    except Exception as e:
        return land(leg_id, "tier1", "EIA-930 " + respondent, "FAILED",
                    detail=e)


def leg_wti_term():
    if not EIA_KEY:
        return land("wti_term", "tier1", "EIA RCLC1/4", "FAILED",
                    detail="EIA_API_KEY not set")
    try:
        c1 = eia_seriesid("PET.RCLC1.D", 120)
        c4 = eia_seriesid("PET.RCLC4.D", 120)
        m4 = {o["date"]: o["value"] for o in c4}
        se = [{"date": o["date"],
               "value": round(o["value"] - m4[o["date"]], 3)}
              for o in c1 if o["date"] in m4]
        if not se:
            raise RuntimeError("no overlapping dates")
        state = "BACKWARDATION" if se[-1]["value"] > 0 else "CONTANGO"
        return land("wti_term", "tier1", "EIA NYMEX RCLC1-RCLC4",
                    "OK", series=se,
                    metrics={"latest_spread": se[-1]["value"],
                             "state": state,
                             "c1": c1[-1]["value"] if c1 else None},
                    detail="c1 minus c4, $/bbl; >0 = backwardation "
                           "(the doctrine crisis precursor)")
    except Exception as e:
        return land("wti_term", "tier1", "EIA RCLC1/4", "FAILED",
                    detail=e)


# ── tier-1: FRED ─────────────────────────────────────────────────────
def leg_fred(leg_id, sid, label, n=400):
    try:
        qs = urllib.parse.urlencode({
            "series_id": sid, "api_key": FRED_KEY, "file_type": "json",
            "sort_order": "desc", "limit": n})
        d = json.loads(http_get(
            "https://api.stlouisfed.org/fred/series/observations?"
            + qs, 25))
        se = [{"date": o["date"], "value": float(o["value"])}
              for o in d.get("observations", [])
              if o.get("value") not in (None, ".", "")]
        se.reverse()
        if not se:
            raise RuntimeError("0 obs")
        return land(leg_id, "tier1", "FRED " + sid, "OK", series=se,
                    detail=label)
    except Exception as e:
        return land(leg_id, "tier1", "FRED " + sid, "FAILED", detail=e)


# ── tier-1: PortWatch chokepoints (keyless ArcGIS, discovery) ───────
def leg_chokepoints():
    """The daily transit series lives in Daily_Chokepoints_Data —
    PortWatch_chokepoints_database is the static registry (4615
    lesson: keys were ISO3/LOCODE/country, no n_* fields)."""
    candidates = ["Daily_Chokepoints_Data", "Daily_Chokepoint_Data",
                  "Daily_Chokepoints", "chokepoints_daily"]
    url = None
    tried = []
    for name in candidates:
        u = ARC + "/%s/FeatureServer/0/query" % name
        try:
            j0 = json.loads(http_get(
                u + "?where=1%3D1&resultRecordCount=1&outFields=*"
                "&f=pjson", 25))
            f0 = ((j0.get("features") or [{}])[0].get("attributes")
                  or {})
            if any(k.lower().startswith("n_")
                   and isinstance(f0[k], (int, float)) for k in f0):
                url = u
                break
            tried.append(name + ":no n_* fields")
        except Exception as e0:
            tried.append(name + ":" + str(e0)[:40])
    if url is None:
        try:
            jd = json.loads(http_get(ARC + "?f=pjson", 25))
            for svc in jd.get("services", []):
                nm = (svc.get("name") or "").split("/")[-1]
                if "chokepoint" not in nm.lower():
                    continue
                u = ARC + "/%s/FeatureServer/0/query" % nm
                try:
                    j0 = json.loads(http_get(
                        u + "?where=1%3D1&resultRecordCount=1"
                        "&outFields=*&f=pjson", 25))
                    f0 = ((j0.get("features") or [{}])[0]
                          .get("attributes") or {})
                    if any(k.lower().startswith("n_")
                           and isinstance(f0[k], (int, float))
                           for k in f0):
                        url = u
                        break
                except Exception:
                    continue
        except Exception as e1:
            tried.append("dir:" + str(e1)[:40])
    if url is None:
        return land("chokepoints", "tier1", "PortWatch chokepoints",
                    "FAILED", detail="no daily layer with n_* "
                    "fields: " + " | ".join(tried)[:220])
    try:
        cutd = datetime.now(timezone.utc) - timedelta(days=70)
        probes = [
            "date >= DATE '%s'" % cutd.strftime("%Y-%m-%d"),
            "date >= TIMESTAMP '%s'" % cutd.strftime(
                "%Y-%m-%d %H:%M:%S"),
            "date >= %d" % int(cutd.timestamp() * 1000),
            "1=1",
        ]
        where, werr = None, []
        for w in probes:
            qs0 = urllib.parse.urlencode({
                "where": w, "returnCountOnly": "true", "f": "pjson"})
            j0 = json.loads(http_get(url + "?" + qs0, 30))
            if isinstance(j0, dict) and j0.get("error"):
                werr.append("%s -> %s" % (
                    w[:24], j0["error"].get("message", "")[:50]))
                continue
            if (j0.get("count") or 0) > 0:
                where = w
                break
            werr.append("%s -> count=0" % w[:24])
        if not where:
            raise RuntimeError("no where works: " + " | ".join(werr))
        rows, offset = [], 0
        for _ in range(8):
            qs = urllib.parse.urlencode({
                "where": where, "outFields": "*",
                "resultOffset": offset, "resultRecordCount": 2000,
                "f": "pjson"})
            j = json.loads(http_get(url + "?" + qs, 40))
            if isinstance(j, dict) and j.get("error"):
                raise RuntimeError("arcgis %s"
                                   % j["error"].get("message"))
            feats = j.get("features") or []
            rows.extend(f.get("attributes") or {} for f in feats)
            if not j.get("exceededTransferLimit") and len(feats) < 2000:
                break
            offset += len(feats)
        if not rows:
            raise RuntimeError("0 rows in 70d window")
        nf = [k for k in rows[0]
              if k.lower().startswith("n_")
              and isinstance(rows[0][k], (int, float))]
        tot_f = ("n_total" if "n_total" in rows[0]
                 else (nf[0] if nf else None))
        if not tot_f:
            raise RuntimeError("no n_* transit fields; keys=%s"
                               % sorted(rows[0])[:14])
        daily = {}
        for a in rows:
            d0 = str(a.get("date", ""))[:10]
            v = a.get(tot_f)
            if len(d0) == 10 and isinstance(v, (int, float)):
                daily.setdefault(d0, 0.0)
                daily[d0] += float(v)
        se = [{"date": d0, "value": round(daily[d0], 1)}
              for d0 in sorted(daily)][-75:]
        return land("chokepoints", "tier1",
                    "IMF PortWatch chokepoints (ArcGIS)", "OK",
                    series=se,
                    metrics={"transit_field": tot_f,
                             "n_chokepoint_rows": len(rows)},
                    detail="daily global chokepoint transits "
                           "(Suez/Panama/Malacca/Bab-el-Mandeb...)")
    except Exception as e:
        return land("chokepoints", "tier1", "PortWatch chokepoints",
                    "FAILED", detail=e)


# ── tier-1: Indeed Hiring Lab (public GitHub CSV) ───────────────────
def leg_indeed():
    urls = [
        "https://raw.githubusercontent.com/hiring-lab/data/master/"
        "US/aggregate_job_postings_US.csv",
        "https://raw.githubusercontent.com/hiring-lab/"
        "job_postings_tracker/master/US/aggregate_job_postings_US.csv",
    ]
    last = ""
    for u in urls:
        try:
            txt = http_get(u, 25).decode("utf-8", "replace")
            rdr = csv.DictReader(io.StringIO(txt))
            se = []
            for row in rdr:
                dk = next((k for k in row if "date" in k.lower()), None)
                vk = next((k for k in row
                           if "postings" in k.lower()
                           and "new" not in k.lower()), None)
                if not dk or not vk:
                    break
                try:
                    se.append({"date": row[dk][:10],
                               "value": float(row[vk])})
                except Exception:
                    continue
            if len(se) > 100:
                se.sort(key=lambda o: o["date"])
                return land("indeed_postings", "tier1",
                            "Indeed Hiring Lab (GitHub)", "OK",
                            series=se, detail="daily US job postings "
                            "index, Feb-2020=100")
            last = "parsed %d rows" % len(se)
        except Exception as e:
            last = str(e)[:120]
    return land("indeed_postings", "tier1", "Indeed Hiring Lab",
                "FAILED", detail=last)


# ── tier-2: structured public files/pages ───────────────────────────
def leg_dts():
    """Daily Treasury Statement (fiscaldata, keyless): withheld
    income/FICA taxes = fastest hard wage read; customs duties =
    physical imports proxy. One fetch lands two legs."""
    try:
        cut = (datetime.now(timezone.utc)
               - timedelta(days=430)).strftime("%Y-%m-%d")
        base = ("https://api.fiscaldata.treasury.gov/services/api/"
                "fiscal_service/v1/accounting/dts/"
                "deposits_withdrawals_operating_cash")
        rows = []
        for page in range(1, 6):
            qs = ("?fields=record_date,transaction_catg,"
                  "transaction_type,transaction_today_amt"
                  "&filter=record_date:gte:%s"
                  "&sort=-record_date&page[size]=10000"
                  "&page[number]=%d" % (cut, page))
            d = json.loads(http_get(base + qs, 60))
            batch = d.get("data") or []
            rows.extend(batch)
            if len(batch) < 10000:
                break
        wh, cu = {}, {}
        for x in rows:
            if x.get("transaction_type") != "Deposits":
                continue
            catg = str(x.get("transaction_catg", ""))
            try:
                amt = float(x.get("transaction_today_amt"))
            except Exception:
                continue
            d0 = x.get("record_date")
            if "Withheld" in catg:
                wh[d0] = wh.get(d0, 0.0) + amt
            elif "Customs" in catg:
                cu[d0] = cu.get(d0, 0.0) + amt
        st_w = "FAILED"
        if wh:
            se = [{"date": k, "value": round(v, 1)}
                  for k, v in sorted(wh.items())]
            st_w = land("dts_withheld", "tier1",
                        "DTS withheld income/FICA taxes", "OK",
                        series=se,
                        detail="daily $M deposits — aggregate wages")
        else:
            st_w = land("dts_withheld", "tier1", "DTS", "FAILED",
                        detail="no Withheld rows in %d" % len(rows))
        if cu:
            se = [{"date": k, "value": round(v, 1)}
                  for k, v in sorted(cu.items())]
            land("dts_customs", "tier1", "DTS customs duties", "OK",
                 series=se, detail="daily $M — physical imports proxy")
        else:
            land("dts_customs", "tier1", "DTS", "FAILED",
                 detail="no Customs rows in %d" % len(rows))
        return st_w
    except Exception as e:
        land("dts_customs", "tier1", "DTS", "FAILED", detail=e)
        return land("dts_withheld", "tier1", "DTS", "FAILED", detail=e)


def leg_eia930_fuel(leg_id, respondent, fuel):
    if not EIA_KEY:
        return land(leg_id, "tier1", "EIA-930 fuel", "FAILED",
                    detail="EIA_API_KEY not set")
    url = ("https://api.eia.gov/v2/electricity/rto/"
           "daily-fuel-type-data/data/?api_key=%s&frequency=daily"
           "&data[0]=value&facets[respondent][]=%s"
           "&facets[fueltype][]=%s&facets[timezone][]=Eastern"
           "&sort[0][column]=period&sort[0][direction]=desc"
           "&length=60" % (EIA_KEY, respondent, fuel))
    try:
        d = json.loads(http_get(url, 40))
        rows = ((d.get("response") or {}).get("data")) or []
        se = []
        for x in rows:
            p, v = str(x.get("period", ""))[:10], x.get("value")
            if v is None:
                continue
            try:
                se.append({"date": p,
                           "value": round(float(v) / 1000.0, 2)})
            except Exception:
                continue
        se.sort(key=lambda o: o["date"])
        if len(se) >= 2:
            se = se[:-1]
        if not se:
            raise RuntimeError("0 rows")
        return land(leg_id, "tier1",
                    "EIA-930 %s %s generation" % (respondent, fuel),
                    "OK", series=se, detail="daily GWh")
    except Exception as e:
        return land(leg_id, "tier1", "EIA-930 fuel", "FAILED",
                    detail=e)


def leg_fbx():
    try:
        html = http_get("https://fbx.freightos.com/", 25).decode(
            "utf-8", "replace")
        m = re.search(r"FBX[^0-9$]{0,60}\$?\s*([\d,]{3,6}"
                      r"(?:\.\d+)?)", html, re.I | re.S)
        if not m:
            raise RuntimeError("headline number not found")
        return land("fbx", "tier2", "Freightos FBX", "OK",
                    metrics={"fbx_usd":
                             float(m.group(1).replace(",", ""))},
                    detail="global container spot headline $/FEU")
    except Exception as e:
        return land("fbx", "tier2", "Freightos FBX", "DEGRADED",
                    detail=e)


def leg_tsa():
    try:
        html = http_get("https://www.tsa.gov/travel/passenger-volumes",
                        25).decode("utf-8", "replace")
        pairs = re.findall(
            r"(\d{1,2}/\d{1,2}/\d{4})[^0-9]*?([\d,]{6,})", html)
        se = []
        for d0, v in pairs[:60]:
            try:
                dt = datetime.strptime(d0, "%m/%d/%Y").date()
                se.append({"date": dt.isoformat(),
                           "value": float(v.replace(",", ""))})
            except Exception:
                continue
        se.sort(key=lambda o: o["date"])
        if len(se) < 5:
            raise RuntimeError("parsed %d rows" % len(se))
        return land("tsa", "tier2", "TSA checkpoint volumes", "OK",
                    series=se, detail="daily travelers through US "
                    "checkpoints")
    except Exception as e:
        return land("tsa", "tier2", "TSA", "DEGRADED", detail=e)


def leg_aisi():
    try:
        html = http_get("https://www.steel.org/industry-data/",
                        25).decode("utf-8", "replace")
        m = re.search(r"capability utilization rate of\s+([\d.]+)\s*"
                      r"percent", html, re.I)
        m2 = re.search(r"production was\s+([\d,]+)\s*net tons", html,
                       re.I)
        if not m:
            raise RuntimeError("utilization phrase not found")
        return land("aisi_steel", "tier2", "AISI weekly raw steel",
                    "OK",
                    metrics={"capacity_utilization_pct":
                             float(m.group(1)),
                             "weekly_net_tons":
                             float(m2.group(1).replace(",", ""))
                             if m2 else None},
                    detail="measured weekly steel output")
    except Exception as e:
        return land("aisi_steel", "tier2", "AISI", "DEGRADED",
                    detail=e)


def leg_copper():
    try:
        txt = ""
        for su in ("https://stooq.com/q/d/l/?s=hg.f&i=d",
                   "https://stooq.pl/q/d/l/?s=hg.f&i=d"):
            try:
                txt = http_get(su, 25).decode("utf-8", "replace")
                if txt.count("\n") > 30:
                    break
            except Exception:
                continue
        rdr = csv.DictReader(io.StringIO(txt))
        se = []
        for row in rdr:
            try:
                se.append({"date": row["Date"],
                           "value": float(row["Close"])})
            except Exception:
                continue
        se = se[-260:]
        if len(se) < 30:
            raise RuntimeError("parsed %d rows" % len(se))
        return land("copper", "tier2", "COMEX copper (stooq)", "OK",
                    series=se, detail="continuous front-month close; "
                    "priced-not-measured — tier-2 by design")
    except Exception as e:
        try:
            qs = urllib.parse.urlencode({
                "series_id": "PCOPPUSDM", "api_key": FRED_KEY,
                "file_type": "json", "sort_order": "desc",
                "limit": 60})
            d = json.loads(http_get(
                "https://api.stlouisfed.org/fred/series/"
                "observations?" + qs, 25))
            se = [{"date": o["date"], "value": float(o["value"])}
                  for o in d.get("observations", [])
                  if o.get("value") not in (None, ".", "")]
            se.reverse()
            if se:
                return land("copper", "tier2",
                            "FRED PCOPPUSDM (fallback)", "OK",
                            series=se,
                            detail="stooq empty (%s) — IMF monthly "
                                   "copper via FRED" % str(e)[:60])
        except Exception:
            pass
        return land("copper", "tier2", "copper stooq", "DEGRADED",
                    detail=e)


def leg_noaa_dd():
    """v1.3.0: land the FULL daily national CDD/HDD series (dates in
    the header row, regions down the rows) so the signal layer can
    weather-adjust the power legs by regression."""
    yr = datetime.now(timezone.utc).year
    series = {}
    for kind in ("Cooling", "Heating"):
        try:
            txt = http_get(
                "https://ftp.cpc.ncep.noaa.gov/htdocs/degree_days/"
                "weighted/daily_data/%d/Population.%s.txt"
                % (yr, kind), 30).decode("utf-8", "replace")
            lines = [l for l in txt.splitlines() if l.strip()]
            hdr = next((l for l in lines
                        if re.search(r"\d{8}.*\d{8}", l)), "")
            dates = re.findall(r"(\d{8})", hdr)
            usl = next((l for l in lines
                        if l.strip().upper().startswith(
                            ("UNITED STATES", "CONUS", "US "))
                        or "|UNITED STATES" in l.upper()), "")
            if "|" in usl:
                cells = [c.strip() for c in usl.split("|")[1:]]
                vals = [c for c in cells
                        if c.replace(".", "", 1).isdigit()]
            else:
                vals = re.findall(r"(\d+)", usl)[1:]
            n = min(len(dates), len(vals))
            if n >= 30:
                series[kind.lower()] = [
                    {"date": "%s-%s-%s" % (dates[k][:4],
                                           dates[k][4:6],
                                           dates[k][6:8]),
                     "value": float(vals[len(vals) - n + k])}
                    for k in range(n)]
        except Exception as e:
            series[kind.lower() + "_err"] = str(e)[:80]
    cdd = series.get("cooling")
    if cdd:
        env = {"leg_id": "noaa_degree_days", "tier": "tier2",
               "source": "NOAA CPC population-weighted degree days",
               "status": "OK",
               "fetched_at": datetime.now(timezone.utc).isoformat(
                   timespec="seconds"),
               "series": cdd[-400:], "n_obs": len(cdd),
               "hdd_latest": (series.get("heating") or [{}])[-1]
               .get("value"),
               "detail": "daily national CDD series (US line, header "
                         "dates); HDD latest attached"}
        s3.put_object(Bucket=BUCKET,
                      Key=PREFIX + "noaa_degree_days.json",
                      Body=json.dumps(env).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=300")
        return "OK"
    return land("noaa_degree_days", "tier2", "NOAA CPC", "DEGRADED",
                detail=json.dumps({k: v for k, v in series.items()
                                   if k.endswith("_err")})[:200]
                or "US row/header not parsed")


def _legacy_noaa_unused():
    yr = datetime.now(timezone.utc).year
    got = {}
    for kind in ("Cooling", "Heating"):
        try:
            txt = http_get(
                "https://ftp.cpc.ncep.noaa.gov/htdocs/degree_days/"
                "weighted/daily_data/%d/Population.%s.txt"
                % (yr, kind), 25).decode("utf-8", "replace")
            hit = False
            for line in txt.splitlines():
                u = line.strip().upper()
                if (u.startswith("UNITED STATES") or u.startswith(
                        "CONUS") or u.startswith("US ")):
                    nums = re.findall(r"(\d+)", line)
                    if nums:
                        got[kind.lower() + "_dd_latest"] = int(
                            nums[-1])
                        hit = True
                    break
            if not hit:
                got[kind.lower() + "_err"] = ("US line not found; "
                                              "head=%s"
                                              % txt[:60].replace(
                                                  "\n", " "))
        except Exception as e:
            got[kind.lower() + "_err"] = str(e)[:80]
    if any(k.endswith("_dd_latest") for k in got):
        return land("noaa_degree_days", "tier2",
                    "NOAA CPC population-weighted degree days", "OK",
                    metrics=got, detail="weather context for the "
                    "power-demand legs")
    return land("noaa_degree_days", "tier2", "NOAA CPC", "DEGRADED",
                detail=json.dumps(got)[:200])


# ── tier-3: best-effort, never load-bearing ─────────────────────────
def follow_link(page_url, href_re):
    html = http_get(page_url, 25).decode("utf-8", "replace")
    m = re.search(href_re, html, re.I)
    if not m:
        return None, html
    href = m.group(1)
    if href.startswith("//"):
        href = "https:" + href
    elif href.startswith("/"):
        base = "/".join(page_url.split("/")[:3])
        href = base + href
    elif not href.startswith("http"):
        href = page_url.rsplit("/", 1)[0] + "/" + href
    return href, html


def leg_aar():
    try:
        link, _ = follow_link(
            "https://www.aar.org/news/",
            r'href="([^"]*rail-traffic[^"]*)"')
        if link:
            page = http_get(link, 25).decode("utf-8", "replace")
            m1 = re.search(r"([\d,]{6,})\s*carloads", page, re.I)
            m2 = re.search(r"([\d,]{6,})\s*(?:intermodal|containers"
                           r"\s+and\s+trailers)", page, re.I)
            if m1:
                return land(
                    "aar_rail", "tier2", "AAR weekly rail traffic",
                    "OK",
                    metrics={"weekly_carloads":
                             float(m1.group(1).replace(",", "")),
                             "weekly_intermodal":
                             float(m2.group(1).replace(",", ""))
                             if m2 else None,
                             "release": link[-80:]},
                    detail="link-followed weekly release")
        raise RuntimeError("no traffic link/number on aar.org")
    except Exception as e:
        try:
            qs = urllib.parse.urlencode({
                "series_id": "RAILFRTCARLOADSD11",
                "api_key": FRED_KEY, "file_type": "json",
                "sort_order": "desc", "limit": 60})
            d = json.loads(http_get(
                "https://api.stlouisfed.org/fred/series/"
                "observations?" + qs, 25))
            se = [{"date": o["date"], "value": float(o["value"])}
                  for o in d.get("observations", [])
                  if o.get("value") not in (None, ".", "")]
            se.reverse()
            if se:
                return land("aar_rail", "tier2",
                            "BTS rail carloads via FRED (fallback)",
                            "OK", series=se,
                            detail="monthly SA carloads; AAR page "
                                   "miss: %s" % str(e)[:60])
        except Exception:
            pass
        return land("aar_rail", "tier2", "AAR", "DEGRADED", detail=e)


def leg_destatis():
    # candidate 0: GENESIS guest API, table 42191-0001 (the documented
    # open path — the EXDAT pages carry no CSV href per ops-4617)
    try:
        raw = http_get(
            "https://www-genesis.destatis.de/genesisWS/rest/2020/"
            "data/tablefile?username=GAST&password=GAST"
            "&name=42191-0001&area=all&format=ffcsv"
            "&compress=false", 40).decode("utf-8-sig", "replace")
        se = []
        rdr = csv.DictReader(io.StringIO(raw), delimiter=";")
        for row in rdr:
            dk = next((k for k in row if k and "zeit" in k.lower()
                       or (k and "time" in k.lower())), None)
            vk = next((k for k in row
                       if k and ("wert" in k.lower()
                                 or "value" in k.lower())), None)
            if not dk or not vk:
                break
            d0 = str(row[dk]).strip()
            dt = None
            for f in ("%d.%m.%Y", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(d0, f).date()
                    break
                except Exception:
                    continue
            if not dt:
                continue
            try:
                se.append({"date": dt.isoformat(),
                           "value": float(str(row[vk])
                                          .replace(",", "."))})
            except Exception:
                continue
        se.sort(key=lambda o: o["date"])
        if len(se) > 60:
            return land("destatis_toll", "tier2",
                        "Destatis GENESIS 42191-0001 (guest)", "OK",
                        series=se[-400:],
                        detail="daily truck-toll mileage index via "
                               "GENESIS guest API")
    except Exception:
        pass
    pages = [
        "https://www.destatis.de/EN/Service/EXDAT/Datensaetze/"
        "truck-toll-mileage.html",
        "https://www.destatis.de/DE/Service/EXDAT/Datensaetze/"
        "lkw-maut.html",
    ]
    tried = []
    for pg in pages:
        try:
            link, _ = follow_link(
                pg, r'href="([^"]+(?:maut|toll)[^"]*\.(?:csv|CSV))"')
            if not link:
                tried.append(pg.split("/")[-1] + ":no csv href")
                continue
            raw = http_get(link, 30).decode("utf-8-sig", "replace")
            delim = ";" if raw.count(";") > raw.count(",") else ","
            se = []
            for row in csv.reader(io.StringIO(raw), delimiter=delim):
                if len(row) < 2:
                    continue
                d0 = row[0].strip()
                dt = None
                for f in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
                    try:
                        dt = datetime.strptime(d0, f).date()
                        break
                    except Exception:
                        continue
                if not dt:
                    continue
                try:
                    se.append({"date": dt.isoformat(),
                               "value": float(
                                   row[-1].replace(",", "."))})
                except Exception:
                    continue
            se.sort(key=lambda o: o["date"])
            if len(se) > 60:
                return land("destatis_toll", "tier2",
                            "Destatis truck-toll mileage (CSV)",
                            "OK", series=se[-400:],
                            detail="daily DE truck-toll index via "
                                   "harvested %s" % link[-60:])
            tried.append("csv parsed %d rows" % len(se))
        except Exception as e:
            tried.append(str(e)[:60])
    return land("destatis_toll", "tier2", "Destatis", "DEGRADED",
                detail=" | ".join(tried)[:250])


def leg_acc():
    urls = [
        "https://www.americanchemistry.com/chemistry-in-america/"
        "data-industry-statistics/resources/"
        "chemical-activity-barometer",
        "https://www.americanchemistry.com/chemistry-in-america/"
        "data-industry-statistics",
    ]
    last = ""
    for u in urls:
        try:
            html = http_get(u, 25).decode("utf-8", "replace")
            m = re.search(r"barometer[^0-9]{0,160}?(1[0-9]{2}\.[0-9])",
                          html, re.I | re.S)
            if m:
                return land("acc_cab", "tier3", "ACC CAB", "OK",
                            metrics={"value": float(m.group(1))},
                            detail="Chemical Activity Barometer")
            last = "no barometer number on " + u.split("/")[-1]
        except Exception as e:
            last = str(e)[:80]
    return land("acc_cab", "tier3", "ACC CAB", "DEGRADED", detail=last)


def leg_scrape3(leg_id, url, pattern, label, transform=float):
    try:
        html = http_get(url, 25).decode("utf-8", "replace")
        m = re.search(pattern, html, re.I | re.S)
        if not m:
            raise RuntimeError("pattern not found")
        return land(leg_id, "tier3", url.split("/")[2], "OK",
                    metrics={"value": transform(
                        m.group(1).replace(",", ""))},
                    detail=label)
    except Exception as e:
        return land(leg_id, "tier3", url.split("/")[2], "DEGRADED",
                    detail=e)


def lambda_handler(event, context):
    t0 = time.time()
    results = {}
    jobs = [
        ("eia_distillate", lambda: leg_eia_weekly(
            "eia_distillate", "PET.WDIUPUS2.W",
            "distillate product supplied — diesel IS freight")),
        ("eia_gasoline", lambda: leg_eia_weekly(
            "eia_gasoline", "PET.WGFUPUS2.W",
            "gasoline supplied — miles driven")),
        ("eia_jet", lambda: leg_eia_weekly(
            "eia_jet", "PET.WKJUPUS2.W", "jet fuel supplied")),
        ("eia_refinery", lambda: leg_eia_weekly(
            "eia_refinery", "PET.WCRRIUS2.W",
            "refinery net crude inputs — industrial runs")),
        ("eia930_us48", lambda: leg_eia930("eia930_us48", "US48")),
        ("eia930_ercot", lambda: leg_eia930("eia930_ercot", "ERCO")),
        ("wti_term", leg_wti_term),
        ("chokepoints", leg_chokepoints),
        ("fred_claims", lambda: leg_fred(
            "fred_claims", "ICSA", "initial claims (inverted leg)")),
        ("fred_houst", lambda: leg_fred(
            "fred_houst", "HOUST", "housing starts SAAR", 200)),
        ("fred_permit", lambda: leg_fred(
            "fred_permit", "PERMIT", "building permits SAAR", 200)),
        ("fred_hours", lambda: leg_fred(
            "fred_hours", "AWHI",
            "aggregate weekly hours index — labor as quantity", 200)),
        ("indeed_postings", leg_indeed),
        ("dts_withheld", leg_dts),
        ("eia_cushing", lambda: leg_eia_weekly(
            "eia_cushing", "PET.W_EPC0_SAX_YCUOK_MBBL.W",
            "Cushing OK crude stocks — WTI delivery point")),
        ("eia_ng_storage", lambda: leg_eia_weekly(
            "eia_ng_storage", "NG.NW2_EPG0_SWO_R48_BCF.W",
            "L48 working gas in storage")),
        ("eia_exports", lambda: leg_eia_weekly(
            "eia_exports", "PET.WTTEXUS2.W",
            "US crude + product exports — physical trade")),
        ("eia930_gasburn", lambda: leg_eia930_fuel(
            "eia930_gasburn", "US48", "NG")),
        ("fred_wei", lambda: leg_fred(
            "fred_wei", "WEI",
            "NY Fed Weekly Economic Index — replication benchmark",
            200)),
        ("fred_neworder", lambda: leg_fred(
            "fred_neworder", "NEWORDER",
            "core capex orders (nondef ex-air)", 200)),
        ("fred_isratio", lambda: leg_fred(
            "fred_isratio", "ISRATIO",
            "business inventories/sales (inverted leg)", 200)),
        ("fred_awhman", lambda: leg_fred(
            "fred_awhman", "AWHMAN",
            "manufacturing weekly hours — the classic leader", 200)),
        ("fred_ttlcons", lambda: leg_fred(
            "fred_ttlcons", "TTLCONS",
            "construction spending", 200)),
        ("fbx", leg_fbx),
        ("tsa", leg_tsa),
        ("aisi_steel", leg_aisi),
        ("copper", leg_copper),
        ("noaa_degree_days", leg_noaa_dd),
        ("aar_rail", leg_aar),
        ("destatis_toll", leg_destatis),
        ("acc_cab", leg_acc),
    ]
    for leg_id, fn in jobs:
        try:
            results[leg_id] = fn()
        except Exception as e:
            try:
                results[leg_id] = land(leg_id, "tier3", "boundary",
                                       "FAILED", detail=e)
            except Exception:
                results[leg_id] = "FAILED"
        time.sleep(0.3)

    counts = {}
    for v in results.values():
        counts[v] = counts.get(v, 0) + 1
    summary = {"schema_version": "1.0",
               "engine": "justhodl-real-economy-collector",
               "as_of": datetime.now(timezone.utc).isoformat(
                   timespec="seconds"),
               "duration_s": round(time.time() - t0, 1),
               "legs": results, "counts": counts,
               "declared_not_public": ["mba_purchase_apps",
                                       "aia_abi"]}
    s3.put_object(Bucket=BUCKET, Key=PREFIX + "_summary.json",
                  Body=json.dumps(summary).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")
    t1_ok = sum(1 for k, fn in [] if False)
    return {"statusCode": 200,
            "body": json.dumps({"ok": counts.get("OK", 0) >= 8,
                                "counts": counts,
                                "duration_s": summary["duration_s"]})}
