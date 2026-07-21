"""justhodl-portwatch v1.0 — IMF PortWatch chokepoint & disruption engine.

Source discovered via Khalid's World Monitor reverse-engineering directive:
IMF PortWatch public ArcGIS FeatureServers (keyless, free). We query the
daily chokepoint transit series (Suez, Hormuz, Panama, Bab el-Mandeb,
Malacca, ...), compute 7d/30d activity vs a trailing-365 baseline + z-scores,
flag disruptions, and join IMF's own disruptions table. Feeds
data/portwatch.json. Attribution: IMF PortWatch (portwatch.imf.org),
UN Global Platform AIS. stdlib-only; source carries full history so no
self-ledger needed; never fabricates.
"""
import json
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "1.2.0"
BUCKET = "justhodl-dashboard-live"
KEY = "data/portwatch.json"
UA = {"User-Agent": "JustHodl research admin@justhodl.ai"}
S3 = boto3.client("s3", region_name="us-east-1")

BASE = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services"
CHOKE_REF = BASE + "/PortWatch_chokepoints_database/FeatureServer/0/query"
DAILY_CHOKE = BASE + "/Daily_Chokepoints_Data/FeatureServer/0/query"
DAILY_PORTS = BASE + "/Daily_Ports_Data/FeatureServer/0/query"
DISRUPT = BASE + "/portwatch_disruptions_database/FeatureServer/0/query"
PORTS_REF = BASE + "/PortWatch_ports_database/FeatureServer/0/query"
MAJOR_PORTS = ("shanghai", "singapore", "ningbo", "shenzhen",
               "qingdao", "busan", "rotterdam", "antwerp",
               "los angeles", "long beach", "hamburg",
               "jebel ali", "dubai", "houston", "santos",
               "tanjung pelepas", "port klang", "kaohsiung",
               "new york", "savannah", "yokohama", "tokyo",
               "haiphong", "ho chi minh", "cai mep", "mundra",
               "nhava sheva", "jawaharlal", "manzanillo",
               "bremerhaven", "colombo", "tanjung priok",
               "laem chabang", "gwangyang", "incheon")
# v1.2: export-nation aggregation — country -> gateway-port pulse
EXPORT_NATIONS = {"CHN": "China", "KOR": "Korea", "JPN": "Japan",
                  "TWN": "Taiwan", "SGP": "Singapore", "VNM": "Vietnam",
                  "DEU": "Germany", "IND": "India", "MEX": "Mexico",
                  "NLD": "Netherlands", "USA": "United States",
                  "MYS": "Malaysia", "THA": "Thailand", "IDN": "Indonesia",
                  "BRA": "Brazil", "ARE": "UAE", "LKA": "Sri Lanka",
                  "BEL": "Belgium"}


def _q(url, params, timeout=30):
    qs = urllib.parse.urlencode({**params, "f": "json"})
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(url + "?" + qs, headers=UA), timeout=timeout)
        j = json.loads(r.read())
        if isinstance(j, dict) and j.get("error"):
            return {"_err": str(j["error"])[:120]}
        return j
    except Exception as e:
        return {"_err": str(e)[:120]}


def _feats(j):
    return [f.get("attributes") or {} for f in (j.get("features") or [])]


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=400)
    since_ms = int(since.timestamp() * 1000)

    out = {"ok": False, "version": VERSION, "generated_at": now.isoformat(),
           "chokepoints": [], "disruptions": [], "errors": [],
           "attribution": "IMF PortWatch (portwatch.imf.org) — UN Global "
                          "Platform AIS; public ArcGIS FeatureServer, keyless"}

    # 1) chokepoint reference (id -> name)
    ref = _q(CHOKE_REF, {"where": "1=1", "outFields": "*",
                         "resultRecordCount": 60})
    if ref.get("_err"):
        out["errors"].append("ref: " + ref["_err"])
    names = {}
    for a in _feats(ref):
        pid = a.get("portid") or a.get("chokepoint_id") or a.get("id")
        nm = a.get("portname") or a.get("name") or a.get("fullname")
        if pid is not None and nm:
            names[str(pid)] = str(nm)
    out["ref_n"] = len(names)

    # 2) daily chokepoint series (primary layer; fallback = Daily_Ports
    #    filtered to chokepoint ids)
    def fetch_daily(url, extra_where=""):
        rows, offset = [], 0
        while offset < 60000:
            w = f"date >= timestamp '{since.strftime('%Y-%m-%d')}'"
            if extra_where:
                w += " AND " + extra_where
            j = _q(url, {"where": w, "outFields": "*",
                         "orderByFields": "date ASC",
                         "resultOffset": offset,
                         "resultRecordCount": 1000})
            if j.get("_err"):
                return rows, j["_err"]
            fs = _feats(j)
            rows += fs
            if len(fs) < 1000:
                return rows, None
            offset += 1000
        return rows, None

    rows, err = fetch_daily(DAILY_CHOKE)
    src_layer = "Daily_Chokepoints_Data"
    if err or not rows:
        if err:
            out["errors"].append("daily_choke: " + err)
        if names:
            ids = ",".join(sorted(names)[:30])
            rows, err2 = fetch_daily(DAILY_PORTS,
                                     f"portid IN ({ids})")
            src_layer = "Daily_Ports_Data(filtered)"
            if err2:
                out["errors"].append("daily_ports: " + err2)
    out["daily_layer"] = src_layer
    out["daily_rows"] = len(rows)

    # 3) aggregate per chokepoint
    by = {}
    metric_field = None
    for a in rows:
        pid = str(a.get("portid") or a.get("chokepoint_id") or "")
        d = a.get("date")
        if not pid or d is None:
            continue
        if metric_field is None:
            for cand in ("n_total", "n_transit", "transit_calls", "n_cargo",
                         "n_ships", "capacity"):
                if isinstance(a.get(cand), (int, float)):
                    metric_field = cand
                    break
        v = a.get(metric_field) if metric_field else None
        if not isinstance(v, (int, float)):
            continue
        try:
            if isinstance(d, (int, float)):
                ds = datetime.fromtimestamp(d / 1000, tz=timezone.utc).date()
            else:
                ds = datetime.fromisoformat(str(d)[:10]).date()
        except Exception:
            continue
        by.setdefault(pid, []).append((ds, float(v)))
    out["metric_field"] = metric_field
    out["pids_seen"] = {p: len(v) for p, v in list(by.items())[:6]}
    out["date_span"] = ([str(min(d for d, _ in v)) + ".." + str(max(d for d, _ in v))
                          for v in list(by.values())[:1]] or [None])[0]

    def stats(series):
        series.sort()
        vals = [v for _, v in series]
        if len(vals) < 30:
            return None
        last7 = vals[-7:]
        prev30 = vals[-37:-7] or vals[-30:]
        base = vals[:-7][-358:]
        m = sum(base) / len(base)
        var = sum((x - m) ** 2 for x in base) / len(base)
        sd = var ** 0.5 or 1e-9
        l7 = sum(last7) / len(last7)
        p30 = sum(prev30) / len(prev30)
        # same-window prior year if depth allows
        yoy = None
        if len(vals) >= 370:
            yr = vals[-372:-358]
            if yr:
                yprior = sum(yr) / len(yr)
                if yprior:
                    yoy = round(100 * (l7 / yprior - 1), 1)
        z = round((l7 - m) / sd, 2)
        pvy = round(100 * (l7 / m - 1), 1)
        status = ("DISRUPTED" if (z <= -1.5 or pvy <= -25) else
                  "ELEVATED" if (z >= 1.5 or pvy >= 25) else "NORMAL")
        return {"latest_7d_avg": round(l7, 1), "prev_30d_avg": round(p30, 1),
                "baseline_1y": round(m, 1), "z": z, "vs_baseline_pct": pvy,
                "yoy_pct": yoy, "n_days": len(vals),
                "last_date": str(series[-1][0]), "status": status}

    for pid, series in by.items():
        st = stats(series)
        if not st:
            continue
        out["chokepoints"].append({"id": pid,
                                   "name": names.get(pid, pid), **st})
    out["chokepoints"].sort(key=lambda c: c["z"])

    # 4) IMF disruptions table (recent)
    dj = _q(DISRUPT, {"where": "1=1", "outFields": "*",
                      "orderByFields": "OBJECTID DESC",
                      "resultRecordCount": 25})
    if dj.get("_err"):
        out["errors"].append("disrupt: " + dj["_err"])
    for a in _feats(dj)[:15]:
        out["disruptions"].append(
            {k: (str(v)[:120] if isinstance(v, str) else v)
             for k, v in a.items()
             if k.lower() in ("portname", "name", "date", "start", "end",
                              "comment", "type", "status", "chokepoint",
                              "objectid")})

    # v1.1: port-level activity layer (major ports, same stats frame)
    out["ports"] = []
    try:
        pref = _q(PORTS_REF, {"where": "1=1",
                              "outFields": "portid,portname,country,fullname",
                              "resultRecordCount": 2000})
        cand = {}
        for a2 in _feats(pref):
            nm = str(a2.get("portname") or a2.get("fullname") or "").lower()
            pid2 = a2.get("portid")
            if pid2 is None:
                continue
            for mp in MAJOR_PORTS:
                if mp in nm:
                    cand[str(pid2)] = {"name": a2.get("portname") or nm,
                                       "country": a2.get("country")}
                    break
        out["ports_ref_matched"] = len(cand)
        if cand:
            ids = ",".join("'" + i + "'" for i in list(cand)[:40])
            prow, perr = fetch_daily(DAILY_PORTS,
                                     "portid IN (" + ids + ")")
            if perr:
                out["errors"].append("ports_daily: " + perr)
            pby = {}
            pmetric = None
            for a2 in prow:
                pid2 = str(a2.get("portid") or "")
                d2 = a2.get("date")
                if pid2 not in cand or d2 is None:
                    continue
                if pmetric is None:
                    for c2 in ("portcalls", "n_total", "calls",
                               "import", "export"):
                        if isinstance(a2.get(c2), (int, float)):
                            pmetric = c2
                            break
                v2 = a2.get(pmetric) if pmetric else None
                if not isinstance(v2, (int, float)):
                    continue
                try:
                    if isinstance(d2, (int, float)):
                        ds2 = datetime.fromtimestamp(
                            d2 / 1000, tz=timezone.utc).date()
                    else:
                        ds2 = datetime.fromisoformat(str(d2)[:10]).date()
                except Exception:
                    continue
                pby.setdefault(pid2, []).append((ds2, float(v2)))
            out["ports_metric"] = pmetric
            for pid2, ser2 in pby.items():
                st2 = stats(ser2)
                if st2:
                    out["ports"].append({"id": pid2,
                                         "name": cand[pid2]["name"],
                                         "country": cand[pid2]["country"],
                                         **st2})
            out["ports"].sort(key=lambda p: p["z"])
    except Exception as _pe:
        out["errors"].append("ports_layer: " + str(_pe)[:90])
    out["ports_disrupted"] = sum(1 for p in out["ports"]
                                 if p["status"] == "DISRUPTED")
    # v1.2: exporters pulse — group ports by country code
    exp = {}
    for p in out["ports"]:
        cc = str(p.get("country") or "").upper()[:3]
        nm = EXPORT_NATIONS.get(cc)
        if not nm:
            continue
        exp.setdefault(cc, {"country": nm, "ports": [], "pcts": [],
                            "zs": []})
        exp[cc]["ports"].append(p["name"])
        exp[cc]["pcts"].append(p["vs_baseline_pct"])
        exp[cc]["zs"].append(p["z"])
    out["exporters"] = []
    for cc, e in exp.items():
        avg = round(sum(e["pcts"]) / len(e["pcts"]), 1)
        az = round(sum(e["zs"]) / len(e["zs"]), 2)
        verdict = ("SLOWING" if (avg <= -12 or az <= -1.2) else
                   "ACCELERATING" if (avg >= 12 or az >= 1.2) else "STABLE")
        out["exporters"].append({"code": cc, "country": e["country"],
                                 "n_ports": len(e["ports"]),
                                 "ports": e["ports"][:4],
                                 "avg_vs_baseline_pct": avg,
                                 "avg_z": az, "verdict": verdict})
    out["exporters"].sort(key=lambda x: x["avg_vs_baseline_pct"])
    out["exporters_slowing"] = [x["country"] for x in out["exporters"]
                                if x["verdict"] == "SLOWING"]

    worst = out["chokepoints"][0] if out["chokepoints"] else None
    out["worst"] = ({"name": worst["name"], "z": worst["z"],
                     "vs_baseline_pct": worst["vs_baseline_pct"],
                     "status": worst["status"]} if worst else None)
    out["n_disrupted"] = sum(1 for c in out["chokepoints"]
                             if c["status"] == "DISRUPTED")
    out["ok"] = len(out["chokepoints"]) >= 5

    S3.put_object(Bucket=BUCKET, Key=KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=900")
    print(f"[portwatch] layer={src_layer} rows={len(rows)} "
          f"chokepoints={len(out['chokepoints'])} metric={metric_field} "
          f"worst={out['worst']}")
    return {"ok": out["ok"], "chokepoints": len(out["chokepoints"]),
            "worst": out["worst"], "rows": len(rows)}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2)[:2000])
