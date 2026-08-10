"""justhodl-port-cargo v1.0 — the 2,065-port daily CARGO TONNAGE layer

ops-4559 BUG-11. portwatch ingests the 28-chokepoint daily layer and counts
vessels (n_total). The industry-boom signal is cargo TONNAGE by class at a
specific port — PortWatch publishes exactly that in Daily_Ports_Data (keyless
ArcGIS FeatureServer, per-port daily estimated import/export metric tons plus
per-vessel-class splits) and the platform was leaving it on the table.

Method:
  * schema-discover the layer (1-record probe, outFields=*) — tonnage fields
    are matched by name, never assumed, per the first-fetch assertion rule
  * pull a rolling window (last WINDOW_D days) with resultOffset pagination
  * per port: latest-7d import/export tons vs trailing-28d daily baseline
  * rollups: country, global pulse, top accelerating / decelerating ports
  * lag honesty: PortWatch publishes ~4-6d behind; data_age_days is explicit
    and fetch_status is separate from data freshness (the BUG-5 lesson).

OUTPUT: data/port-cargo.json    Daily 12:40 UTC.
lag_months ≈ -3 to -6 vs recognized revenue (physical precedes financial).
"""
import json, time, urllib.parse, urllib.request
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/port-cargo.json"
BASE = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services"
LAYER = BASE + "/Daily_Ports_Data/FeatureServer/0/query"
UA = {"User-Agent": "JustHodl/1.0 (ops@justhodl.ai)"}
WINDOW_D = 42          # pull window
BASE_D = 28            # baseline window inside it
RECENT_D = 7           # signal window
PAGE = 2000
MAX_PAGES = 260        # hard ceiling ≈ 520k rows (2065 ports × 42d ≈ 87k — generous)
EXPECTED_LAG_D = 6
s3 = boto3.client("s3", region_name=REGION)


def _q(url, params, t=60):
    """ArcGIS returns errors as HTTP-200 {"error": {...}} — surface, never
    swallow (ops-4559 follow-up: a where-clause type error looked exactly
    like an empty layer)."""
    try:
        req = urllib.request.Request(url + "?" + urllib.parse.urlencode(params), headers=UA)
        with urllib.request.urlopen(req, timeout=t) as r:
            j = json.loads(r.read().decode())
            if isinstance(j, dict) and j.get("error"):
                e = j["error"]
                return None, "arcgis error %s: %s" % (e.get("code"),
                                                      str(e.get("message"))[:120])
            return j, None
    except Exception as e:
        return None, "%s: %s" % (type(e).__name__, str(e)[:100])


def discover_fields(gaps):
    """Probe one record; classify tonnage vs portcall fields by name."""
    j, err = _q(LAYER, {"where": "1=1", "outFields": "*", "resultRecordCount": 1,
                        "orderByFields": "date DESC", "f": "pjson"})
    if err:  # orderBy on a differently-cased field can itself error — retry bare
        j, err = _q(LAYER, {"where": "1=1", "outFields": "*",
                            "resultRecordCount": 1, "f": "pjson"})
    feats = (j or {}).get("features") or []
    if not feats:
        gaps.append("Daily_Ports_Data probe returned 0 features%s"
                    % (" — " + err if err else ""))
        return None
    attrs = feats[0].get("attributes") or {}
    fields = sorted(attrs.keys())
    imp = [f for f in fields if f.lower().startswith("import")]
    exp = [f for f in fields if f.lower().startswith("export")]
    tot_imp = "import" if "import" in attrs else (imp[0] if imp else None)
    tot_exp = "export" if "export" in attrs else (exp[0] if exp else None)
    if not tot_imp or not tot_exp:
        gaps.append("no import/export tonnage fields found; fields=%s" % fields[:30])
        return None
    return {"all": fields, "import_total": tot_imp, "export_total": tot_exp,
            "import_class": [f for f in imp if f != tot_imp],
            "export_class": [f for f in exp if f != tot_exp],
            "sample": {k: attrs.get(k) for k in ("portid", "portname", "country", "date")}}


def negotiate_where(gaps):
    """ArcGIS date predicates vary by backend: probe TIMESTAMP / DATE /
    epoch-ms with returnCountOnly and take the first formulation that
    counts >0. Every rejection is recorded — first-fetch assertion."""
    dt_cut = datetime.now(timezone.utc) - timedelta(days=WINDOW_D)
    cands = ["date >= TIMESTAMP '%s'" % dt_cut.strftime("%Y-%m-%d %H:%M:%S"),
             "date >= DATE '%s'" % dt_cut.strftime("%Y-%m-%d"),
             "date >= %d" % int(dt_cut.timestamp() * 1000)]
    errs = []
    for w in cands:
        j, err = _q(LAYER, {"where": w, "returnCountOnly": "true", "f": "pjson"})
        cnt = (j or {}).get("count")
        if not err and isinstance(cnt, int) and cnt > 0:
            return w, cnt
        errs.append("%r → %s" % (w[:40], err or "count=%s" % cnt))
    gaps.append("no where-clause formulation returned rows: " + " | ".join(errs))
    return None, 0


def fetch_window(gaps):
    where, expected = negotiate_where(gaps)
    if not where:
        return []
    rows, offset = [], 0
    for _ in range(MAX_PAGES):
        j, err = _q(LAYER, {"where": where, "outFields": "*",
                            "resultOffset": offset, "resultRecordCount": PAGE,
                            "orderByFields": "date ASC", "f": "pjson"})
        if err:
            gaps.append("page fetch failed at offset %d: %s" % (offset, err))
            break
        feats = (j or {}).get("features") or []
        if not feats:
            break
        rows.extend(a.get("attributes") or {} for a in feats)
        # servers cap below the requested count (maxRecordCount) — advance by
        # what was RETURNED and stop only when the transfer-limit flag clears
        if not j.get("exceededTransferLimit") and len(feats) < PAGE:
            break
        offset += len(feats)
    else:
        gaps.append("pagination hit MAX_PAGES ceiling — window truncated")
    if expected and len(rows) < expected * 0.9:
        gaps.append("fetched %d of %d counted rows — partial window" % (len(rows), expected))
    return rows


def lambda_handler(event=None, context=None):
    t0 = time.time(); gaps = []
    schema = discover_fields(gaps)
    fetch_status = "OK" if schema else "FAILED"
    rows = fetch_window(gaps) if schema else []
    if schema and not rows:
        gaps.append("window fetch returned 0 rows"); fetch_status = "EMPTY"

    ports = defaultdict(lambda: {"days": {}})
    latest_ord = 0
    date_type_seen = set()
    fi, fe = (schema or {}).get("import_total"), (schema or {}).get("export_total")

    def _day_ord(v):
        """ISO string ('2026-06-29[..]') or epoch-ms number → ordinal day.
        v1.0.2 (ops 4576): the live layer serves ISO strings; int(v)
        crashed the whole run (4574 v2 caught the trace)."""
        if isinstance(v, (int, float)):
            date_type_seen.add("epoch_ms")
            return int(v / 86400000.0) if v > 10_000_000 else int(v)
        try:
            date_type_seen.add("iso_string")
            return datetime.fromisoformat(str(v)[:10]).toordinal()
        except Exception:
            return None

    for a in rows:
        pid = a.get("portid") or a.get("portname")
        od = _day_ord(a.get("date"))
        if not pid or od is None:
            continue
        latest_ord = max(latest_ord, od)
        p = ports[pid]
        p["name"] = a.get("portname") or str(pid)
        p["country"] = a.get("country") or a.get("ISO3") or "?"
        try:
            imp = float(a.get(fi) or 0); expo = float(a.get(fe) or 0)
        except Exception:
            imp = expo = 0.0
        p["days"][od] = (imp, expo)

    latest_date = (datetime.fromordinal(latest_ord).date()
                   if latest_ord else None)
    data_age_days = ((datetime.now(timezone.utc).date() - latest_date).days
                     if latest_date else None)

    port_rows, ctry = [], defaultdict(lambda: [0.0, 0.0, 0.0, 0.0])
    g_recent = [0.0, 0.0]; g_base = [0.0, 0.0]
    if latest_ord:
        recent_cut = latest_ord - RECENT_D
        base_cut = recent_cut - BASE_D
        for pid, p in ports.items():
            ri = re_ = bi = be = 0.0; rn = bn = 0
            for ts, (imp, expo) in p["days"].items():
                if ts > recent_cut:
                    ri += imp; re_ += expo; rn += 1
                elif ts > base_cut:
                    bi += imp; be += expo; bn += 1
            if rn == 0 or bn == 0:
                continue
            rid, red = ri / rn, re_ / rn          # daily-rate recent
            bid, bed = bi / bn, be / bn           # daily-rate baseline
            tot_r, tot_b = rid + red, bid + bed
            chg = round((tot_r / tot_b - 1) * 100, 1) if tot_b > 0 else None
            port_rows.append({"port": p["name"], "country": p["country"],
                              "import_tpd_7d": round(rid), "export_tpd_7d": round(red),
                              "import_tpd_base": round(bid), "export_tpd_base": round(bed),
                              "total_chg_pct": chg,
                              "import_chg_pct": round((rid / bid - 1) * 100, 1) if bid > 0 else None,
                              "export_chg_pct": round((red / bed - 1) * 100, 1) if bed > 0 else None})
            c = ctry[p["country"]]
            c[0] += rid; c[1] += red; c[2] += bid; c[3] += bed
            g_recent[0] += rid; g_recent[1] += red
            g_base[0] += bid; g_base[1] += bed

    port_rows.sort(key=lambda x: -(abs(x["total_chg_pct"]) if x["total_chg_pct"] is not None else 0))
    big = [r for r in port_rows if (r["import_tpd_base"] + r["export_tpd_base"]) > 5000]
    accel = sorted([r for r in big if (r["total_chg_pct"] or 0) > 0],
                   key=lambda x: -(x["total_chg_pct"] or 0))[:25]
    decel = sorted([r for r in big if (r["total_chg_pct"] or 0) < 0],
                   key=lambda x: (x["total_chg_pct"] or 0))[:25]
    countries = []
    for k, (ri, re_, bi, be) in ctry.items():
        tb = bi + be
        if tb < 1000:
            continue
        countries.append({"country": k, "import_tpd_7d": round(ri), "export_tpd_7d": round(re_),
                          "total_chg_pct": round(((ri + re_) / tb - 1) * 100, 1)})
    countries.sort(key=lambda x: -abs(x["total_chg_pct"]))
    gt_b = g_base[0] + g_base[1]
    global_pulse = {"import_tpd_7d": round(g_recent[0]), "export_tpd_7d": round(g_recent[1]),
                    "total_chg_pct": round(((g_recent[0] + g_recent[1]) / gt_b - 1) * 100, 2) if gt_b else None}

    out = {
        "engine": "port-cargo", "version": "1.0.2",
        "date_field_type": sorted(date_type_seen) or None,
        "engine_class": "physical_trade_fast_layer",
        "evidence_tier": "tier_1_measured_physical",
        "lag_months": -4,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "fetch_status": fetch_status,
        "latest_data_date": latest_date.isoformat() if latest_date else None,
        "data_age_days": data_age_days,
        "expected_lag_days": EXPECTED_LAG_D,
        "stale": (data_age_days is not None and data_age_days > EXPECTED_LAG_D + 4),
        "method": ("IMF PortWatch Daily_Ports_Data (keyless ArcGIS): per-port daily "
                   "estimated import/export cargo in METRIC TONS. 7d daily-rate vs "
                   "prior-28d baseline per port; country + global rollups. Fields are "
                   "schema-discovered each run, never assumed (ops-4559 BUG-11)."),
        "n_ports_with_data": len(port_rows),
        "n_rows_window": len(rows),
        "tonnage_fields": {"import": fi, "export": fe,
                           "class_fields_import": (schema or {}).get("import_class"),
                           "class_fields_export": (schema or {}).get("export_class")},
        "global_pulse": global_pulse,
        "countries": countries[:40],
        "accelerating_ports": accel,
        "decelerating_ports": decel,
        "gaps": gaps,
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print("[port-cargo] DONE %ss — ports=%d rows=%d age=%sd status=%s gaps=%d"
          % (round(time.time() - t0, 1), len(port_rows), len(rows), data_age_days,
             fetch_status, len(gaps)))
    return {"statusCode": 200, "body": json.dumps({"ok": fetch_status == "OK",
        "n_ports": len(port_rows), "data_age_days": data_age_days, "gaps": len(gaps)})}
