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

from impact_mapper import (build as impact_build, load_graph,
                           measured_row, beta_impact)

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/port-cargo.json"
BASE = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services"
# v1.2.0 (ops 4612): LAYER + DATEFIELD are resolved at runtime — the
# hardcoded single service name was the FAILED root: one rename upstream
# and every probe returns 0 features. Resolution: cached choice ->
# known candidates -> live org services directory.
SERVICE_CANDIDATES = [
    "Daily_Ports_Data",
    "Daily_Port_Activity_Data_and_Trade_Estimates",
    "Daily_Trade_and_Ports_Data",
    "PortWatch_Daily_Ports_Data",
]
LAYER = BASE + "/Daily_Ports_Data/FeatureServer/0/query"
DATEFIELD = "date"
CHOICE_KEY = "data/warm/portwatch/layer-choice.json"
RESOLVER_PATH = "unresolved"
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


def _probe_layer(url):
    j, err = _q(url, {"where": "1=1", "outFields": "*",
                      "resultRecordCount": 1, "f": "pjson"}, t=30)
    feats = (j or {}).get("features") or []
    if not feats:
        return None, err or "0 features"
    return feats[0].get("attributes") or {}, None


def _detect_datefield(attrs):
    for k in attrs:
        if k.lower() == "date":
            return k
    for k in attrs:
        if "date" in k.lower():
            return k
    return "date"


def _persist_choice(service, layer_idx):
    try:
        s3.put_object(
            Bucket=BUCKET, Key=CHOICE_KEY,
            Body=json.dumps({
                "layer_url": LAYER, "service": service,
                "layer_index": layer_idx, "datefield": DATEFIELD,
                "chosen_at": datetime.now(timezone.utc).isoformat(
                    timespec="seconds")}).encode(),
            ContentType="application/json")
    except Exception as e:
        print("[port] persist choice: %s" % e)


def resolve_layer(gaps):
    """v1.2.0 self-healing layer resolution. Sets module LAYER /
    DATEFIELD; every rejection recorded (first-fetch assertion)."""
    global LAYER, DATEFIELD, RESOLVER_PATH
    tried = []
    try:
        ch = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key=CHOICE_KEY)["Body"].read())
        url = ch.get("layer_url")
        if url:
            attrs, err = _probe_layer(url)
            if attrs is not None:
                LAYER = url
                DATEFIELD = (ch.get("datefield")
                             or _detect_datefield(attrs))
                RESOLVER_PATH = "cached:" + str(ch.get("service"))
                return True
            tried.append("cached → %s" % err)
    except Exception:
        pass
    for name in SERVICE_CANDIDATES:
        url = "%s/%s/FeatureServer/0/query" % (BASE, name)
        attrs, err = _probe_layer(url)
        if attrs is not None and any("import" in k.lower()
                                     for k in attrs):
            LAYER = url
            DATEFIELD = _detect_datefield(attrs)
            RESOLVER_PATH = "candidate:" + name
            _persist_choice(name, 0)
            return True
        tried.append("%s → %s" % (name, err or "no import fields"))
    j, derr = _q(BASE, {"f": "pjson"}, t=30)
    for svc in (j or {}).get("services", []):
        nm = (svc.get("name") or "").split("/")[-1]
        if "port" not in nm.lower():
            continue
        for li in (0, 1, 2):
            url = "%s/%s/FeatureServer/%d/query" % (BASE, nm, li)
            attrs, perr = _probe_layer(url)
            if (attrs is not None
                    and any("import" in k.lower() for k in attrs)
                    and any("date" in k.lower() for k in attrs)):
                LAYER = url
                DATEFIELD = _detect_datefield(attrs)
                RESOLVER_PATH = "directory:%s/%d" % (nm, li)
                _persist_choice(nm, li)
                return True
        tried.append("dir:" + nm)
    gaps.append(("layer resolve failed — tried: "
                 + " | ".join(tried))[:420]
                + (" | dir err: %s" % derr if derr else ""))
    RESOLVER_PATH = "failed"
    return False


def discover_fields(gaps):
    """Probe one record; classify tonnage vs portcall fields by name."""
    j, err = _q(LAYER, {"where": "1=1", "outFields": "*", "resultRecordCount": 1,
                        "orderByFields": DATEFIELD + " DESC", "f": "pjson"})
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
    fmts = [
        ("timestamp", lambda d: "TIMESTAMP '%s'" % d.strftime("%Y-%m-%d %H:%M:%S")),
        ("date", lambda d: "DATE '%s'" % d.strftime("%Y-%m-%d")),
        ("epoch_ms", lambda d: "%d" % int(d.timestamp() * 1000)),
    ]
    errs = []
    for name, f in fmts:
        w = DATEFIELD + " >= " + f(dt_cut)
        j, err = _q(LAYER, {"where": w, "returnCountOnly": "true", "f": "pjson"})
        cnt = (j or {}).get("count")
        if not err and isinstance(cnt, int) and cnt > 0:
            return w, cnt, f
        errs.append("%r → %s" % (w[:40], err or "count=%s" % cnt))
    gaps.append("no where-clause formulation returned rows: " + " | ".join(errs))
    return None, 0, None


def fetch_window(gaps):
    where, expected, fmt = negotiate_where(gaps)
    if not where:
        return [], None
    rows, offset = [], 0
    for _ in range(MAX_PAGES):
        j, err = _q(LAYER, {"where": where, "outFields": "*",
                            "resultOffset": offset, "resultRecordCount": PAGE,
                            "orderByFields": DATEFIELD + " ASC", "f": "pjson"})
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
    return rows, fmt


def seasonal_baseline(fmt, latest_date, fi, fe, gaps, years=(1, 2, 3)):
    """wo4580: same-week-of-year totals via SERVER-SIDE stats (tiny
    responses) for k years back — the honest answer to Chinese-New-Year /
    pre-tariff false signals a naive 28d baseline screams on. Global +
    per-country sums for the matching 7d window, 364-day steps (weekday-
    aligned). Falls back to a declared gap if outStatistics unsupported."""
    if not (fmt and latest_date and fi and fe):
        return None
    stats = json.dumps([
        {"statisticType": "sum", "onStatisticField": fi, "outStatisticFieldName": "imp"},
        {"statisticType": "sum", "onStatisticField": fe, "outStatisticFieldName": "exp"}])
    out = {"windows": [], "by_country": {}}
    for k in years:
        d1 = datetime.combine(latest_date, datetime.min.time(),
                              tzinfo=timezone.utc) - timedelta(days=364 * k)
        d0 = d1 - timedelta(days=RECENT_D)
        where = (DATEFIELD + " >= %s AND " + DATEFIELD
                 + " <= %s") % (fmt(d0), fmt(d1))
        jg, err = _q(LAYER, {"where": where, "outStatistics": stats, "f": "pjson"})
        row = ((jg or {}).get("features") or [{}])[0].get("attributes") or {}
        if err or row.get("imp") is None:
            gaps.append("seasonal window -%dy stats failed: %s" % (k, err or "no attrs"))
            continue
        out["windows"].append({"years_back": k,
                               "window_end": d1.date().isoformat(),
                               "import_t": round(float(row.get("imp") or 0)),
                               "export_t": round(float(row.get("exp") or 0))})
        jc, err2 = _q(LAYER, {"where": where, "outStatistics": stats,
                              "groupByFieldsForStatistics": "country", "f": "pjson"})
        for f2 in ((jc or {}).get("features") or []):
            a2 = f2.get("attributes") or {}
            c2 = a2.get("country")
            if not c2:
                continue
            d3 = out["by_country"].setdefault(c2, [])
            d3.append(float(a2.get("imp") or 0) + float(a2.get("exp") or 0))
    return out if out["windows"] else None


def lambda_handler(event=None, context=None):
    t0 = time.time(); gaps = []
    resolved = resolve_layer(gaps)
    schema = discover_fields(gaps) if resolved else None
    fetch_status = "OK" if schema else "FAILED"
    rows, date_fmt = fetch_window(gaps) if schema else ([], None)
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
        od = _day_ord(a.get(DATEFIELD, a.get("date")))
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

    # v1.3.0 (ops 4619) ragged-edge guard: ports report at different
    # lags, so trailing days with partial coverage undercount global
    # tonnage and manufacture phantom collapse (the "port leg pinned
    # at 0" incident). Trim to the last day carrying >=90% of median
    # port coverage before ANY window math.
    true_latest_date = latest_date
    ragged_trimmed = 0
    cov_med = cov_last = None
    cover = defaultdict(int)
    for p in ports.values():
        for od0 in p["days"]:
            cover[od0] += 1
    if cover and latest_ord:
        counts = sorted(cover.values())
        cov_med = counts[len(counts) // 2]
        complete_ord = max((o for o in cover
                            if cover[o] >= 0.9 * cov_med),
                           default=latest_ord)
        if complete_ord < latest_ord:
            ragged_trimmed = latest_ord - complete_ord
            for p in ports.values():
                for od0 in [d for d in p["days"] if d > complete_ord]:
                    del p["days"][od0]
            latest_ord = complete_ord
            latest_date = datetime.fromordinal(latest_ord).date()
            data_age_days = (datetime.now(timezone.utc).date()
                             - latest_date).days
            gaps.append("ragged edge: trimmed %d trailing day(s) with "
                        "partial coverage" % ragged_trimmed)
        cov_last = cover.get(latest_ord)

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

    # wo4580: seasonal same-week-of-year comparison (global + country)
    seasonal = seasonal_baseline(date_fmt, latest_date, fi, fe, gaps) \
        if latest_ord else None
    seasonal_block = {"status": "UNAVAILABLE"}
    if seasonal and seasonal["windows"]:
        cur_tot = (g_recent[0] + g_recent[1]) * RECENT_D  # daily-rate → window total
        hist = [w["import_t"] + w["export_t"] for w in seasonal["windows"]]
        mean_h = sum(hist) / len(hist) if hist else 0
        seasonal_block = {
            "status": "OK", "n_years": len(hist),
            "current_window_t": round(cur_tot),
            "same_week_prior_years": seasonal["windows"],
            "seasonal_chg_pct": (round((cur_tot / mean_h - 1) * 100, 1)
                                 if mean_h > 0 else None),
            "method": ("current 7d tonnage vs mean of the SAME weekday-aligned "
                       "7d window 1-3 years back (364-day steps, server-side "
                       "sums) — kills Chinese-New-Year / pre-tariff false "
                       "signals the naive 28d baseline screams on")}
        # country-level seasonal joins onto the existing country rows
        for c2 in countries:
            hs = (seasonal["by_country"] or {}).get(c2["country"]) or []
            if hs:
                mh = sum(hs) / len(hs)
                cur_c = (c2["import_tpd_7d"] + c2["export_tpd_7d"]) * RECENT_D
                if mh > 0:
                    c2["seasonal_chg_pct"] = round((cur_c / mh - 1) * 100, 1)

    # wo4580: PortWatch industry-exposure + chokepoint join (fleet data reuse)
    pw = None
    try:
        pw = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/portwatch.json")["Body"].read())
    except Exception:
        gaps.append("portwatch.json unreadable — industry exposure join skipped")
    ind_moves = defaultdict(lambda: {"w": 0.0, "wchg": 0.0, "ports": []})
    chokepoints_ctx = None
    if isinstance(pw, dict):
        exp_by_name = {}
        for p2 in (pw.get("ports") or []):
            nm = (p2.get("name") or "").strip().lower()
            ie = p2.get("industry_exposure")
            if nm and ie:
                exp_by_name[nm] = ie
        for r2 in (accel + decel):
            ie = exp_by_name.get((r2.get("port") or "").strip().lower())
            if not ie:
                continue
            r2["industry_exposure"] = ie if isinstance(ie, list) else [ie]
            wt = r2["import_tpd_base"] + r2["export_tpd_base"]
            for ind in r2["industry_exposure"][:6]:
                d4 = ind_moves[str(ind)]
                d4["w"] += wt
                d4["wchg"] += wt * (r2["total_chg_pct"] or 0)
                d4["ports"].append(r2["port"])
        cps = pw.get("chokepoints")
        if isinstance(cps, list) and cps:
            chokepoints_ctx = {"n": len(cps),
                               "disrupted": pw.get("ports_disrupted") or pw.get("n_disrupted"),
                               "note": ("congestion up while throughput falls = "
                                        "supply-side; both falling = demand-side "
                                        "— different trades")}
    ben_i, suf_i = [], []
    for ind, d4 in ind_moves.items():
        if d4["w"] <= 0 or len(d4["ports"]) < 2:
            continue
        ppv = d4["wchg"] / d4["w"]
        row = measured_row(ind, "industry", ppv, "port_throughput_chg_pct",
                           "tonnage-weighted mean 7d-vs-28d change of %d "
                           "exposed ports (%s)" % (len(d4["ports"]),
                                                   ",".join(d4["ports"][:3])))
        row["n_members"] = len(d4["ports"])
        (ben_i if ppv > 0 else suf_i).append(row)
    shock = (global_pulse.get("total_chg_pct") or 0)
    est_rows, missing = beta_impact(
        sorted({s2 for s2 in ((load_graph() or {}).get("sector_etf_proxy") or {})}),
        "port_throughput_pulse", shock, kind="industry")
    impact = impact_build(
        "port-cargo", "port_throughput_pulse",
        ben_i + [r for r in est_rows if r["pp"] > 0],
        suf_i + [r for r in est_rows if r["pp"] <= 0],
        "Industry rows: measured tonnage-weighted throughput change of ports "
        "carrying that PortWatch industry exposure. Sector rows: estimated "
        "from the accruing beta store (nightly factor history) — absent "
        "until n_obs>=8, honestly.",
        insufficient_rows=missing[:10],
        basis_note="global pulse %+.2f%%; seasonal %s" % (
            shock, seasonal_block.get("status")))

    out = {
        "engine": "port-cargo", "version": "1.3.0",
        "date_field_type": sorted(date_type_seen) or None,
        "engine_class": "physical_trade_fast_layer",
        "evidence_tier": "tier_1_measured_physical",
        "lag_months": -4,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "fetch_status": fetch_status,
        "complete_through": (latest_date.isoformat()
                             if latest_date else None),
        "true_latest_date": (true_latest_date.isoformat()
                             if true_latest_date else None),
        "ragged_days_trimmed": ragged_trimmed,
        "coverage": {"median_ports_per_day": cov_med,
                     "ports_on_last_complete_day": cov_last},
        "layer": {"service_path": RESOLVER_PATH,
                  "datefield": DATEFIELD,
                  "url_tail": LAYER[-80:]},
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
        "seasonal_baseline": seasonal_block,
        "chokepoints_context": chokepoints_ctx,
        "impact_map": impact,
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
