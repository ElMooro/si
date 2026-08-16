"""justhodl-repo — THE repo master engine (ops 4763).

Khalid's spec: one engine named repo feeding one page named repo, with
EVERY repo-related series the system holds -- the master inventory's
300 (TRI/TRIV1/DVP/GCF, FNYR rates+underlying volumes, PD fails +
repo/reverse-repo financing) -- PLUS the FRED trade-weighted dollar
complex, each shown with day/week/month/quarter/year percentage
changes, click-through full-history charts, and a combined barometer.

Design:
  - inputs are the BANK, never the wire: series bodies come from the
    warm docs the convergence engines maintain (data/warm/ofr/series/*,
    data/providers/fred-scoped/*), and the scope comes from
    data/repo-master-inventory.json (ops 4761) so anything the
    inventory gains later flows in automatically
  - shape-robust pair extraction (OFR payloads and FRED docs differ);
    the longest [date,value] sequence found anywhere in the doc wins
  - changes: pop = vs previous observation (the honest 'day-to-day'
    for daily series, period-over-period for weekly), plus calendar
    lookbacks 7/30/91/365d against the series' own history
  - outputs: data/repo.json (hot index: groups, 5-change grid,
    barometer) + data/repo-history/{id}.json per series (lean
    dates/values arrays the page lazy-loads for full-history charts)
  - barometer: labeled heuristic, fully transparent -- every component
    (venue spreads, collateral spread, fails momentum, overnight-share
    proxy, dollar impulse) ships with its value, sign, and clipped
    contribution; score = 50 + 10*mean(clipped z), 0..100
Explicit failures per series; a missing doc becomes a skipped row with
a reason, never a fabricated number."""
import gzip
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")

DOLLAR_IDS = ["DTWEXBGS", "RTWEXBGS", "DTWEXAFEGS", "DTWEXEMEGS",
               "DTWEXM", "DTWEXB", "TWEXBGSMTH", "TWEXAFEGSMTH",
               "TWEXEMEGSMTH"]
FRED_META_KEY = "data/_state/fred-series-meta.json"
FRED_WARM_TPL = "data/warm/fred-scoped/{cat}/{id}.json"
DATE_KEYS = ("date", "d", "asofdate", "as_of_date", "observation_date",
              "DATE", "period")
VAL_KEYS = ("value", "v", "val", "rate", "close", "VALUE", "obs_value")


def sread(key):
    raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def _num(x):
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        xs = str(x).replace(",", "").strip()
        if xs in ("", ".", "null", "None", "NA", "NaN"):
            return None
        return float(xs)
    except Exception:
        return None


def _isdate(sv):
    return (isinstance(sv, str) and len(sv) >= 10 and sv[:4].isdigit()
             and sv[4] == "-" and sv[7] == "-")


def extract_pairs(obj):
    """Longest [ISO-date, number] sequence anywhere in the doc."""
    best = []

    def walk(o, depth=0):
        nonlocal best
        if depth > 8:
            return
        if isinstance(o, list):
            pairs = []
            for row in o:
                if isinstance(row, (list, tuple)) and len(row) >= 2 \
                        and _isdate(row[0]):
                    v = _num(row[1])
                    if v is not None:
                        pairs.append((row[0][:10], v))
                elif isinstance(row, dict):
                    dv = next((row[k] for k in DATE_KEYS
                                if _isdate(row.get(k))), None)
                    vv = next((_num(row[k]) for k in VAL_KEYS
                                if _num(row.get(k)) is not None), None)
                    if dv is not None and vv is not None:
                        pairs.append((dv[:10], vv))
            if len(pairs) > len(best):
                best = pairs
            for row in o[:50]:
                walk(row, depth + 1)
        elif isinstance(o, dict):
            for v in o.values():
                walk(v, depth + 1)

    walk(obj)
    dd = {}
    for d, v in best:
        dd[d] = v
    return sorted(dd.items())


def pct(last, base):
    if base is None or last is None or base == 0:
        return None
    return round((last / base - 1.0) * 100.0, 3)


def changes(pairs):
    if not pairs:
        return None, None, {}
    dates = [p[0] for p in pairs]
    vals = [p[1] for p in pairs]
    last_d, last_v = dates[-1], vals[-1]
    out = {"pop": pct(last_v, vals[-2]) if len(vals) > 1 else None}
    import bisect
    ld = datetime.strptime(last_d, "%Y-%m-%d")
    for tag, days in (("w", 7), ("m", 30), ("q", 91), ("y", 365)):
        target = (ld - timedelta(days=days)).strftime("%Y-%m-%d")
        i = bisect.bisect_right(dates, target) - 1
        out[tag] = pct(last_v, vals[i]) if i >= 0 else None
    return last_d, last_v, out


def safe_id(m):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", m)


def group_of(e):
    m = e["mnemonic"]
    if m.startswith("REPO-TRIV1"):
        return "Tri-party (TRIV1, ex-Fed)"
    if m.startswith("REPO-TRI_"):
        return "Tri-party (TRI legacy)"
    if m.startswith("REPO-DVP"):
        return "DVP bilateral cleared"
    if m.startswith("REPO-GCF"):
        return "GCF"
    if m.startswith("FNYR"):
        return "Reference rates & volumes"
    if "AFtD" in m or "AFtR" in m:
        return "Primary dealer FAILS"
    if m.startswith("NYPD"):
        return "Primary dealer financing"
    if e.get("family") == "DOLLAR":
        return "Dollar (FRED trade-weighted)"
    return "Other"


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    inv = sread("data/repo-master-inventory.json")
    scope = list(inv.get("series") or [])

    # add the dollar complex (v1.2): warm-first at
    # data/warm/fred-scoped/Dollar_TradeWeighted/{ID}.json; if a series
    # was outside the scoped FRED import, self-heal ONCE from the FRED
    # API (key inherited via deploy inherit_env) and bank it to warm
    # permanently -- thereafter it reads from the bank like everything.
    import urllib.request
    dollar_miss = []
    fred_key = os.environ.get("FRED_API_KEY", "")
    DOLLAR_DIR = "data/warm/fred-scoped/Dollar_TradeWeighted"
    for fid in DOLLAR_IDS:
        wkey = f"{DOLLAR_DIR}/{fid}.json"
        ok = False
        try:
            s3.head_object(Bucket=BUCKET, Key=wkey)
            ok = True
        except Exception:
            if not fred_key:
                dollar_miss.append({"id": fid, "reason": "no_api_key"})
                continue
            try:
                u = ("https://api.stlouisfed.org/fred/series/observations"
                     f"?series_id={fid}&api_key={fred_key}"
                     "&file_type=json&observation_start=1970-01-01")
                req = urllib.request.Request(u, headers={
                    "User-Agent": "JustHodl research raafouis@gmail.com"})
                with urllib.request.urlopen(req, timeout=60) as r:
                    doc = json.loads(r.read())
                obs = doc.get("observations") or []
                if not obs:
                    dollar_miss.append({"id": fid,
                                         "reason": "fred_zero_obs"})
                    continue
                s3.put_object(Bucket=BUCKET, Key=wkey,
                               Body=json.dumps(
                                   {"id": fid,
                                    "source": "FRED API via justhodl-repo",
                                    "banked_at": now,
                                    "observations": obs},
                                   separators=(",", ":")).encode(),
                               ContentType="application/json")
                ok = True
            except Exception as ex:
                dollar_miss.append({"id": fid,
                                     "reason": f"{type(ex).__name__}: "
                                               f"{str(ex)[:60]}"})
        if ok:
            scope.append({"mnemonic": fid, "name": f"FRED {fid}",
                           "family": "DOLLAR", "s3_key": wkey,
                           "api_url": ("https://fred.stlouisfed.org/series/"
                                        + fid),
                           "tier": 1 if fid == "DTWEXBGS" else 2})

    values = {}
    rows = []
    skipped = []
    for e in scope:
        if (time.time() - t0) > 780:
            skipped.append({"id": e["mnemonic"], "reason": "time_cap"})
            continue
        sid = safe_id(e["mnemonic"])
        try:
            doc = sread(e["s3_key"])
            pairs = extract_pairs(doc)
            if not pairs:
                skipped.append({"id": e["mnemonic"],
                                 "reason": "no_pairs_extracted"})
                continue
            last_d, last_v, chg = changes(pairs)
            values[e["mnemonic"]] = (last_d, last_v, pairs)
            s3.put_object(
                Bucket=BUCKET, Key=f"data/repo-history/{sid}.json",
                Body=json.dumps({"id": e["mnemonic"],
                                  "label": e.get("name") or e["mnemonic"],
                                  "dates": [p[0] for p in pairs],
                                  "values": [p[1] for p in pairs]},
                                 separators=(",", ":")).encode(),
                ContentType="application/json",
                CacheControl="public, max-age=1800")
            rows.append({"id": e["mnemonic"], "sid": sid,
                          "label": e.get("name") or "",
                          "group": group_of(e),
                          "tier": e.get("tier", 2),
                          "n_obs": len(pairs),
                          "first": pairs[0][0], "last": last_d,
                          "last_value": last_v, "chg": chg})
        except Exception as ex:
            skipped.append({"id": e["mnemonic"],
                             "reason": f"{type(ex).__name__}: {str(ex)[:60]}"})

    # ── barometer: labeled, transparent heuristic ────────────────────
    def latest(m):
        return values.get(m, (None, None, None))[1]

    def chg_of(m, tag):
        for r in rows:
            if r["id"] == m:
                return (r["chg"] or {}).get(tag)
        return None

    comps = []

    def add(name, raw, unit, z, note):
        if raw is None or z is None:
            comps.append({"name": name, "value": None, "unit": unit,
                           "z": None, "note": note + " (unavailable)"})
            return
        comps.append({"name": name, "value": round(raw, 3), "unit": unit,
                       "z": round(max(-3.0, min(3.0, z)), 2), "note": note})

    sofr, tgcr = latest("FNYR-SOFR-A"), latest("FNYR-TGCR-A")
    sp = (sofr - tgcr) * 100 if (sofr is not None and tgcr is not None) \
        else None
    add("SOFR−TGCR spread", sp, "bps",
         (sp / 3.0) if sp is not None else None,
         "bilateral-vs-triparty GC premium; wide = collateral pressure")
    dvp, tri = latest("REPO-DVP_AR_TOT-P"), latest("REPO-TRIV1_AR_TOT-P")
    sp2 = (dvp - tri) * 100 if (dvp is not None and tri is not None) else None
    add("DVP−Triparty rate", sp2, "bps",
         (sp2 / 4.0) if sp2 is not None else None,
         "cleared-bilateral premium over triparty")
    cord, tt = latest("REPO-TRIV1_AR_CORD-P"), latest("REPO-TRIV1_AR_T-P")
    sp3 = (cord - tt) * 100 if (cord is not None and tt is not None) else None
    add("Corp−Tsy collateral spread", sp3, "bps",
         ((sp3 - 8.0) / 6.0) if sp3 is not None else None,
         "collateral risk premium inside triparty")
    fm = chg_of("NYPD-PD_AFtD_TOT-A", "m")
    add("Total fails Δ30d", fm, "%",
         (fm / 40.0) if fm is not None else None,
         "settlement stress momentum")
    ftm = chg_of("NYPD-PD_AFtD_T-A", "m")
    add("Treasury fails Δ30d", ftm, "%",
         (ftm / 40.0) if ftm is not None else None,
         "UST-specific settlement stress")
    vm = chg_of("REPO-TRIV1_TV_OO-P", "m")
    add("Overnight triparty volume Δ30d", vm, "%",
         (-vm / 8.0) if vm is not None else None,
         "falling ON volume = funding withdrawal (sign inverted)")
    dy = chg_of("DTWEXBGS", "m")
    add("Broad dollar Δ30d", dy, "%",
         (dy / 2.0) if dy is not None else None,
         "dollar impulse tightens global funding")
    zs = [c["z"] for c in comps if c["z"] is not None]
    score = round(50 + 10 * (sum(zs) / len(zs)), 1) if zs else None
    label = (None if score is None else
              "CALM" if score < 45 else
              "NORMAL" if score < 55 else
              "TIGHTENING" if score < 65 else "STRESS")

    rows.sort(key=lambda r: (r["tier"], r["group"], r["id"]))
    groups = {}
    for r in rows:
        groups.setdefault(r["group"], []).append(r)
    out = {"as_of": now,
            "note": ("barometer is a labeled heuristic: score = 50 + "
                     "10*mean(clipped z), components listed in full"),
            "counts": {"series": len(rows), "skipped": len(skipped),
                        "history_files": len(rows)},
            "barometer": {"score": score, "label": label,
                           "components": comps},
            "groups": [{"name": g, "series": groups[g]}
                        for g in sorted(groups)],
            "skipped": skipped[:40], "dollar_missing": dollar_miss}
    s3.put_object(Bucket=BUCKET, Key="data/repo.json",
                   Body=json.dumps(out, separators=(",", ":")).encode(),
                   ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "v": "1.2", "series": len(rows), "skipped": len(skipped),
            "barometer": score, "label": label,
            "secs": round(time.time() - t0, 1)}
    print("[repo] " + json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
