"""justhodl-fleet-auditor v1.0 — the immune system born from the
2026-08-03 why.html forensics session. Sweeps every data/ artifact and
flags the exact bug classes that burned us: STALE (27-day doc),
UNITS (FX P/E 0.9x), COVERAGE (rows capped vs universe), FROZEN
(writer ran, artifact identical), ZERO_SCOPE ($selling 0 everywhere),
NULL_FIELD (schema drift), CONTRA (cross-artifact price divergence),
plus NaN/negative-price/future-timestamp/empty-success.
Writes data/fleet-audit.json (+ history)."""
import json, math, re, time
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
B = "justhodl-dashboard-live"
OUT = "data/fleet-audit.json"
HIST = "data/fleet-audit-history.json"
VERSION = "1.0"
MAX_BODY = 9_000_000
SKIP = ("fleet-audit",)

TS_KEYS = ("generated_at", "as_of", "updated_at", "timestamp", "ts",
           "run_at", "computed_at", "generated", "last_updated")
PRICE_KEYS = ("price", "close", "last", "px", "latest_price")
RULES = [  # (key-regex, lo, hi, class-label)
    (re.compile(r"(^|_)pe(_ttm|_ratio)?$|price_?to_?earnings", re.I),
     3.0, 250.0, "UNITS"),
    (re.compile(r"(^|_)(ps|price_?to_?sales)(_ttm)?$", re.I),
     0.05, 200.0, "UNITS"),
    (re.compile(r"(^|_)(pb|price_?to_?book)(_ttm)?$", re.I),
     0.05, 200.0, "UNITS"),
    (re.compile(r"ev_?(to_?)?ebitda", re.I), 0.5, 300.0, "UNITS"),
    (re.compile(r"yield(_pct)?$", re.I), -5.0, 60.0, "UNITS"),
    (re.compile(r"growth(_pct)?$|_g_pct$|cagr", re.I),
     -95.0, 150.0, "UNITS"),
    (re.compile(r"margin(_pct)?$", re.I), -200.0, 100.0, "UNITS"),
]


def now():
    return datetime.now(timezone.utc)


def parse_ts(v):
    if not v:
        return None
    try:
        v = str(v)
        v = v.replace("Z", "+00:00")
        dt = datetime.fromisoformat(v[:32])
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def walk(o, fn, depth=0, path=""):
    if depth > 6:
        return
    if isinstance(o, dict):
        for k, v in o.items():
            fn(k, v, path + "/" + str(k))
            walk(v, fn, depth + 1, path + "/" + str(k))
    elif isinstance(o, list):
        for i, v in enumerate(o[:400]):
            walk(v, fn, depth + 1, path + "[%d]" % i)


def audit_doc(key, doc, age_h, fn_ages):
    finds = []

    def add(cls, msg):
        if len(finds) < 14:
            finds.append({"cls": cls, "msg": msg[:170]})
    # STALE — vs generic cadence expectation (48h default; history/
    # long files exempt at 14d)
    lim = 336 if any(t in key for t in ("-history", "-long",
                                        "archive")) else 48
    if age_h is not None and age_h > lim:
        add("STALE", "artifact %dh old (limit %dh)" % (age_h, lim))
    # FROZEN — engine deployed/ran after artifact yet artifact older
    eng = key.split("/")[-1].replace(".json", "")
    fa = fn_ages.get("justhodl-" + eng)
    if fa is not None and age_h is not None and age_h - fa > 30:
        add("FROZEN", "engine touched %dh ago but artifact %dh old"
            % (fa, age_h))
    # timestamps in future
    for tk in TS_KEYS:
        t = parse_ts(doc.get(tk)) if isinstance(doc, dict) else None
        if t and (t - now()).total_seconds() > 6 * 3600:
            add("TIME", "%s in the future: %s" % (tk, doc.get(tk)))
    # COVERAGE — rows vs declared universe
    if isinstance(doc, dict):
        rows = None
        for rk in ("rows", "top_setups", "board", "deals", "league",
                   "items", "results"):
            if isinstance(doc.get(rk), list):
                rows = doc[rk]
                break
        for uk in ("universe_n", "n", "n_results", "count",
                   "n_analyzed"):
            u = doc.get(uk)
            if rows is not None and isinstance(u, int) and u > 0 \
                    and abs(len(rows) - u) > max(3, 0.02 * u) \
                    and len(rows) < u:
                add("COVERAGE", "%s=%d but %d rows shipped"
                    % (uk, u, len(rows)))
                break
        # duplicate tickers in rows
        if rows and isinstance(rows[0], dict) \
                and "ticker" in rows[0]:
            seen, dups = set(), 0
            for x in rows:
                t = x.get("ticker")
                if t in seen:
                    dups += 1
                seen.add(t)
            if dups > 2:
                add("COVERAGE", "%d duplicate tickers" % dups)
    # numeric plausibility + NaN + zero-scope + null-field
    stats = {}
    nan_hits = [0]
    neg_price = [0]

    def probe(k, v, path):
        if isinstance(v, float) and (math.isnan(v)
                                     or math.isinf(v)):
            nan_hits[0] += 1
            return
        if isinstance(v, (int, float)):
            lk = str(k).lower()
            if lk in PRICE_KEYS and v is not None and v < 0:
                neg_price[0] += 1
            st = stats.setdefault(k, [0, 0, 0, None])
            st[0] += 1
            if v == 0:
                st[1] += 1
            for rx, lo, hi, cls in RULES:
                if rx.search(str(k)):
                    if not (lo <= v <= hi) and v != 0:
                        st[2] += 1
                        st[3] = (cls, lo, hi, v)
                    break
        elif v is None:
            st = stats.setdefault(k, [0, 0, 0, None])
            st[0] += 1
    walk(doc, probe)
    if nan_hits[0]:
        add("NAN", "%d NaN/Inf values" % nan_hits[0])
    if neg_price[0]:
        add("UNITS", "%d negative prices" % neg_price[0])
    for k, (n, zeros, oob, last) in stats.items():
        if n >= 12 and oob > 0.6 * n and last:
            cls, lo, hi, v = last
            add(cls, "%s: %d/%d outside [%s,%s], e.g. %s"
                % (k, oob, n, lo, hi, v))
        if n >= 15 and zeros == n and re.search(
                r"sell|sold|out|short", str(k), re.I):
            add("ZERO_SCOPE",
                "%s is 0 across all %d rows — scope smell" % (k, n))
        if n >= 15 and zeros == 0 and stats.get(k, [0])[0] == n:
            pass
    # NULL_FIELD: fields null across all rows of a fresh doc
    if age_h is not None and age_h < 48 and isinstance(doc, dict):
        rows = doc.get("rows") if isinstance(doc.get("rows"),
                                             list) else None
        if rows and len(rows) >= 15 and isinstance(rows[0], dict):
            for k in list(rows[0])[:40]:
                vals = [r.get(k) for r in rows[:200]]
                if all(v is None for v in vals):
                    add("NULL_FIELD",
                        "rows.%s null in all %d rows (fresh doc)"
                        % (k, len(vals)))
    return finds


def collect_prices(key, doc, book):
    def probe(k, v, path):
        if str(k).lower() in ("ticker", "symbol"):
            return
    if isinstance(doc, dict):
        rows = doc.get("rows") or doc.get("top_setups") or []
        if isinstance(rows, list):
            for r in rows[:800]:
                if not isinstance(r, dict):
                    continue
                t = str(r.get("ticker") or r.get("symbol")
                        or "").upper()
                if not t:
                    continue
                for pk in PRICE_KEYS:
                    v = r.get(pk)
                    if isinstance(v, (int, float)) and v > 0:
                        book.setdefault(t, []).append(
                            (key, pk, float(v)))
                        break


def lambda_handler(event, context):
    t0 = time.time()
    keys = []
    tok = None
    while True:
        kw = {"Bucket": B, "Prefix": "data/", "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        keys += [(o["Key"], o["LastModified"], o["Size"])
                 for o in r.get("Contents", [])
                 if o["Key"].endswith(".json")]
        tok = r.get("NextContinuationToken")
        if not tok:
            break
    fn_ages = {}
    try:
        p = lam.get_paginator("list_functions")
        for pg in p.paginate():
            for f in pg["Functions"]:
                nm = f["FunctionName"]
                if nm.startswith("justhodl-"):
                    lm = datetime.strptime(
                        f["LastModified"].split(".")[0],
                        "%Y-%m-%dT%H:%M:%S").replace(
                        tzinfo=timezone.utc)
                    fn_ages[nm] = int(
                        (now() - lm).total_seconds() // 3600)
    except Exception as e:
        print("fn_ages: %s" % e)
    results = []
    price_book = {}
    n_parse_fail = 0
    for key, lm, size in keys:
        if any(sk in key for sk in SKIP):
            continue
        age_h = int((now() - lm).total_seconds() // 3600)
        if size > MAX_BODY:
            results.append({"key": key, "age_h": age_h,
                            "size": size, "status": "SKIPPED_BIG",
                            "findings": []})
            continue
        try:
            doc = json.loads(s3.get_object(
                Bucket=B, Key=key)["Body"].read())
        except Exception as e:
            n_parse_fail += 1
            results.append({"key": key, "age_h": age_h,
                            "size": size, "status": "PARSE_FAIL",
                            "findings": [{"cls": "PARSE",
                                          "msg": str(e)[:120]}]})
            continue
        finds = audit_doc(key, doc, age_h, fn_ages)
        collect_prices(key, doc, price_book)
        st = ("FAIL" if any(f["cls"] in
                            ("UNITS", "PARSE", "NAN", "FROZEN")
                            for f in finds)
              else "WARN" if finds else "OK")
        results.append({"key": key, "age_h": age_h, "size": size,
                        "status": st, "findings": finds})
    # cross-artifact price contradictions (same ticker, >2 sources)
    contra = []
    for t, obs in price_book.items():
        vals = sorted(v for _, _, v in obs)
        if len(vals) >= 2 and vals[0] > 0:
            spread = (vals[-1] / vals[0] - 1) * 100
            if spread > 12:  # generous: EODs differ by a day
                contra.append({"ticker": t,
                               "spread_pct": round(spread, 1),
                               "obs": [(k.split("/")[-1], pk,
                                        round(v, 2))
                                       for k, pk, v in obs[:5]]})
    contra.sort(key=lambda x: -x["spread_pct"])
    by_cls = {}
    for r in results:
        for f in r["findings"]:
            by_cls[f["cls"]] = by_cls.get(f["cls"], 0) + 1
    fails = [r for r in results if r["status"] == "FAIL"]
    warns = [r for r in results if r["status"] == "WARN"]
    out = {"engine": "justhodl-fleet-auditor", "version": VERSION,
           "generated_at": now().isoformat(),
           "elapsed_s": round(time.time() - t0, 1),
           "n_scanned": len(results),
           "n_ok": sum(1 for r in results if r["status"] == "OK"),
           "n_warn": len(warns), "n_fail": len(fails),
           "n_parse_fail": n_parse_fail,
           "by_class": by_cls,
           "contradictions": contra[:25],
           "offenders": sorted(
               (r for r in results if r["findings"]),
               key=lambda r: (-len([f for f in r["findings"]
                                    if f["cls"] != "STALE"]),
                              -len(r["findings"])))[:120],
           "results_index": [
               {"key": r["key"], "status": r["status"],
                "age_h": r["age_h"], "n": len(r["findings"])}
               for r in results]}
    s3.put_object(Bucket=B, Key=OUT,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    try:
        h = json.loads(s3.get_object(Bucket=B,
                                     Key=HIST)["Body"].read())
    except Exception:
        h = {"days": []}
    h["days"] = (h.get("days") or [])[-60:] + [{
        "d": now().strftime("%Y-%m-%d %H:%M"),
        "scanned": len(results), "fail": len(fails),
        "warn": len(warns), "by_class": by_cls}]
    s3.put_object(Bucket=B, Key=HIST,
                  Body=json.dumps(h).encode(),
                  ContentType="application/json")
    print(json.dumps({"ok": True, "scanned": len(results),
                      "fail": len(fails), "warn": len(warns),
                      "by_class": by_cls}))
    return {"ok": True, "n": len(results), "fail": len(fails)}
