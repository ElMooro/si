"""
justhodl-data-census v1.0 — ONE catalog of every value the fleet publishes.

Khalid (2026-07-27): "put all the data we have under one engine all
displaying in one page so that we don't repeat the same mistake, we don't
rebuild, we just fill gaps, and we can compare them against real data to
detect bugs. my system has tons of engines and tons of data — you just have
to discover them all."

The JPEXPYY bug motivates the design: Japan's export symbol published
Korea's number for weeks because no layer compares VALUES across engines.
feed-registry (freshness) and feed-catalog (schemas/writers/pages) already
inventory the fleet — this engine CONSUMES both and adds the missing layer:

  1. WALK every data/*.json to its scalar leaves (bounded) → one path index.
  2. MISLABEL DETECTOR: the same distinctive numeric value appearing in 2+
     artifacts under paths whose COUNTRY tokens differ (JPEXPYY = 47.96 =
     korea_exports.yoy_pct would have lit up here).
  3. CONFLICT DETECTOR: same country+measure tokens, materially different
     values → which engine is right?
  4. GAP FILLER: dead vault symbols matched to candidate fleet paths by
     country+measure tokens — candidates for a human/ops decision, never
     auto-wired (the same-measure/same-country bar stays manual).

EMITS data/data-census.json (compact) + data-census/paths-ledger.json (full)
"""
import json
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

import boto3

MARKER = "data-census v1.7 ops3986 vault-full"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/data-census.json"
LEDGER_KEY = "data-census/paths-ledger.json"
s3 = boto3.client("s3")

MAX_ARTIFACT_MB = 4   # >4MB = ledgers/brain dumps: few scalars, seconds of parse each
MAX_PATHS_PER = 400
MAX_DEPTH = 5
TRIVIAL = {0.0, 1.0, -1.0, 100.0, 50.0, 2.0, 10.0}

COUNTRY = {"us": "US", "usa": "US", "united_states": "US", "korea": "KR", "kr": "KR",
           "korean": "KR", "japan": "JP", "jp": "JP", "jpn": "JP", "china": "CN",
           "cn": "CN", "chn": "CN", "taiwan": "TW", "tw": "TW", "euro": "EU",
           "eu": "EU", "ea": "EU", "germany": "DE", "de": "DE", "deu": "DE",
           "uk": "GB", "gb": "GB", "gbr": "GB", "france": "FR", "fr": "FR",
           "italy": "IT", "it": "IT", "spain": "ES", "es": "ES", "swiss": "CH",
           "ch": "CH", "che": "CH", "norway": "NO", "no": "NO", "chile": "CL",
           "cl": "CL", "peru": "PE", "pe": "PE", "india": "IN", "brazil": "BR",
           "canada": "CA", "australia": "AU", "mexico": "MX", "global": "WW",
           "world": "WW"}
MEASURE = {"export": "exports", "exports": "exports", "import": "imports",
           "cpi": "cpi", "inflation": "cpi", "ppi": "ppi", "gdp": "gdp",
           "pmi": "pmi", "yield": "yield", "rate": "rate", "policy": "rate",
           "m0": "m0", "m1": "m1", "m2": "m2", "m3": "m3", "tsf": "tsf",
           "credit": "credit", "impulse": "credit", "unemployment": "unemp",
           "orders": "orders", "yoy": "yoy", "reserves": "reserves",
           "fx": "fx", "spread": "spread", "cli": "cli", "sentiment": "sent",
           "tankan": "tankan", "loans": "loans", "lending": "loans"}
TOK = re.compile(r"[a-z0-9]+")


def toks(path):
    ts = set(TOK.findall(path.lower()))
    return ({COUNTRY[t] for t in ts if t in COUNTRY},
            {MEASURE[t] for t in ts if t in MEASURE})


ID_FIELDS = ("symbol", "ticker", "id", "code", "country", "name", "key", "label")
NAME_SIB = ("label", "name", "title", "description")
SRC_SIB = ("source", "src", "provider", "resolved_via", "via", "feed")
MAX_LIST_SAMPLE = 40


def walk(o, pre="", depth=0, out=None, cap=None):
    """v1.2: lists of keyed records are walked up to MAX_LIST_SAMPLE deep,
    labelled by their identifier (symbols[TWEXPYY].value) instead of [0] —
    v1.0 sampled only element [0], which is why 561 vault symbols yielded
    one path and every detector read zero. Numeric leaves also capture the
    provenance SIBLINGS Khalid asked to see: name, upstream source, status.
    """
    if out is None:
        out = []
    if cap is None:
        cap = MAX_PATHS_PER
    if len(out) >= cap or depth > MAX_DEPTH:
        return out
    if isinstance(o, dict):
        sib_name = next((str(o[k])[:70] for k in NAME_SIB
                         if isinstance(o.get(k), str)), None)
        sib_src = next((str(o[k])[:60] for k in SRC_SIB
                        if isinstance(o.get(k), str)), None)
        sib_st = o.get("status") if isinstance(o.get("status"), str) else None
        for k, v in o.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                out.append({"p": (f"{pre}.{k}" if pre else str(k)),
                            "v": float(v), "name": sib_name,
                            "src": sib_src, "st": sib_st})
                if len(out) >= cap:
                    return out
            else:
                walk(v, f"{pre}.{k}" if pre else str(k), depth + 1, out, cap)
    elif isinstance(o, list) and o:
        if isinstance(o[0], dict):
            idf = next((f for f in ID_FIELDS if isinstance(o[0].get(f), str)), None)
            if idf:
                for el in o[:MAX_LIST_SAMPLE]:
                    tag = str(el.get(idf, "?"))[:24] if isinstance(el, dict) else "?"
                    walk(el, f"{pre}[{tag}]", depth + 1, out, cap)
                    if len(out) >= MAX_PATHS_PER:
                        return out
                return out
        walk(o[0], f"{pre}[0]", depth + 1, out, cap)
    elif isinstance(o, (int, float)) and not isinstance(o, bool):
        out.append({"p": pre, "v": float(o), "name": None, "src": None, "st": None})
    return out


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    print(f"[census] {MARKER}")

    # ── consume the EXISTING registries (wire, don't rebuild) ────────────
    def gj(key, default=None):
        try:
            return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        except Exception:
            return default

    freg = gj("data/feed-registry.json", {}) or {}
    fcat = gj("data/feed-catalog.json", {}) or {}
    vault = gj("data/tradingview.json", {}) or {}
    stale_set = {f.get("key") for f in (freg.get("stale") or []) if isinstance(f, dict)}
    writers = {}
    for it in (fcat.get("feeds") or fcat.get("catalog") or []):
        if isinstance(it, dict) and it.get("key"):
            writers[it["key"]] = {"writer": it.get("writer") or it.get("writers"),
                                  "pages": it.get("pages") or it.get("consumers")}

    # ── list + walk every artifact ───────────────────────────────────────
    keys, tok = [], None
    while True:
        kw = dict(Bucket=BUCKET, Prefix="data/", MaxKeys=1000)
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents") or []:
            if o["Key"].endswith(".json"):
                keys.append((o["Key"], o["Size"], o["LastModified"]))
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")

    arts, paths, errors = [], [], []
    val_index = defaultdict(list)          # rounded value -> [(key, path)]
    # v1.6 determinism: the v1.5 guard protected only the WALK — it ran
    # until 90s remained, then detectors + indexes + the ledger dump over
    # millions of paths blew through the reserve and the run timed out with
    # nothing written. Three rules now guarantee completion:
    #   1. PRIORITY artifacts walk first (vault leads → FRED/US10Y always
    #      present even on a truncated run);
    #   2. hard global cap MAX_TOTAL_PATHS;
    #   3. the walk surrenders with 240s still on the clock — the reserve
    #      belongs to the post-walk pipeline, not the walk.
    PRIORITY = ("data/tradingview.json", "data/asia-leads.json",
                "data/boj-detail.json", "data/risk-gate.json",
                "data/rotation-dashboard.json", "data/domain-barometers.json",
                "data/china-liquidity.json", "data/global-business-cycle.json")
    keys.sort(key=lambda k: (k[0] not in PRIORITY, k[0]))
    MAX_TOTAL_PATHS = 250_000
    truncated = 0
    for key, size, lm in keys:
        low_time = False
        try:
            low_time = bool(context) and \
                context.get_remaining_time_in_millis() < 240_000
        except Exception:
            low_time = time.time() - t0 > 560
        if low_time or len(paths) >= MAX_TOTAL_PATHS:
            truncated += 1
            continue
        age_h = (now - lm).total_seconds() / 3600
        rec = {"key": key, "size": size, "age_h": round(age_h, 1),
               "stale": key in stale_set, **(writers.get(key) or {})}
        if size > MAX_ARTIFACT_MB * 1_000_000:
            rec["skipped"] = f">{MAX_ARTIFACT_MB}MB"
            arts.append(rec)
            continue
        try:
            doc = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            # v1.7: the 500-path cap starved the vault (561 symbols x ~10
            # fields) — the single richest source-attributed artifact walked
            # only its first slice, which is why FRED counted <40 on v1.6.
            pl = walk(doc, cap=(6000 if key in PRIORITY else MAX_PATHS_PER))
            rec["n_paths"] = len(pl)
            gen = doc.get("generated_at") if isinstance(doc, dict) else None
            rec["generated_at"] = gen
            if isinstance(doc, dict):
                ups = []
                for f in ("source", "sources", "method", "provider"):
                    x = doc.get(f)
                    if isinstance(x, str):
                        ups.append(x[:60])
                    elif isinstance(x, list):
                        ups += [str(i)[:60] for i in x[:3] if isinstance(i, str)]
                    elif isinstance(x, dict):
                        ups += [str(v)[:60] for v in list(x.values())[:3]
                                if isinstance(v, str)]
                if ups:
                    rec["upstream"] = ups[:4]
            live_art = age_h <= 48
            for r0 in pl:
                r0["k"] = key
                r0["live"] = (r0.get("st") == "LIVE") if r0.get("st") \
                    else (None if r0.get("st") else live_art)
                paths.append(r0)
                rv = round(r0["v"], 4)
                if rv not in TRIVIAL and abs(rv) > 0.01:
                    val_index[rv].append((key, r0["p"]))
        except Exception as e:
            rec["error"] = f"{type(e).__name__}"
            errors.append(key)
        arts.append(rec)

    # ── detector 1: MISLABELS (same value, different country tokens) ─────
    mislabels = []
    for rv, hits in val_index.items():
        if len(hits) < 2 or len(hits) > 12:
            continue
        cs = [(k, p, *toks(k + "." + p)) for k, p in hits]
        countries = {c for _k, _p, cset, _m in cs for c in cset}
        if len(countries) >= 2:
            mislabels.append({
                "value": rv, "n_occurrences": len(hits),
                "countries_claimed": sorted(countries),
                "paths": [{"artifact": k, "path": p, "countries": sorted(cc),
                           "measures": sorted(mm)} for k, p, cc, mm in cs][:8],
                "read": "one number published under multiple country labels — "
                        "verify which is real (JPEXPYY class)"})
    mislabels.sort(key=lambda m: -m["n_occurrences"])

    # ── detector 2: CONFLICTS (same country+measure, different values) ───
    grp = defaultdict(list)
    for r in paths:
        cset, mset = toks(r["k"] + "." + r["p"])
        if len(cset) == 1 and mset:
            grp[(next(iter(cset)), tuple(sorted(mset)))].append(r)
    conflicts = []
    for (c, ms), rs in grp.items():
        by_art = {}
        for r in rs:
            by_art.setdefault(r["k"], r)
        if len(by_art) < 2:
            continue
        vals = [r["v"] for r in by_art.values()]
        lo, hi = min(vals), max(vals)
        if hi - lo > max(0.5, abs(lo) * 0.15):
            conflicts.append({
                "country": c, "measures": list(ms),
                "spread": round(hi - lo, 3), "n_engines": len(by_art),
                "values": [{"artifact": r["k"], "path": r["p"],
                            "value": r["v"]} for r in list(by_art.values())[:6]],
                "read": "engines disagree on the same measure — different "
                        "vintages/definitions, or one is wrong"})
    conflicts.sort(key=lambda c: -c["n_engines"])

    # ── detector 3: GAP FILL candidates for dead vault symbols ───────────
    SYM_RX = [(re.compile(r"^(US|JP|CN|TW|KR|EU|DE|GB|FR|IT|ES|CH|NO|CL|PE)"), 0)]
    MEAS_SYM = {"EXPYY": {"exports", "yoy"}, "IRYY": {"cpi", "yoy"},
                "PPIYY": {"ppi", "yoy"}, "GDPYY": {"gdp", "yoy"},
                "INTR": {"rate"}, "FER": {"reserves", "fx"}, "M2": {"m2"},
                "M1": {"m1"}, "M0": {"m0"}, "CLI": {"cli"}, "TOT": {"exports"}}
    dead = [r for r in (vault.get("symbols") or []) if r.get("status") != "LIVE"]
    gaps = []
    for d in dead:
        sym = d["symbol"]
        m = SYM_RX[0][0].match(sym)
        if not m:
            continue
        cc = m.group(1)
        want = None
        for suf, ms in MEAS_SYM.items():
            if sym[len(cc):].startswith(suf) or sym.endswith(suf):
                want = ms
                break
        if not want:
            continue
        cands = []
        for r in paths:
            cset, mset = toks(r["k"] + "." + r["p"])
            if cc in cset and want <= mset:
                cands.append({"artifact": r["k"], "path": r["p"], "value": r["v"]})
        if cands:
            gaps.append({"symbol": sym, "n_notes": d.get("n_notes"),
                         "needs": {"country": cc, "measures": sorted(want)},
                         "candidates": cands[:5],
                         "rule": "wire ONLY if same measure AND same country — "
                                 "the bar JPEXPYY failed"})
    gaps.sort(key=lambda g: -(g["n_notes"] or 0))

    def eng_of(key):
        w = (writers.get(key) or {}).get("writer")
        if isinstance(w, list):
            w = w[0] if w else None
        if not w:
            stem = key.split("/")[-1].replace(".json", "")
            w = f"justhodl-{stem}"
        return w

    # ── by_source: "FRED: US10Y = 4.25 — pulled from fred:DGS10 through
    # justhodl-tradingview". Khalid 2026-07-27: categorise by data source so
    # anything is locatable in seconds. Family is parsed from the src sibling
    # (and the path/artifact as fallback), never guessed from the value. ──
    FAMS = (("FRED", ("fred",)), ("ECB", ("ecb",)),
            ("BOJ", ("boj", "stat-search")), ("MOF-JAPAN", ("mof", "jgbcme")),
            ("YAHOO", ("yahoo", "^")), ("MOEA-TAIWAN", ("moea", "gov.tw", "getpointdata")),
            ("BCRP-PERU", ("bcrp",)), ("NORGES", ("norges",)),
            ("BOE", ("boe", "iadb")), ("SNB", ("snb",)), ("IMF", ("imf",)),
            ("EUROSTAT", ("eurostat", "ec.europa")),
            ("US-TREASURY", ("treasury", "ust:")), ("PBOC", ("pboc",)),
            ("POLYGON", ("polygon", "poly:")), ("FMP", ("fmp",)),
            ("CFTC", ("cftc",)), ("COINMETRICS", ("coinmetrics",)),
            ("SEC-EDGAR", ("edgar", "sec.gov")), ("GNEWS", ("gnews",)),
            ("FLEET-INTERNAL", ("fleet:", "data/")))

    def src_family(src_str):
        t = (src_str or "").lower()
        if not t:
            return None
        for fam, keys in FAMS:
            if any(k in t for k in keys):
                return fam
        return "OTHER"

    TAGRX = re.compile(r"\[([A-Za-z0-9_\.\-\^\!]{1,24})\]")

    def disp_name(r):
        if r.get("name"):
            return r["name"]
        m = TAGRX.search(r["p"])
        if m and m.group(1) != "0":
            return m.group(1)
        return r["p"].split(".")[-1]

    by_source = {}
    seen_sn = set()
    for r in paths:
        fam = src_family(r.get("src"))
        if not fam:
            continue
        nm = disp_name(r)
        if (fam, nm) in seen_sn:
            continue
        seen_sn.add((fam, nm))
        b = by_source.setdefault(fam, {"n": 0, "engines": set(), "metrics": []})
        b["n"] += 1
        b["engines"].add(eng_of(r["k"]))
        if len(b["metrics"]) < 150:
            b["metrics"].append({"name": nm, "value": r["v"],
                                 "live": r.get("live"),
                                 "pulled_from": r.get("src"),
                                 "engine": eng_of(r["k"]),
                                 "artifact": r["k"], "path": r["p"]})
    for b in by_source.values():
        b["engines"] = sorted(b["engines"])[:8]
        b["metrics"].sort(key=lambda m: (not m.get("live"), str(m["name"])))

    named = [r for r in paths if r.get("name") and r.get("src")]
    named.sort(key=lambda r: (not r.get("live"), r["k"], r["p"]))
    metric_directory = [{"name": r["name"], "value": r["v"],
                         "live": r.get("live"), "pulled_from": r["src"],
                         "engine": eng_of(r["k"]), "artifact": r["k"],
                         "path": r["p"]} for r in named[:1200]]

    pidx = {}
    for r in paths:
        pidx.setdefault((r["k"], r["p"]), r)

    def enrich(key, path):
        r = pidx.get((key, path))
        if r:
            return {"name": r.get("name"), "pulled_from": r.get("src"),
                    "live": r.get("live"), "engine": eng_of(key)}
        return {"engine": eng_of(key)}

    for m in mislabels[:40]:
        for pp in m["paths"]:
            pp.update(enrich(pp["artifact"], pp["path"]))
    for c in conflicts[:40]:
        for vv in c["values"]:
            vv.update(enrich(vv["artifact"], vv["path"]))
    for g in gaps[:60]:
        for cc in g["candidates"]:
            cc.update(enrich(cc["artifact"], cc["path"]))

    live_paths = len(paths)
    fresh = sum(1 for a in arts if (a.get("age_h") or 9e9) <= 48)
    out = {
        "engine": "justhodl-data-census", "version": "1.0", "marker": MARKER,
        "generated_at": now.isoformat(),
        "doctrine": "one catalog of every value the fleet publishes, so nothing is "
                    "rebuilt, gaps are visible, and cross-engine comparison catches "
                    "mislabels before they poison a downstream join.",
        "consumes": ["data/feed-registry.json (freshness — wired, not rebuilt)",
                     "data/feed-catalog.json (writers/pages — wired, not rebuilt)",
                     "data/tradingview.json (dead-symbol gap join)"],
        "totals": {"artifacts": len(arts), "artifacts_fresh_48h": fresh,
                   "artifacts_truncated_by_time_budget": truncated,
                   "artifacts_stale": sum(1 for a in arts if a.get("stale")),
                   "scalar_paths": live_paths, "parse_errors": len(errors)},
        "mislabel_candidates": mislabels[:40],
        "measure_conflicts": conflicts[:40],
        "gap_fill_candidates": gaps[:60],
        "metric_directory": metric_directory,
        "by_source": {k: v for k, v in sorted(by_source.items(), key=lambda kv: -kv[1]["n"])},
        "artifacts": [dict(a, engine=eng_of(a["key"])) for a in sorted(arts, key=lambda a: -(a.get("n_paths") or 0))],
        "honesty": "detectors are heuristic token matches — every hit is a CANDIDATE "
                   "for verification, never an auto-fix. Auto-wiring on token match is "
                   "how JPEXPYY happened.",
        "elapsed_s": round(time.time() - t0, 1),
    }
    led_paths = sorted(paths, key=lambda r: (not (r.get("name") and r.get("src")),
                                             not r.get("name")))[:150000]
    s3.put_object(Bucket=BUCKET, Key=LEDGER_KEY,
                  Body=json.dumps({"generated_at": now.isoformat(),
                                   "n_total_indexed": len(paths),
                                   "n_in_ledger": len(led_paths),
                                   "note": "capped at 150k records, named+sourced "
                                           "first; totals reflect the full index",
                                   "paths": led_paths}, default=str),
                  ContentType="application/json")
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str),
                  ContentType="application/json", CacheControl="max-age=600")
    print(f"[census] DONE {out['elapsed_s']}s arts={len(arts)} paths={live_paths} "
          f"mislabels={len(mislabels)} conflicts={len(conflicts)} gaps={len(gaps)}")
    return {"ok": True, **out["totals"],
            "mislabels": len(mislabels), "conflicts": len(conflicts),
            "gap_candidates": len(gaps)}
