"""justhodl-provider-catalog — Khalid's Data section (ops 4506).

One engine keeps ~30 dedicated provider pages complete forever: scan the
bucket against a provider registry (prefixes + hot keys), pull per-key
size/age, and enrich with FULL series inventories from each provider's
own state/catalog files (OFR 442 mnemonics, NYFed PD 1,539 ids, FRED
panel ids, BIS/Eurostat/OECD/StatCan flow lists, Treasury datasets, rp op
counts...). Writes data/provider-catalog.json (hub index) +
data/providers/{slug}.json (the complete per-provider manifest the
template page renders). Daily 06:20 + on demand."""
import gzip
import json
import os
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")

REG = {
 "ofr": {"name": "OFR — Office of Financial Research",
  "api": "data.financialresearch.gov/v1",
  "engines": ["justhodl-ofr-stfm", "justhodl-nyfed-repo-deep",
              "justhodl-plumbing-panel", "justhodl-warm-bridge"],
  "prefixes": ["data/warm/ofr/"], "hot": ["data/ofr-funding.json"],
  "series_from": ("data/warm/ofr/state.json", "catalog")},
 "nyfed": {"name": "NY Fed — Markets API",
  "api": "markets.newyorkfed.org/api",
  "engines": ["justhodl-nyfed-markets-full", "justhodl-nyfed-repo-deep",
              "justhodl-plumbing-panel", "fedliquidityapi"],
  "prefixes": ["data/warm/nyfed-markets/"],
  "hot": ["data/soma-holdings.json"],
  "series_from": ("data/warm/nyfed-markets/pd-state.json", "catalog")},
 "treasury": {"name": "US Treasury — FiscalData",
  "api": "api.fiscaldata.treasury.gov",
  "engines": ["justhodl-usgov-direct", "justhodl-warm-bridge"],
  "prefixes": ["data/warm/treasury/"],
  "hot": ["data/treasury-fiscal.json"]},
 "bea": {"name": "BEA — Bureau of Economic Analysis",
  "api": "apps.bea.gov/api", "engines": ["justhodl-usgov-direct"],
  "prefixes": ["data/warm/usgov/bea/"], "hot": ["data/bea-gdp.json"],
  "series_from": ("data/_state/bea-walk.json", "tables")},
 "bls": {"name": "BLS — Bureau of Labor Statistics",
  "api": "api.bls.gov/publicAPI/v2",
  "engines": ["justhodl-usgov-direct", "justhodl-canary-macro"],
  "prefixes": ["data/warm/usgov/bls/",
               "data/warm/fred-canary/bls-labor"],
  "hot": ["data/bls-macro.json"]},
 "fed-board": {"name": "Federal Reserve Board — DDP",
  "api": "federalreserve.gov/datadownload",
  "engines": ["justhodl-usgov-direct"],
  "prefixes": ["data/warm/usgov/ddp/", "data/warm/usgov/fed-ddp/",
               "data/warm/ddp/", "data/warm/fed-ddp/",
               "data/warm/usgov/frb"]},
 "fred": {"name": "FRED — St. Louis Fed",
  "api": "fred.stlouisfed.org",
  "engines": ["justhodl-fred-catalog", "justhodl-canary-macro",
              "many legacy engines"],
  "prefixes": ["data/warm/fred-canary/", "data/warm/fred-scoped/",
               "data/warm/fred-catalog/",
               "data/providers/fred-scoped/"],
  "hot": ["data/canary-macro.json"]},
 "sec-edgar": {"name": "SEC EDGAR — filings index",
  "api": "sec.gov/Archives", "engines": ["justhodl-edgar-index",
                                          "justhodl-cusip-map-rebuild"],
  "prefixes": ["data/warm/edgar-filings/"],
  "hot": ["data/13f-cusip-map-v2.json"]},
 "sec-bulk": {"name": "SEC — bulk XBRL",
  "api": "sec.gov (companyfacts/submissions)",
  "engines": ["justhodl-sec-bulk"],
  "prefixes": ["data/warm/sec-bulk/"]},
 "sec-dera": {"name": "SEC DERA — statement datasets",
  "api": "sec.gov/dera", "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/sec-dera/"]},
 "sec-midas": {"name": "SEC MIDAS — market structure",
  "api": "sec.gov/marketstructure",
  "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/sec-midas/"]},
 "polygon": {"name": "Polygon.io — US equities",
  "api": "api.polygon.io", "engines": ["justhodl-polygon-daily"],
  "prefixes": ["data/warm/us-equities-daily/"]},
 "gleif": {"name": "GLEIF — LEI system",
  "api": "gleif.org", "engines": ["justhodl-bis-gleif",
                                   "justhodl-symbology-master"],
  "prefixes": ["data/warm/gleif/"]},
 "bis": {"name": "BIS — Bank for Intl Settlements",
  "api": "stats.bis.org/api/v1",
  "engines": ["justhodl-bis-gleif", "justhodl-sdmx-walker"],
  "prefixes": ["data/warm/bis/"],
  "series_from": ("data/warm/bis/catalog.json.gz", "dataflows")},
 "eurostat": {"name": "Eurostat",
  "api": "ec.europa.eu/eurostat SDMX",
  "engines": ["justhodl-eurostat-oecd", "justhodl-sdmx-walker"],
  "prefixes": ["data/warm/eurostat/"],
  "series_from": ("data/warm/eurostat/catalog.json.gz",
                  "dataflows")},
 "oecd": {"name": "OECD",
  "api": "sdmx.oecd.org/public/rest",
  "engines": ["justhodl-eurostat-oecd", "justhodl-sdmx-walker"],
  "prefixes": ["data/warm/oecd/"],
  "series_from": ("data/warm/oecd/catalog.json.gz", "dataflows")},
 "statcan": {"name": "Statistics Canada",
  "api": "www150.statcan.gc.ca/t1/wds",
  "engines": ["justhodl-global-expansion", "justhodl-sdmx-walker"],
  "prefixes": ["data/warm/statcan/"],
  "series_from": ("data/warm/statcan/cube-list.json.gz", "cubes")},
 "banxico": {"name": "Banxico — SIE API",
  "api": "banxico.org.mx/SieAPIRest",
  "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/banxico/"]},
 "boe": {"name": "Bank of England — IADB",
  "api": "bankofengland.co.uk/boeapps/database",
  "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/boe/"]},
 "gdelt": {"name": "GDELT — global events",
  "api": "data.gdeltproject.org/gdeltv2",
  "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/gdelt/"]},
 "eiopa": {"name": "EIOPA — Solvency II RFR",
  "api": "eiopa.europa.eu", "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/eiopa/"]},
 "nasa": {"name": "NASA POWER — ag weather",
  "api": "power.larc.nasa.gov",
  "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/nasa-power/"]},
 "dol": {"name": "US DOL — ETA claims",
  "api": "oui.doleta.gov", "engines": ["justhodl-canary-macro"],
  "prefixes": ["data/warm/fred-canary/dol-"]},
 "chicagofed": {"name": "Chicago Fed — NFCI",
  "api": "chicagofed.org", "engines": ["justhodl-plumbing-panel"],
  "prefixes": ["data/warm/chicagofed/",
               "data/warm/fred-canary/nfci"]},
 "clevelandfed": {"name": "Cleveland Fed — yield-curve model",
  "api": "clevelandfed.org", "engines": ["justhodl-canary-macro"],
  "prefixes": ["data/warm/fred-canary/cleveland-"]},
 "atlantafed": {"name": "Atlanta Fed — GDPNow",
  "api": "atlantafed.org", "engines": ["justhodl-canary-macro"],
  "prefixes": ["data/warm/fred-canary/atlanta-"]},
 "occ": {"name": "OCC — options volume",
  "api": "theocc.com", "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/occ/"]},
 "cftc": {"name": "CFTC — futures positioning",
  "api": "publicreporting.cftc.gov",
  "engines": ["cftc-futures-positioning-agent",
              "justhodl-cftc-deep"],
  "prefixes": ["data/warm/cftc/"]},
 "yahoo": {"name": "Yahoo Finance", "api": "query1.finance.yahoo.com",
  "engines": ["legacy fleet (signature-mapped)"], "prefixes": []},
 "coinmetrics": {"name": "CoinMetrics — community",
  "api": "community-api.coinmetrics.io",
  "engines": ["legacy fleet"], "prefixes": []},
 "imf": {"name": "IMF — SDMX/MFS", "api": "dataservices.imf.org",
  "engines": ["families-feed"], "prefixes": ["data/warm/imf"]},
 "worldbank": {"name": "World Bank", "api": "api.worldbank.org",
  "engines": ["families-feed"], "prefixes": ["data/warm/worldbank"]},
 "dbnomics": {"name": "DBnomics", "api": "api.db.nomics.world",
  "engines": ["legacy fleet"], "prefixes": []},
 "snb": {"name": "Swiss National Bank", "api": "data.snb.ch",
  "engines": ["gov-sources"], "prefixes": []},
 "bcb": {"name": "Banco Central do Brasil", "api": "api.bcb.gov.br",
  "engines": ["gov-sources"], "prefixes": []},
 "cboe": {"name": "Cboe", "api": "cdn.cboe.com",
  "engines": ["legacy fleet"], "prefixes": []},
 "boj": {"name": "Bank of Japan", "api": "stat-search.boj.or.jp",
  "engines": ["gov-sources"], "prefixes": []},
 "other": {"name": "Other / mixed-signature",
  "api": "(various)", "engines": ["fleet"], "prefixes": []},
 "ecb": {"name": "ECB — SDMX",
  "api": "data-api.ecb.europa.eu",
  "engines": ["justhodl-ecb-catalog"],
  "prefixes": ["data/warm/ecb/", "data/warm/ecb-", "data/ecb"],
  "series_from": ("data/warm/ecb/catalog.json.gz", "dataflows")},
}


def _get_json(key):
    b = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz"):
        b = gzip.decompress(b)
    return json.loads(b)


def _series_list(spec):
    if not spec:
        return None
    key, field = spec
    try:
        d = _get_json(key)
        v = d.get(field)
        if isinstance(v, list):
            ids = [(x.get("id") if isinstance(x, dict) else x)
                   for x in v]
            ids = [str(i) for i in ids if i]
            return {"count": len(ids),
                    "ids": ids if len(ids) <= 2000 else
                    ids[:400] + ["…+" + str(len(ids) - 400) + " more"]}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)[:50]}"}
    return None


def _load_rollup_feeds():
    """ops 4513 v4: provider->feeds via the two-file join —
    engine-provider-map (engine->provider) x lambda-graph
    (engine->writes[feeds]). Shape-flexible on the map values."""
    try:
        emap = _get_json("data/audit/engine-provider-map.json")
        graph = _get_json("data/audit/lambda-graph.json")
    except Exception:
        # ops 4530: last-good fallback — nightly graph rewrites raced the
        # join and flapped totals 1,108<->1,627; never lose the attribution
        try:
            return _get_json("data/audit/provider-join-lastgood.json")
        except Exception:
            return {}
    if isinstance(emap, dict) and isinstance(emap.get("map"), dict):
        emap = emap["map"]  # ops 4520: THE unwrap
    elif isinstance(emap, dict) and isinstance(emap.get("engines"),
                                               dict):
        emap = emap["engines"]
    engines = (graph.get("engines") or {}) if isinstance(graph, dict) \
        else {}
    try:  # ops 4532: runner-grepped writes the graph regex missed
        ov = _get_json("data/audit/engine-writes-overrides.json")
        for e2, ws in (ov.get("writes") or {}).items():
            rec2 = engines.setdefault(e2, {"writes": [], "reads": []})
            rec2["writes"] = sorted(set(rec2.get("writes") or []) |
                                    set(ws))
    except Exception:
        pass
    out = {}
    for eng, rec in engines.items():
        pv = emap.get(eng)
        if isinstance(pv, dict):
            pv = pv.get("providers") or [pv.get("provider")]
        pvs = pv if isinstance(pv, list) else [pv]
        pvs = [x.lower().strip() for x in pvs if isinstance(x, str)]
        if not pvs:
            continue
        for feed in (rec.get("writes") or []):
            if isinstance(feed, dict):
                feed = (feed.get("key") or feed.get("feed")
                        or feed.get("path") or feed.get("name"))
            if isinstance(feed, str):
                for prov in pvs:  # ops 4533: shared feeds -> EVERY provider
                    out.setdefault(prov, []).append(feed)
    for k in out:
        out[k] = sorted(set(out[k]))
    if out:  # ops 4530: persist last-good
        try:
            s3.put_object(Bucket=BUCKET,
                          Key="data/audit/provider-join-lastgood.json",
                          Body=json.dumps(out, default=str).encode(),
                          ContentType="application/json")
        except Exception:
            pass
    try:  # ops 4519: self-debug — no more guessing
        s3.put_object(Bucket=BUCKET,
                      Key="data/audit/provider-join-debug.json",
                      Body=json.dumps({
                          "n_map": len(emap),
                          "n_graph_engines": len(engines),
                          "map_sample": dict(list(emap.items())[:2]),
                          "graph_sample": {k: v for k, v in
                                           list(engines.items())[:1]},
                          "n_out_providers": len(out),
                          "out_sizes": {k: len(v) for k, v in
                                        list(out.items())[:12]}},
                          default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
    except Exception:
        pass
    return out


def _hot_index():
    """Size/age for every object directly under data/ (hot feeds)."""
    now = datetime.now(timezone.utc)
    idx = {}
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": "data/", "Delimiter": "/",
              "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            idx[o["Key"]] = (o["Size"], round(
                (now - o["LastModified"]).total_seconds() / 3600, 1))
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    return idx


ALIAS = {"llm-anthropic": "other", "transform-agent": "other",
         "fleet-feed": None, "unmapped": "other",
         "fed-board": "fed-board", "nasdaq": "other"}


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    roll = _load_rollup_feeds()
    hotidx = _hot_index()
    key_engines = {}  # ops 4544: reverse index for the drill-down
    KEY_PROVIDER_SKIP = {  # 4546: misattributions (Perplexity)
        "data/finviz-universe.json": {"fred"}}
    PREFIX_ENGINES = {  # walker/dynamic keys the graph can't see
        "data/warm/eurostat/": ["justhodl-sdmx-walker"],
        "data/warm/oecd/": ["justhodl-sdmx-walker"],
        "data/warm/statcan/": ["justhodl-sdmx-walker",
                               "justhodl-global-expansion"],
        "data/warm/bis/": ["justhodl-sdmx-walker",
                           "justhodl-bis-gleif"],
        "data/warm/fred-canary/": ["justhodl-canary-macro"],
        "data/warm/fred-scoped/": ["justhodl-fred-catalog"],
        "data/warm/fred-catalog/": ["justhodl-fred-catalog"],
        "data/providers/fred-scoped/": ["justhodl-fred-catalog"]}
    try:
        g2 = _get_json("data/audit/lambda-graph.json")
        ov2 = _get_json("data/audit/engine-writes-overrides.json")
        for _e, _rec in ((g2.get("engines") or {}) | {}).items():
            for _w in (_rec.get("writes") or []):
                if isinstance(_w, dict):
                    _w = _w.get("key") or _w.get("feed")
                if isinstance(_w, str):
                    key_engines.setdefault(_w, []).append(_e)
        for _e, _ws in (ov2.get("writes") or {}).items():
            for _w in _ws:
                key_engines.setdefault(_w, []).append(_e)
        for _k in key_engines:
            key_engines[_k] = sorted(set(key_engines[_k]))[:4]
    except Exception:
        pass
    try:  # ops 4508 discovery: real subfolder names, printed once
        r1 = s3.list_objects_v2(Bucket=BUCKET,
                                Prefix="data/warm/usgov/",
                                Delimiter="/")
        print("USGOV_SUBS:", [c["Prefix"] for c in
                              r1.get("CommonPrefixes", [])])
        r2 = s3.list_objects_v2(Bucket=BUCKET, Prefix="data/warm/ec",
                                MaxKeys=15)
        print("EC_KEYS:", [o["Key"] for o in r2.get("Contents", [])])
    except Exception as _e:
        print("disc_err", _e)
    hub = {"as_of": now.isoformat(timespec="seconds"), "providers": []}
    for slug, r in REG.items():
        keys = []
        tot = 0
        for pref in r.get("prefixes", []):
            tok = None
            while True:
                kw = {"Bucket": BUCKET, "Prefix": pref,
                      "MaxKeys": 400}
                if tok:
                    kw["ContinuationToken"] = tok
                resp = s3.list_objects_v2(**kw)
                for o in resp.get("Contents", []):
                    age = round((now - o["LastModified"])
                                .total_seconds() / 3600, 1)
                    keys.append({"key": o["Key"],
                                 "bytes": o["Size"],
                                 "age_h": age})
                    tot += o["Size"]
                if not resp.get("IsTruncated"):
                    break
                tok = resp.get("NextContinuationToken")
        for hk in r.get("hot", []):
            try:
                h = s3.head_object(Bucket=BUCKET, Key=hk)
                keys.append({"key": hk, "bytes": h["ContentLength"],
                             "age_h": round(
                                 (now - h["LastModified"])
                                 .total_seconds() / 3600, 1),
                             "hot": True})
                tot += h["ContentLength"]
            except Exception:
                keys.append({"key": hk, "missing": True,
                             "hot": True})
        # v3: rollup-attributed hot feeds for this provider
        rslugs = [slug] + [k for k, v in ALIAS.items() if v == slug]
        seen = {k["key"] for k in keys}
        n_roll = 0
        for rs in rslugs:
            for feed in roll.get(rs, []):
                fk = feed if feed.startswith("data/") else (
                    "data/" + feed.lstrip("/"))
                if slug in KEY_PROVIDER_SKIP.get(fk, ()):  # 4546
                    continue
                if fk in seen:
                    continue
                sz, age = hotidx.get(fk, (None, None))
                keys.append({"key": fk, "bytes": sz, "age_h": age,
                             "hot": True, "via": "rollup"})
                if sz:
                    tot += sz
                seen.add(fk)
                n_roll += 1
        keys.sort(key=lambda x: (x.get("bytes") or 0), reverse=True)
        ser = _series_list(r.get("series_from"))
        doc = {"slug": slug, "name": r["name"], "api": r["api"],
               "engines": r["engines"],
               "as_of": hub["as_of"],
               "n_keys": len([k for k in keys
                              if not k.get("missing")]),
               "total_bytes": tot,
               "total_mb": round(tot / 1e6, 2),
               "freshest_h": min([k["age_h"] for k in keys
                                  if k.get("age_h") is not None]
                                 or [None],
                                 key=lambda x: (x is None, x)),
               "series": ser,
               "page_size": 500,
               "n_pages": max(0, (len(keys) - 100 + 499) // 500),
               "keys": [dict(k, status=("missing" if k.get("missing")
                                        else "live" if k.get("age_h")
                                        is not None else "cold"),
                             engines=(key_engines.get(k["key"])
                                      or next((v for pf, v in
                                               PREFIX_ENGINES.items()
                                               if k["key"]
                                               .startswith(pf)), [])))
                        for k in keys[:100]]}
        for _pi in range(doc["n_pages"]):
            _chunk = keys[100 + _pi * 500:100 + (_pi + 1) * 500]
            s3.put_object(
                Bucket=BUCKET,
                Key=f"data/providers/{slug}/page-{_pi:03d}.json",
                Body=json.dumps({"page": _pi, "keys": [
                    dict(k, status=("missing" if k.get("missing")
                                    else "live" if k.get("age_h")
                                    is not None else "cold"),
                         engines=(key_engines.get(k["key"])
                                  or next((v for pf, v in
                                           PREFIX_ENGINES.items()
                                           if k["key"]
                                           .startswith(pf)), [])))
                    for k in _chunk]}, default=str).encode(),
                ContentType="application/json",
                CacheControl="no-cache")
        s3.put_object(Bucket=BUCKET,
                      Key=f"data/providers/{slug}.json",
                      Body=json.dumps(doc, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        n_live = len([k for k in keys if not k.get("missing")])
        # ops 4543 (Perplexity P1): the catalog count is a TARGET, not a
        # holding. datasets = what we actually have; target + coverage
        # ride alongside. Eurostat is 3.6%, not done.
        ds = n_live
        # ops 4548 (Khalid: why isn't Eurostat/StatCan/BIS coverage
        # showing?): my earlier hardcoded/catalog-file targets went
        # stale the moment the walker's OWN live catalog grew past
        # them (StatCan) or drifted slightly (Eurostat extra keys).
        # The walker's state file already stores n_total — the exact
        # number it is walking against. Use THAT as ground truth.
        tgt = None
        cov_basis = None
        if slug in ("eurostat", "oecd", "statcan", "bis"):
            try:
                wst = _get_json(
                    f"data/_state/sdmx-walk-{slug}.json")
                tgt = wst.get("n_total")
                cov_basis = "keys/keys (walker n_total — same unit)"
            except Exception:
                tgt = None
        # ops 4574 (wo-4559 BUG-10, real half): the old fallback compared
        # banked KEYS to catalog SERIES counts — a fabricated percentage
        # whenever one key holds many series. A coverage % is only shown
        # when numerator and denominator share a declared unit; otherwise
        # datasets and series_count both stay visible, un-ratioed.
        # never hide progress: cap the DISPLAYED pct at 100, note if
        # actual exceeds nominal (a few retried/renamed keys, not a
        # real overshoot).
        cov = (round(min(100.0, 100.0 * n_live / tgt), 1)
               if tgt else None)
        cov_note = ("at_or_above_target"
                    if (tgt and n_live >= tgt) else None)
        denied = None
        if slug in ("oecd", "statcan"):
            # ops 4568: StatCan's 8,221-vs-7,962 gap = walker failures;
            # surface them exactly like OECD's source-side denials.
            try:
                denied = len((_get_json(
                    f"data/_state/sdmx-walk-{slug}.json")
                    .get("failures") or {}))
            except Exception:
                denied = None
        note = None
        if slug == "fred":
            # ops 4569: the manifest counter double-counts re-walked
            # series (its imported_ids dedup list caps at 2000) — the
            # on-disk scoped objects are the truth. Count those; the
            # manifest only supplies category progress + status.
            _fi = len([k for k in keys if k.get("key", "")
                       .startswith("data/warm/fred-scoped/")])
            try:
                _fm = _get_json(
                    "data/providers/fred-scoped/manifest.json")
                note = (f"scoped import: {_fi:,} series banked · "
                        f"{_fm.get('categories_done') or 0}/"
                        f"{_fm.get('categories_total') or 0}"
                        f" categories · {_fm.get('status') or '—'}")
            except Exception:
                note = f"scoped import: {_fi:,} series banked"
            if _fi:
                ser = dict(ser or {}, count=_fi)
        hub["providers"].append(
            {"slug": slug, "name": r["name"], "api": r["api"],
             "datasets": ds, "datasets_target": tgt,
             "coverage_pct": cov, "coverage_note": cov_note,
             "coverage_basis": (cov_basis if cov is not None else None),
             "denied_source_side": denied,
             "unit": "keys",
             "n_keys": doc["n_keys"], "total_mb": doc["total_mb"],
             "hot_feeds": n_roll,
             "series_count": (ser or {}).get("count"),
             "catalog_note": note,
             "freshest_h": doc["freshest_h"]})
    hub["providers"].sort(key=lambda p: -(p["total_mb"] or 0))
    hub.setdefault("breakdown", {})["keys"] = sum(
        p["n_keys"] for p in hub["providers"])
    # ops 4534: Khalid — "a LOT more datasets": count at the SERIES level.
    # keys = S3 objects (containers); datasets = the series/symbols/flows
    # the platform actually tracks inside them.
    series_sum = sum((p.get("series_count") or 0)
                     for p in hub["providers"])
    extras = {}
    _xstat = {}  # ops 4568: extras were shipping 0 keys/0 MB/no
    # freshness — the page card rendered nothing but zeros while the
    # real instrument counts sat unrendered in `datasets`.
    try:  # canonical indicator bus (18k+ indicators)
        ib = _get_json("data/indicator-bus.json")
        n_ib = (ib.get("n") or ib.get("count") or
                len(ib.get("indicators") or ib.get("items") or []))
        if n_ib:
            extras["indicator_bus"] = n_ib
            _sa = hotidx.get("data/indicator-bus.json")
            if _sa:
                _xstat["indicator_bus"] = {
                    "n_keys": 1, "mb": round(_sa[0] / 1e6, 2),
                    "freshest_h": _sa[1]}
    except Exception:
        pass
    try:  # per-ticker equity research universe (count objects)
        n_eq, tok2, _teq, _feq = 0, None, 0, None
        while True:
            kw2 = {"Bucket": BUCKET, "Prefix": "equity-research/",
                   "MaxKeys": 1000}
            if tok2:
                kw2["ContinuationToken"] = tok2
            r2 = s3.list_objects_v2(**kw2)
            for _o2 in r2.get("Contents", []):
                n_eq += 1
                _teq += _o2["Size"]
                _a2 = round((now - _o2["LastModified"])
                            .total_seconds() / 3600, 1)
                if _feq is None or _a2 < _feq:
                    _feq = _a2
            if not r2.get("IsTruncated"):
                break
            tok2 = r2.get("NextContinuationToken")
        if n_eq:
            extras["equity_research_tickers"] = n_eq
            _xstat["equity_research_tickers"] = {
                "n_keys": n_eq, "mb": round(_teq / 1e6, 2),
                "freshest_h": _feq}
    except Exception:
        pass
    try:  # TradingView vault LIVE symbols
        tv = _get_json("data/tradingview.json")
        n_tv = (tv.get("n_live") or tv.get("live") or
                len([s for s in (tv.get("symbols") or {}).values()
                     if isinstance(s, dict)
                     and s.get("status") == "LIVE"])
                if isinstance(tv, dict) else 0)
        if isinstance(n_tv, int) and n_tv:
            extras["tradingview_vault_live"] = n_tv
            _st2 = hotidx.get("data/tradingview.json")
            if _st2:
                _xstat["tradingview_vault_live"] = {
                    "n_keys": 1, "mb": round(_st2[0] / 1e6, 2),
                    "freshest_h": _st2[1]}
    except Exception:
        pass
    # ops 4537 (Perplexity item 6): instruments become first-class
    # attributed provider rows — nothing floats unattributed.
    _extra_meta = {
        "indicator_bus": ("Canonical Indicator Bus",
                          "internal — justhodl indicator registry"),
        "equity_research_tickers": ("Equity Research Universe",
                                    "internal — per-ticker reports"),
        "tradingview_vault_live": ("TradingView Vault (LIVE)",
                                   "tradingview.com via extension")}
    for _ek, _ev in extras.items():
        _nm, _api = _extra_meta.get(_ek, (_ek, "internal"))
        _xs = _xstat.get(_ek, {})
        hub["providers"].append(
            {"slug": _ek.replace("_", "-"), "name": _nm, "api": _api,
             "datasets": _ev, "unit": "instruments",
             "n_keys": _xs.get("n_keys", 0),
             "total_mb": _xs.get("mb", 0), "hot_feeds": 0,
             "series_count": None,
             "freshest_h": _xs.get("freshest_h")})
    hub["series_extras"] = extras
    # ops 4535 (Perplexity P1): auditable reconcile —
    # sum(provider.datasets) + instruments == datasets_total, and units
    # are split so series never mix silently with instrument counts.
    hub["datasets_total"] = sum(p.get("datasets") or 0
                                for p in hub["providers"])
    hub["reconcile_ok"] = True  # by construction: total = sum(rows)
    # ops 4548b: every normal provider is unit=="keys" now (by
    # design, per Perplexity's actual/target split) — the old
    # "series" bucket was permanently 0 and confusing on the page.
    # Two honest buckets only.
    hub["breakdown"] = {
        "provider_datasets": sum(p["datasets"]
                                 for p in hub["providers"]
                                 if p.get("unit") == "keys"),
        "instruments": sum(p["datasets"] for p in hub["providers"]
                           if p.get("unit") == "instruments")}
    hub["totals"] = {"providers": len(hub["providers"]),
                     "datasets": hub["datasets_total"],
                     "keys": sum(p["n_keys"]
                                 for p in hub["providers"]),
                     "gb": round(sum(p["total_mb"]
                                     for p in hub["providers"])
                                 / 1000, 2)}
    s3.put_object(Bucket=BUCKET, Key="data/provider-catalog.json",
                  Body=json.dumps(hub, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    res = {"ok": True, **hub["totals"]}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
