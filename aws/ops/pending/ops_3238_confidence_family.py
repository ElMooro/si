"""ops 3238 — the confidence families, batched: the census showed *BCOI
(business confidence) and *CCI (consumer confidence) dry across many
countries, each blocking multiple engines — and EUBCOI's solve proved
dbn_search_factory handles exactly this class. One batch: every dry
ECONOMICS:<CC>(BCOI|CCI) in the misses ledger goes through the proven
searcher with its country tokens (USCCI short-circuits to FRED UMCSENT).
Probe-gated; fleet; wakes by name."""
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
COUNTRY = {"CH": ("Switzerland", ("CH", "CHE")),
           "JP": ("Japan", ("JP", "JPN")),
           "IT": ("Italy", ("IT", "ITA")),
           "NL": ("Netherlands", ("NL", "NLD")),
           "CN": ("China", ("CN", "CHN")),
           "FR": ("France", ("FR", "FRA")),
           "ES": ("Spain", ("ES", "ESP")),
           "SE": ("Sweden", ("SE", "SWE")),
           "DE": ("Germany", ("DE", "DEU")),
           "GB": ("United Kingdom", ("GB", "GBR")),
           "KR": ("Korea", ("KR", "KOR")),
           "AU": ("Australia", ("AU", "AUS")),
           "CA": ("Canada", ("CA", "CAN")),
           "BR": ("Brazil", ("BR", "BRA")),
           "MX": ("Mexico", ("MX", "MEX")),
           "IN": ("India", ("IN", "IND"))}
PAT = re.compile(r"^ECONOMICS:([A-Z]{2})(BCOI|CCI)$")


def s3_json(key, default=None, gz=False):
    try:
        import gzip
        b = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if gz:
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return default


with report("3238_confidence_family") as rep:
    fails, warns = [], []
    rep.heading("ops 3238 — the confidence families through the proven "
                "searcher")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    dry = dict(prev.get("dry") or {})
    cache = dict(prev.get("search_cache") or {})
    st = s3_json("data/thesis-state-v2.json.gz", {}, gz=True) or {}
    misses = st.get("misses") or {}
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}
    searcher = SS.dbn_search_factory(cache)

    targets = sorted({s for s in list(misses) + list(mapped)
                      if PAT.match(s)
                      and (s in misses or s not in curated)})
    rep.kv(family_targets=len(targets))
    landed, t0 = 0, time.time()

    rep.section("1. Batch search → probe (budgeted)")
    for sym in targets:
        if time.time() - t0 > 420:
            warns.append("budget reached — remaining targets next pass")
            break
        cc, kind = PAT.match(sym).groups()
        cinfo = COUNTRY.get(cc)
        if not cinfo:
            continue
        cname, toks = cinfo
        if sym == "ECONOMICS:USCCI":
            cands = ["__FRED__UMCSENT"]
        else:
            q = (f"business confidence indicator {cname}"
                 if kind == "BCOI"
                 else f"consumer confidence indicator {cname}")
            cands = searcher(q, country=cname, iso=toks)
        got = False
        for sid in cands[:3]:
            src = "FRED" if sid.startswith("__FRED__") else "DBNOMICS"
            rid = sid.replace("__FRED__", "")
            if dry.get(sym) == rid:
                continue
            try:
                n = len(SS.fetch(src, rid))
            except Exception:
                n = 0
            if n >= 60:
                e = {"source": src, "id": rid, "confidence": 0.8,
                     "note": f"{cname} {'business' if kind == 'BCOI' else 'consumer'} "
                             "confidence (ops 3238)"}
                mapped[sym] = e
                curated[sym] = e
                landed += 1
                got = True
                rep.log(f"  ✓ {sym:<22} {rid[:52]}  ({n})")
                break
        if not got:
            rep.log(f"  ✗ {sym:<22} {len(cands)} cands, none wet")
    rep.kv(curated_now=landed)

    if landed:
        rep.section("2. Write + fleet — wakes by name")
        wl = s3_json("data/tv-watchlists.json") or {}
        uniq = sorted({s.upper() for l in (wl.get("lists") or [])
                       if not str(l.get("id", "")).startswith("e2e-")
                       for s in (l.get("symbols") or [])})
        cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
        S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                      Body=json.dumps({**{k: prev.get(k) for k in
                                          ("licensed_econ",
                                           "usi_intraday_only",
                                           "retired") if k in prev},
                                       "generated_at":
                                           datetime.now(timezone.utc)
                                           .isoformat(),
                                       "coverage_pct": cov,
                                       "map": mapped, "curated": curated,
                                       "dry": dry,
                                       "search_cache": cache,
                                       "note": "ops 3238: confidence "
                                               "families"}),
                      ContentType="application/json")
        rep.kv(coverage_now=cov)
        mark = datetime.now(timezone.utc).isoformat()
        try:
            LAM.invoke(FunctionName="justhodl-wl-engines",
                       InvocationType="Event", Payload=b"{}")
        except Exception as e:
            fails.append(f"invoke: {str(e)[:70]}")
        idx2 = None
        for _ in range(70):
            time.sleep(10)
            d = s3_json("data/wl-engines.json") or {}
            if str(d.get("generated_at", "")) > mark:
                idx2 = d
                break
        if idx2:
            eng2 = idx2.get("engines") or []
            act2 = {e["engine_id"] for e in eng2
                    if str(e.get("state")) == "ACTIVE"}
            woken = sorted(act2 - prev_active)
            rep.kv(active_before=len(prev_active),
                   active_now=len(act2), woken=len(woken))
            for w in woken[:10]:
                nm = next((e.get("name") for e in eng2
                           if e.get("engine_id") == w), w)
                rep.log(f"  ⏰ WOKE: {nm}")
            if woken:
                rep.ok(f"{len(woken)} panels WOKEN")
        else:
            warns.append("index not fresh in window")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
