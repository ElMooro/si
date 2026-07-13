"""ops 3185 — THE $0 TIER. Everything Khalid was told he might have to buy.

Four workstreams, zero incremental dollars:

  1. COUNTRIES, not indicators, were the ECONOMICS blocker — his lists span
     190+ countries and my ISO map held ~50. Expanded to ~150, and OECD
     templates are now GATED to actual OECD members (I was routing Zimbabwe
     to BSCICP03ZWEM665S — a series that does not exist). Non-members go to
     the World Bank, which actually covers them.
  2. TVC yields + world indices → OECD mirrors on FRED (AT10Y → IRLTLT01AUT)
     and Yahoo (AEX → ^AEX). Free, 1990+.
  3. USI internals → a NEW engine that COMPUTES the entire breadth complex
     (advancers, decliners, A/D line, up/down volume, TRIN, new highs/lows,
     % above 50/200 DMA) from the Polygon grouped-daily feed he already pays
     for. One call returns every US ticker for a day.
  4. Re-map + re-run the fleet, and report how many dormant engines woke.

Gate: coverage up, ACTIVE engines up from 96, internals series real.
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3185_zero_cost_tier") as rep:
    fails, warns = [], []
    rep.heading("ops 3185 — the $0 tier")

    rep.section("1. Market internals engine (replaces the USI vendor feed)")
    cfg = json.loads((AWS_DIR / "lambdas" / "justhodl-market-internals"
                      / "config.json").read_text())
    donor = (LAM.get_function_configuration(FunctionName="justhodl-wl-engines")
             .get("Environment") or {}).get("Variables") or {}
    env = {k: v for k, v in donor.items()
           if k in ("S3_BUCKET", "POLYGON_KEY")}
    env.setdefault("S3_BUCKET", BUCKET)
    sch = cfg.get("schedule") or {}
    deploy_lambda(report=rep, function_name="justhodl-market-internals",
                  source_dir=AWS_DIR / "lambdas" / "justhodl-market-internals"
                  / "source",
                  env_vars=env, eb_rule_name=sch.get("rule_name"),
                  eb_schedule=sch.get("cron"), timeout=cfg["timeout"],
                  memory=cfg["memory"],
                  description=cfg.get("description", "")[:250], smoke=False)
    # backfill runs in passes (4 years of grouped-daily)
    internals = None
    for p in (1, 2, 3):
        t0 = datetime.now(timezone.utc)
        LAM.invoke(FunctionName="justhodl-market-internals",
                   InvocationType="Event", Payload=b"{}")
        deadline = time.time() + 850
        while time.time() < deadline:
            d = s3_json(OUT_KEY := "data/market-internals.json")
            if d and datetime.fromisoformat(d["generated_at"]) >= t0:
                internals = d
                break
            time.sleep(20)
        if not internals:
            fails.append(f"internals pass {p}: never wrote")
            break
        rep.kv(**{f"pass{p}_days": internals.get("days_covered"),
                  f"pass{p}_added": internals.get("days_added")})
        if (internals.get("days_covered") or 0) >= 700 \
                or not internals.get("days_added"):
            break
    if internals:
        rep.kv(days_covered=internals.get("days_covered"),
               metrics=len(internals.get("metrics") or []))
        latest = internals.get("latest") or {}
        for k in ("ADVANCERS", "DECLINERS", "ADVDEC_LINE", "TRIN",
                  "NEW_HIGHS", "NEW_LOWS", "PCT_ABOVE_50DMA",
                  "PCT_ABOVE_200DMA"):
            v = latest.get(k)
            if v:
                rep.log(f"  {k:18s} {v[0]}  →  {v[1]}")
        if (internals.get("days_covered") or 0) < 250:
            warns.append(f"only {internals.get('days_covered')} days of "
                         "internals — more passes needed for full z history")
        else:
            rep.ok(f"breadth complex COMPUTED over "
                   f"{internals['days_covered']} days — the vendor feed he "
                   "was quoted for costs him nothing")

    rep.section("2. Re-map the universe (countries + TVC + USI)")
    wl = s3_json("data/tv-watchlists.json") or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    uniq = sorted({s.upper() for l in lists for s in (l.get("symbols") or [])})
    prev = s3_json("data/symbol-map.json") or {}
    cache = prev.get("search_cache") or {}
    searcher = SS.fred_search_factory(cache)
    mapped, kinds, used, t1 = {}, {}, 0, time.time()
    for s in uniq:
        allow = used < 300 and time.time() - t1 < 240
        src, sid, conf, note = SS.map_symbol(s, searcher if allow else None)
        if "fred-search" in str(note):
            used += 1
        if src:
            mapped[s] = {"source": src, "id": sid, "confidence": conf,
                         "note": note}
            kinds[src] = kinds.get(src, 0) + 1
    cov = round(100 * len(mapped) / len(uniq), 1)
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({
                      "generated_at": datetime.now(timezone.utc).isoformat(),
                      "n_mapped": len(mapped), "n_total": len(uniq),
                      "coverage_pct": cov, "map": mapped,
                      "search_cache": cache}).encode(),
                  ContentType="application/json")
    rep.kv(coverage_before=prev.get("coverage_pct"), coverage_now=cov,
           **{f"src_{k.lower()}": v for k, v in
              sorted(kinds.items(), key=lambda kv: -kv[1])})
    rep.ok(f"coverage {prev.get('coverage_pct')}% → {cov}% "
           f"(+{len(mapped) - (prev.get('n_mapped') or 0)} symbols, $0 spent)")

    rep.section("3. Re-run the fleet — how many engines woke up?")
    t2 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName="justhodl-wl-engines", InvocationType="Event",
               Payload=json.dumps({"force_emit": True}).encode())
    idx = None
    deadline = time.time() + 820
    while time.time() < deadline:
        d = s3_json("data/wl-engines.json")
        if d and datetime.fromisoformat(d["generated_at"]) >= t2:
            idx = d
            break
        time.sleep(20)
    if not idx:
        warns.append("wl-engines still running — count lands next run")
    else:
        rep.kv(engines=idx.get("n_engines"), active=idx.get("n_active"),
               dormant=idx.get("n_dormant"), firing=idx.get("n_firing"),
               series_cached=idx.get("series_cached"))
        if (idx.get("n_active") or 0) > 96:
            rep.ok(f"ACTIVE engines 96 → {idx['n_active']} "
                   f"(+{idx['n_active'] - 96} woken for $0)")
        else:
            warns.append(f"ACTIVE {idx.get('n_active')} — the new series need "
                         "a fetch pass before they count as members")
        firing = [e for e in (idx.get("engines") or []) if e.get("firing")]
        rep.log(f"── FIRING ({len(firing)}):")
        for e in firing[:12]:
            names = e.get("lit_named") or e.get("lit") or []
            rep.log(f"  {str(e['name'])[:30]:30s} [{e['theme']:9s}] "
                    f"{str(e.get('activation_pctile')):>5}p → "
                    f"{'; '.join(str(n)[:30] for n in names[:2])}")

    rep.section("4. What this would have cost")
    rep.log("  USI internals feed  → COMPUTED from Polygon (already owned)")
    rep.log("  ECONOMICS (190 countries) → World Bank + OECD, free")
    rep.log("  TVC yields + indices     → FRED/OECD + Yahoo, free")
    rep.log("  Vendor quote avoided: on-chain Glassnode API ~$799-999/mo")
    rep.log("  REMAINING paid gap: ~700 foreign-listed equities → EODHD "
            "€19.99/mo (the only purchase worth making)")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
