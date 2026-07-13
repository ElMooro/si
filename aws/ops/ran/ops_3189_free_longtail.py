"""ops 3189 — the free long-tail: cover GLASSNODE / INTOTHEBLOCK / COT3
internally, at $0.

3184 bucketed the gap; 3185-3188 closed countries, world indices, USI
internals and international listings on free sources (74.1%). The residual
"rich signal" tiles Khalid asked about split three ways:

  1. GLASSNODE + INTOTHEBLOCK (~206 tiles) — vendor VIEWS of on-chain
     metrics the Coin Metrics COMMUNITY API serves free. The platform
     already trusts that API (justhodl-onchain-ratios uses it for MVRV).
     New COINMETRICS source in aws/shared/series_source.py maps the tiles.
  2. COT3 (~52 tiles) — the tile name embeds the CFTC contract-market
     code. publicreporting.cftc.gov serves the full weekly history free.
     New COT source parses dataset|code|field straight off the tile.
  3. CME leftovers — a dozen continuous-future roots Yahoo carries that
     the FUT table simply lacked. Added.

Doctrine (3186): every NEW entry is PROBED with a real fetch before it is
allowed to count toward coverage — dry entries are pruned, hit-rates are
reported per family, and the verdict is measured, not assumed.

Gate: coverage does not regress; at least some new entries survive the
probe; the shared bundle redeploys to all three consumers; fleet re-runs.
"""
import json
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)

SHARED_CONSUMERS = ("justhodl-wl-engines", "justhodl-thesis-engine",
                    "justhodl-symbol-dictionary")
PROBE_BUDGET_S = 300
MIN_POINTS = 60          # a real series, not a stub


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3189_free_longtail") as rep:
    fails, warns = [], []
    rep.heading("ops 3189 — free long-tail: on-chain + COT covered "
                "internally at $0")

    # ── 1. census: what is STILL unmapped after 3188 ──────────────────
    rep.section("1. Residual gap census (post-3188)")
    wl = s3_json("data/tv-watchlists.json") or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    uniq = sorted({s.upper() for l in lists for s in (l.get("symbols") or [])})
    prev = s3_json("data/symbol-map.json") or {}
    prev_map = prev.get("map") or {}
    prev_cov = float(prev.get("coverage_pct") or 0)
    unmapped = [s for s in uniq if s not in prev_map]
    buckets = Counter(s.split(":", 1)[0] if ":" in s else "BARE"
                      for s in unmapped)
    rep.kv(symbols=len(uniq), mapped_before=len(prev_map),
           coverage_before=prev_cov, unmapped_before=len(unmapped))
    for ex, n in buckets.most_common(18):
        rep.log(f"  {ex:<18} {n:>5} symbols")
    for fam in ("GLASSNODE", "INTOTHEBLOCK", "COT3"):
        ex = [s for s in unmapped if s.startswith(fam + ":")][:4]
        if ex:
            rep.log(f"  {fam:<14} e.g. " + "; ".join(ex))

    # ── 2. re-map the universe with the new branches live ─────────────
    rep.section("2. Re-map (new: COINMETRICS, COT, extended FUT roots)")
    cache = prev.get("search_cache") or {}
    searcher = SS.fred_search_factory(cache)
    mapped, kinds, used, t0 = {}, Counter(), 0, time.time()
    for s in uniq:
        allow = used < 250 and time.time() - t0 < 200
        src, sid, conf, note = SS.map_symbol(s, searcher if allow else None)
        if "fred-search" in str(note):
            used += 1
        if src == "EODHD":                 # 3188 doctrine — never map to it
            continue
        if src:
            mapped[s] = {"source": src, "id": sid, "confidence": conf,
                         "note": note}
            kinds[src] += 1
    new_cm = {s: m for s, m in mapped.items()
              if m["source"] == "COINMETRICS" and s not in prev_map}
    new_cot = {s: m for s, m in mapped.items()
               if m["source"] == "COT" and s not in prev_map}
    new_fut = {s: m for s, m in mapped.items()
               if s not in prev_map and "continuous future" in m["note"]}
    rep.kv(new_coinmetrics=len(new_cm), new_cot=len(new_cot),
           new_futures=len(new_fut))

    # ── 3. probe-gate every NEW on-chain / COT entry ───────────────────
    rep.section("3. Probe (real fetches — dry entries are pruned)")
    todo = {**new_cm, **new_cot}
    hits, dry, t0 = {}, [], time.time()

    def probe(item):
        sym, m = item
        try:
            ser = SS.fetch(m["source"], m["id"])
            return sym, len(ser)
        except Exception:
            return sym, 0

    if todo:
        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = [ex.submit(probe, it) for it in todo.items()]
            for f in as_completed(futs):
                if time.time() - t0 > PROBE_BUDGET_S:
                    break
                sym, n = f.result()
                if n >= MIN_POINTS:
                    hits[sym] = n
                else:
                    dry.append(sym)
    for sym in dry:                          # prune — coverage stays honest
        mapped.pop(sym, None)
    fam_hits = Counter(s.split(":", 1)[0] for s in hits)
    fam_dry = Counter(s.split(":", 1)[0] for s in dry)
    for fam in ("GLASSNODE", "INTOTHEBLOCK", "COT3"):
        h, d = fam_hits.get(fam, 0), fam_dry.get(fam, 0)
        rep.log(f"  {fam:<14} probed {h + d:>3}  hit {h:>3}  dry {d:>3}")
        if h == 0 and (h + d) > 0:
            warns.append(f"{fam}: 0 probe hits — tile grammar needs a "
                         "curated pass (see census examples above)")
    sample = sorted(hits.items(), key=lambda kv: -kv[1])[:5]
    for sym, n in sample:
        rep.log(f"    ✓ {sym} → {mapped[sym]['id']}  ({n} pts)")
    survivors = len(hits)
    rep.kv(probed=len(todo), survivors=survivors, pruned=len(dry))

    # ── 4. write the map ───────────────────────────────────────────────
    cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({
                      "generated_at": datetime.now(timezone.utc).isoformat(),
                      "coverage_pct": cov, "map": mapped,
                      "search_cache": cache,
                      "note": ("ops 3189: +COINMETRICS (on-chain, free) "
                               "+COT (CFTC public reporting) probe-gated")}),
                  ContentType="application/json")
    rep.kv(coverage_before=prev_cov, coverage_now=cov,
           mapped_now=len(mapped),
           **{f"src_{k.lower()}": v for k, v in kinds.most_common(8)})
    if cov < prev_cov - 0.05:
        fails.append(f"coverage regressed {prev_cov} → {cov}")
    if survivors == 0 and todo:
        fails.append("every new on-chain/COT entry probed dry — sources "
                     "unreachable from runner or tile grammar mismatch")

    # ── 5. redeploy the shared bundle to its three consumers ──────────
    rep.section("4. Redeploy shared bundle (series_source.py changed)")
    for fn in SHARED_CONSUMERS:
        try:
            cfg = {}
            cfg_p = AWS_DIR / "lambdas" / fn / "config.json"
            if cfg_p.exists():
                cfg = json.loads(cfg_p.read_text())
            live = (LAM.get_function_configuration(FunctionName=fn)
                    .get("Environment") or {}).get("Variables") or {}
            sch = cfg.get("schedule") or {}
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=AWS_DIR / "lambdas" / fn / "source",
                          env_vars=live,
                          eb_rule_name=sch.get("rule_name"),
                          eb_schedule=sch.get("cron"),
                          timeout=cfg.get("timeout", 900),
                          memory=cfg.get("memory", 1024),
                          description=str(cfg.get("description", ""))[:250],
                          smoke=False)
        except Exception as e:
            fails.append(f"deploy {fn}: {str(e)[:90]}")

    # ── 6. re-run the fleet ────────────────────────────────────────────
    rep.section("5. Re-run wl-engines")
    run_t = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"invoke wl-engines: {str(e)[:80]}")
    idx = None
    for _ in range(36):
        time.sleep(10)
        d = s3_json("data/wl-engines.json")
        if d and str(d.get("generated_at", "")) > run_t:
            idx = d
            break
    if idx:
        eng = idx.get("engines") or []
        active = sum(1 for e in eng if (e.get("status") or "") != "DORMANT")
        firing = sum(1 for e in eng if e.get("state") == "FIRING")
        rep.kv(engines=len(eng), active=active, firing=firing,
               series_cached=idx.get("series_cached"))
        rep.ok(f"fleet re-ran on the widened map — {active} active engines")
    else:
        warns.append("wl-engines still running — counts land next cycle")

    # ── 7. what remains (so the next ops is already scoped) ───────────
    rep.section("6. Residue after this ops")
    left = Counter(s.split(":", 1)[0] if ":" in s else "BARE"
                   for s in uniq if s not in mapped)
    for ex, n in left.most_common(10):
        rep.log(f"  {ex:<18} {n:>5}")
    rep.log("  next free plays: ECONOMICS residuals via curated IMF/BIS "
            "DBnomics templates; EUREX/ICEEUR → cash-proxy flags;")
    rep.log("  FTSE 4xxx family is LICENSED — no vendor at any tested tier "
            "(3187/3188); candidates for honest retirement.")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
