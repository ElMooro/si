"""ops 3193 — on-chain derivables: vendor tiles that are TRANSFORMS of
free primaries get COMPUTED, not bought.

3189 proved most GLASSNODE/INTOTHEBLOCK tiles are not raw metrics but
composites (ATHDRAWDOWN, NUPL...). New DERIVED source in series_source.py:
'SRC~ID~transform' — drawdown-from-ATH / running-max / NUPL-from-MVRV /
negate / pct-change over any base series the ladder can fetch. Real data
in, math out. Tiles that are genuinely proprietary (BULLSCOUNT, holder
composition) stay unmapped and are CENSUSED here by token frequency so any
future curated pass has evidence, not guesses.
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
MIN_POINTS = 200


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3193_onchain_derived") as rep:
    fails, warns = [], []
    rep.heading("ops 3193 — DERIVED on-chain composites (computed from "
                "free primaries)")

    wl = s3_json("data/tv-watchlists.json") or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    uniq = sorted({s.upper() for l in lists for s in (l.get("symbols") or [])})
    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    cache = prev.get("search_cache") or {}
    prev_cov = float(prev.get("coverage_pct") or 0)

    rep.section("1. Token census of the unmapped on-chain tiles")
    left = [s for s in uniq if s not in mapped
            and s.split(":", 1)[0] in ("GLASSNODE", "INTOTHEBLOCK")]
    toks = Counter()
    for s in left:
        t = s.split(":", 1)[1]
        toks[t.partition("_")[2].replace("_", "")[:26] or t] += 1
    rep.kv(onchain_unmapped=len(left))
    for tk, n in toks.most_common(20):
        rep.log(f"  {tk:<28} {n:>3}")

    rep.section("2. Re-map + probe the DERIVED entries")
    for s in uniq:
        if s in curated:
            mapped.setdefault(s, curated[s])
            continue
        if s in mapped:
            continue
        src, sid, conf, note = SS.map_symbol(s)
        if src and src != "EODHD":
            mapped[s] = {"source": src, "id": sid, "confidence": conf,
                         "note": note}
    new_dv = {s: m for s, m in mapped.items()
              if m["source"] == "DERIVED" and s not in (prev.get("map") or {})}
    rep.kv(new_derived=len(new_dv))

    def probe(item):
        sym, m = item
        try:
            return sym, len(SS.fetch(m["source"], m["id"]))
        except Exception:
            return sym, 0

    hits, dry, t0 = {}, [], time.time()
    with ThreadPoolExecutor(max_workers=6) as ex:
        for f in as_completed([ex.submit(probe, it) for it in new_dv.items()]):
            if time.time() - t0 > 240:
                break
            sym, n = f.result()
            (hits.__setitem__(sym, n) if n >= MIN_POINTS else dry.append(sym))
    for sym in dry:
        mapped.pop(sym, None)
    for sym in hits:
        curated[sym] = mapped[sym]
    for sym, n in sorted(hits.items(), key=lambda kv: -kv[1])[:6]:
        rep.log(f"    ✓ {sym} → {mapped[sym]['id']}  ({n} pts)")
    rep.kv(probed=len(new_dv), survivors=len(hits), pruned=len(dry))
    if new_dv and not hits:
        warns.append("all DERIVED entries probed dry — check transform "
                     "recursion in the runner env")

    rep.section("3. Write + redeploy + kick")
    cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({
                      "generated_at": datetime.now(timezone.utc).isoformat(),
                      "coverage_pct": cov, "map": mapped,
                      "curated": curated, "search_cache": cache,
                      "licensed_econ": prev.get("licensed_econ") or [],
                      "retired": prev.get("retired") or {},
                      "note": ("ops 3193: DERIVED transforms live; curated "
                               "MERGES FIRST on rebuild")}),
                  ContentType="application/json")
    rep.kv(coverage_before=prev_cov, coverage_now=cov,
           curated_total=len(curated))
    if cov < prev_cov - 0.05:
        fails.append(f"coverage regressed {prev_cov} → {cov}")
    for fn in SHARED_CONSUMERS:
        try:
            cfg = {}
            p = AWS_DIR / "lambdas" / fn / "config.json"
            if p.exists():
                cfg = json.loads(p.read_text())
            live = (LAM.get_function_configuration(FunctionName=fn)
                    .get("Environment") or {}).get("Variables") or {}
            sch = cfg.get("schedule") or {}
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=AWS_DIR / "lambdas" / fn / "source",
                          env_vars=live, eb_rule_name=sch.get("rule_name"),
                          eb_schedule=sch.get("cron"),
                          timeout=cfg.get("timeout", 900),
                          memory=cfg.get("memory", 1024),
                          description=str(cfg.get("description", ""))[:250],
                          smoke=False)
        except Exception as e:
            fails.append(f"deploy {fn}: {str(e)[:90]}")
    try:
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        warns.append(f"fleet kick: {str(e)[:70]}")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
