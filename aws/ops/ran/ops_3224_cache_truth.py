"""ops 3224 — the cache learns the map, the fetchers learn manners.

3223's trace named three mechanisms:
  1. SYMBOL-KEYED CACHE STALENESS: remapped symbols keep serving their
     old series (GBDIR: 9-pt WorldBank ghost) until the 6-day stamp.
     Fix: an ids-ledger — every cached symbol records what it was
     fetched AS; a mapping change now invalidates that entry alone.
  2. FETCH STORMS: 10 unthrottled workers blow CoinGecko (429→401
     lockout) and can trip FRED — ~30 crypto symbols re-fail EVERY run.
     Fix: per-source politeness gates in series_source (the one place).
  3. FAKE AGGREGATE IDS: TV CRYPTOCAP tiles (total2, btc.d, …) were
     mapped to CoinGecko ids that 404 — no free historical source
     exists. Retired honestly.
Plus: the named poisoned cache entry evicted, consumers redeployed with
config-propagation AWAITED (the 3221 race), fleet re-run, wakes by name.
"""
import gzip
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]
STATE_KEY = "data/thesis-state-v2.json.gz"
CONSUMERS = ("justhodl-wl-engines", "justhodl-thesis-engine",
             "justhodl-symbol-dictionary")
EVICT = ("ECONOMICS:GBDIR",)
AGG_ID = re.compile(r"^(total\w*|others(\.d)?|stable\.c\.d|c(\.d)?|"
                    r"btcshorts|[a-z0-9]+\.d)$")


def s3_json(key, default=None, gz=False):
    try:
        b = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if gz:
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return default


with report("3224_cache_truth") as rep:
    fails, warns = [], []
    rep.heading("ops 3224 — ids-ledger cache, polite fetchers, honest "
                "aggregates")

    rep.section("1. Evict poisoned cache entries + retire fake aggregates")
    st = s3_json(STATE_KEY, {}, gz=True) or {}
    cache = st.get("weekly") or {}
    ev = 0
    for sym in EVICT:
        if cache.pop(sym, None) is not None:
            ev += 1
    if ev:
        st["weekly"] = cache
        S3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                      Body=gzip.compress(json.dumps(st).encode()),
                      ContentType="application/json",
                      ContentEncoding="gzip")
    rep.kv(evicted=ev)
    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    retired = dict(prev.get("retired") or {})
    ret = 0
    for sym in list(mapped):
        m = mapped[sym]
        if m.get("source") == "COINGECKO" and AGG_ID.match(
                str(m.get("id", ""))):
            retired[sym] = "no_free_history_source_crypto_aggregate"
            mapped.pop(sym, None)
            curated.pop(sym, None)
            ret += 1
    rep.kv(aggregates_retired=ret)
    wl = s3_json("data/tv-watchlists.json") or {}
    uniq = sorted({s.upper() for l in (wl.get("lists") or [])
                   if not str(l.get("id", "")).startswith("e2e-")
                   for s in (l.get("symbols") or [])})
    cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({**{k: prev.get(k) for k in
                                      ("licensed_econ", "usi_intraday_only",
                                       "dry", "search_cache") if k in prev},
                                   "generated_at":
                                       datetime.now(timezone.utc)
                                       .isoformat(),
                                   "coverage_pct": cov, "map": mapped,
                                   "curated": curated, "retired": retired,
                                   "note": "ops 3224: cache truth"}),
                  ContentType="application/json")
    rep.kv(coverage_now=cov)

    rep.section("2. Deploy (config-propagation AWAITED)")
    for fn in CONSUMERS:
        try:
            cfg = {}
            p = AWS_DIR / "lambdas" / fn / "config.json"
            if p.exists():
                cfg = json.loads(p.read_text())
            sch = cfg.get("schedule")
            rule, cron = (sch.get("rule_name"), sch.get("cron")) \
                if isinstance(sch, dict) else (None, None)
            live = (LAM.get_function_configuration(FunctionName=fn)
                    .get("Environment") or {}).get("Variables") or {}
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=AWS_DIR / "lambdas" / fn / "source",
                          env_vars=live, eb_rule_name=rule,
                          eb_schedule=cron,
                          timeout=cfg.get("timeout", 900),
                          memory=cfg.get("memory", 1024),
                          description=str(cfg.get("description", ""))[:250],
                          smoke=False)
            LAM.get_waiter("function_updated_v2").wait(
                FunctionName=fn,
                WaiterConfig={"Delay": 2, "MaxAttempts": 30})
        except Exception as e:
            fails.append(f"deploy {fn}: {str(e)[:80]}")

    rep.section("3. Fleet run — wakes by name")
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}
    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"invoke: {str(e)[:70]}")
    idx2 = None
    for _ in range(75):
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
        rep.kv(active_before=len(prev_active), active_now=len(act2),
               woken=len(woken),
               series_cached=idx2.get("series_cached"))
        for w in woken[:10]:
            nm = next((e.get("name") for e in eng2
                       if e.get("engine_id") == w), w)
            rep.log(f"  ⏰ WOKE: {nm}")
        for nm in ("Europe Liquidity", "Global Deposit Rates"):
            e = next((x for x in eng2
                      if nm.lower() in str(x.get("name", "")).lower()),
                     None)
            if e:
                rep.log(f"  → {str(e.get('name'))[:36]:<36} "
                        f"{e.get('state')} "
                        f"resolved={e.get('members_resolved')}")
        if woken:
            rep.ok(f"{len(woken)} panels WOKEN")
    else:
        warns.append("index not fresh — throttled first fill can exceed "
                     "the window; next run completes it")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
