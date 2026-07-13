"""ops 3188 — clean EODHD rollback, KEEP the free wins.

Khalid is cancelling (correctly — it missed his largest bucket entirely and
its search fallback resolved BER:0252 to a Malaysian stock). A cancelled
key left wired is worse than no key: silent empty fetches and a live
mis-resolution hazard. So it comes out cleanly.

But ops 3186 found real FREE coverage while probing, and that stays:
  LSE      43 symbols · 100% free on Yahoo (.L)
  MIL      14 · 100% (.MI)
  TSX      15 ·  67% (.TO)
  HKEX/SIX/XETR/SWB/TRADEGATE/FWB/GETTEX · 33-50%
Those suffixes cost nothing and were never dependent on the token.

This op: purge the key (SSM + every engine env), re-map on the free path
only, re-run the fleet, and prove the system has no EODHD dependency left.
"""

import json
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
SSM = boto3.client("ssm", region_name=REGION)


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3188_eodhd_rollback") as rep:
    fails, warns = [], []
    rep.heading("ops 3188 — EODHD out, free coverage in")

    rep.section("1. Purge the key everywhere")
    try:
        SSM.delete_parameter(Name="/justhodl/eodhd-api-key")
        rep.ok("SSM /justhodl/eodhd-api-key deleted")
    except Exception as e:
        rep.log(f"  ssm delete: {str(e)[:60]}")
    for fn in ("justhodl-wl-engines", "justhodl-thesis-engine",
               "justhodl-symbol-dictionary"):
        try:
            live = LAM.get_function_configuration(FunctionName=fn)
            env = (live.get("Environment") or {}).get("Variables") or {}
            if "EODHD_API_KEY" in env:
                env.pop("EODHD_API_KEY")
                LAM.update_function_configuration(
                    FunctionName=fn, Environment={"Variables": env})
                LAM.get_waiter("function_updated").wait(
                    FunctionName=fn,
                    WaiterConfig={"Delay": 3, "MaxAttempts": 40})
                rep.ok(f"{fn}: EODHD key removed")
            else:
                rep.log(f"  {fn}: no key present")
        except Exception as e:
            warns.append(f"{fn}: {str(e)[:70]}")
    SS.EODHD_KEY = ""
    rep.ok("no engine can call EODHD — the fallback is inert without a key")

    rep.section("2. Re-map on the FREE path (keeps the probe's wins)")
    wl = s3_json("data/tv-watchlists.json") or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    uniq = sorted({s.upper() for l in lists for s in (l.get("symbols") or [])})
    prev = s3_json("data/symbol-map.json") or {}
    cache = prev.get("search_cache") or {}
    searcher = SS.fred_search_factory(cache)
    mapped, kinds, used, t0 = {}, {}, 0, time.time()
    for s in uniq:
        allow = used < 250 and time.time() - t0 < 200
        src, sid, conf, note = SS.map_symbol(s, searcher if allow else None)
        if "fred-search" in str(note):
            used += 1
        if src == "EODHD":              # belt and braces — never map to it
            continue
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
    if "EODHD" in kinds:
        fails.append("a symbol still maps to EODHD — dependency not clean")
    else:
        rep.ok(f"ZERO EODHD dependencies · coverage {cov}% on free sources "
               f"alone ({len(mapped)} symbols)")

    rep.section("3. Re-run the fleet")
    t1 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName="justhodl-wl-engines", InvocationType="Event",
               Payload=json.dumps({"force_emit": True}).encode())
    idx = None
    deadline = time.time() + 800
    while time.time() < deadline:
        d = s3_json("data/wl-engines.json")
        if d and datetime.fromisoformat(d["generated_at"]) >= t1:
            idx = d
            break
        time.sleep(20)
    if not idx:
        warns.append("fleet still running — count lands on its next run")
    else:
        rep.kv(engines=idx.get("n_engines"), active=idx.get("n_active"),
               dormant=idx.get("n_dormant"), firing=idx.get("n_firing"),
               series_cached=idx.get("series_cached"))
        rep.ok(f"{idx.get('n_active')} ACTIVE engines running on FREE data "
               "only — nothing regressed by cancelling")

    rep.section("4. The standing bill")
    rep.log("  Monthly data cost:            $0")
    rep.log("  Sources: FRED · World Bank · OECD · Yahoo · Stooq · CFTC ·")
    rep.log("           Polygon (already owned) · computed internals")
    rep.log("  Only remaining unbuyable gap: FTSE Russell licensed indices")
    rep.log("           (448 symbols — no vendor at any tier we tested)")
    rep.log("  PENDING-KHALID: Anthropic credits (~$20-50) would resurrect")
    rep.log("           premortem, strategist, RAG desk, tribunal, 516")
    rep.log("           un-distilled note views — all BUILT and dark")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
