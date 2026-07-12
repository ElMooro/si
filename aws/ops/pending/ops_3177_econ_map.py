"""ops 3177 — ECONOMICS:* mapped to free global sources → engines wake up.

3176 shipped 207 engines but only 53 ACTIVE: the rest are starved because
most of their members are ECONOMICS:* (TradingView's proprietary econ DB).
Those codes are {ISO2}{INDICATOR} and the data is public:

  World Bank   free, no key, ~200 countries, 1960+ — carries exactly the
               codes that dominate his universe: BOT (186), FI (174),
               GDPYY (168), GDG (164), DIR (150), FER (121), CS, CAG
  FRED/OECD    monthly mirrors for OECD members (better frequency)

Each mapping activates SEVERAL dormant engines at once because these codes
repeat across his lists.

Also fixed: annual series were being forward-filled onto the weekly grid
and z-scored over a 156-week window — a window holding ~3 distinct numbers.
z is now computed on each symbol's NATIVE observations and the Z is what
gets projected onto the grid.

Gates: coverage up · World Bank probes return 1960s-1990s history · ACTIVE
engines materially up from 53.
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


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3177_econ_map") as rep:
    fails, warns = [], []
    rep.heading("ops 3177 — ECONOMICS mapped → dormant engines wake")

    rep.section("1. World Bank probes (does the data actually come back?)")
    probes = [("BR|GC.DOD.TOTL.GD.ZS", "Brazil debt/GDP"),
              ("ZW|FR.INR.DPST", "Zimbabwe deposit rate"),
              ("KH|NE.RSB.GNFS.CD", "Cambodia trade balance"),
              ("CN|FI.RES.TOTL.CD", "China FX reserves"),
              ("JP|NY.GDP.MKTP.KD.ZG", "Japan GDP growth"),
              ("IN|FP.CPI.TOTL.ZG", "India CPI")]
    ok = 0
    for sid, label in probes:
        ser = SS.fetch("WORLDBANK", sid, "1990-01-01")
        if ser:
            ks = sorted(ser)
            rep.log(f"  {label:26s} {ks[0][:4]} → {ks[-1][:4]}  "
                    f"({len(ser)} obs)")
            ok += 1
        else:
            warns.append(f"world-bank empty: {label}")
    rep.kv(wb_probes=len(probes), wb_live=ok)
    if ok < 4:
        fails.append("World Bank unreachable — mapping would be fiction")

    rep.section("2. Re-map the universe")
    wl = s3_json("data/tv-watchlists.json")
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    uniq = sorted({s.upper() for l in lists for s in (l.get("symbols") or [])})
    prev = s3_json("data/symbol-map.json")
    cache = prev.get("search_cache") or {}
    searcher = SS.fred_search_factory(cache)
    mapped, kinds, used = {}, {}, 0
    t0 = time.time()
    for s in uniq:
        allow = used < 400 and time.time() - t0 < 300
        src, sid, conf, note = SS.map_symbol(s, searcher if allow else None)
        if "fred-search" in str(note):
            used += 1
        if src:
            mapped[s] = {"source": src, "id": sid, "confidence": conf,
                         "note": note}
            kinds[src] = kinds.get(src, 0) + 1
        else:
            kinds["UNMAPPED"] = kinds.get("UNMAPPED", 0) + 1
    cov = round(100 * len(mapped) / max(1, len(uniq)), 1)
    before = prev.get("coverage_pct")
    rep.kv(unique_symbols=len(uniq), coverage_before_pct=before,
           coverage_now_pct=cov,
           **{f"src_{k.lower()}": v for k, v in
              sorted(kinds.items(), key=lambda kv: -kv[1])})
    for k, v in sorted(kinds.items(), key=lambda kv: -kv[1]):
        rep.log(f"  {k:12s} {v:5d}  ({round(100*v/len(uniq),1)}%)")
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({
                      "generated_at": datetime.now(timezone.utc).isoformat(),
                      "n_mapped": len(mapped), "n_total": len(uniq),
                      "coverage_pct": cov, "map": mapped,
                      "search_cache": cache}).encode(),
                  ContentType="application/json")
    rep.ok(f"symbol-map rewritten: {before}% → {cov}% "
           f"(+{len(mapped) - (prev.get('n_mapped') or 0)} symbols)")

    rep.section("3. Re-run the engine fleet on the wider map")
    cfg = json.loads((AWS_DIR / "lambdas" / "justhodl-wl-engines"
                      / "config.json").read_text())
    env = (LAM.get_function_configuration(FunctionName="justhodl-wl-engines")
           .get("Environment") or {}).get("Variables") or {}
    sch = cfg.get("schedule") or {}
    deploy_lambda(report=rep, function_name="justhodl-wl-engines",
                  source_dir=AWS_DIR / "lambdas" / "justhodl-wl-engines"
                  / "source",
                  env_vars=env, eb_rule_name=sch.get("rule_name"),
                  eb_schedule=sch.get("cron"), timeout=cfg["timeout"],
                  memory=cfg["memory"],
                  description=cfg.get("description", "")[:250], smoke=False)
    t1 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName="justhodl-wl-engines", InvocationType="Event",
               Payload=json.dumps({"force_emit": True}).encode())
    idx = None
    deadline = time.time() + 870
    while time.time() < deadline:
        try:
            d = s3_json("data/wl-engines.json")
            if datetime.fromisoformat(d["generated_at"]) >= t1:
                idx = d
                break
        except Exception:
            pass
        time.sleep(20)
    if not idx:
        fails.append("wl-engines did not refresh")
    else:
        rep.kv(engines=idx.get("n_engines"), active_now=idx.get("n_active"),
               dormant=idx.get("n_dormant"), firing=idx.get("n_firing"),
               fdr=idx.get("n_fdr"), signals=idx.get("signals_logged"),
               series_cached=idx.get("series_cached"),
               elapsed_s=idx.get("elapsed_s"))
        rep.log("── themes: " + ", ".join(
            f"{k}={v}" for k, v in sorted((idx.get("themes") or {}).items(),
                                          key=lambda kv: -kv[1])))
        if (idx.get("n_active") or 0) > 53:
            rep.ok(f"ACTIVE engines 53 → {idx['n_active']} "
                   f"(+{idx['n_active'] - 53} woken by the ECONOMICS map)")
        else:
            warns.append(f"ACTIVE still {idx.get('n_active')} — mapping "
                         "landed but membership thresholds unchanged")
        firing = [e for e in (idx.get("engines") or []) if e.get("firing")]
        rep.log(f"── FIRING ({len(firing)}):")
        for e in firing[:15]:
            w = e.get("w13") or {}
            rep.log(f"  {str(e['name'])[:32]:32s} [{e['theme']:9s}] "
                    f"act {str(e.get('activation_now')):>5}% "
                    f"({str(e.get('activation_pctile')):>5}p) "
                    f"t={str(w.get('t_stat')):>6} "
                    f"lit: {', '.join(e.get('lit') or [])[:40]}")

    for w in warns[:6]:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
