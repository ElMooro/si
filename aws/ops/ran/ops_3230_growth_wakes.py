"""ops 3230 — the growth engines: pct4 (YoY over quarterly levels) ships,
and the one-symbol closes it unlocks, probe-gated:

  · ECONOMICS:DEGDPYY → DERIVED FRED~CLVMNACSCAB1GQDE~pct4 (Germany real
    GDP, Eurostat-family FRED — alive, unlike the OECD MEI mirrors).
    Wakes "Europe Growth" (needs 1).
  · ECONOMICS:FRGDPYY → same family for France. Wakes "France" (needs 1).
  · ECONOMICS:DEIFOE → one probe at the proven OECD KEI template with the
    BC (business confidence) measure — OECD's German BCI IS Ifo-based.
    If dry, "Euro Predict" stays open honestly.

Shared changed ⇒ redeploy the three consumers (schedule-type-safe, config
propagation awaited), fleet run, wakes by name vs the current index.
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

from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]
CONSUMERS = ("justhodl-wl-engines", "justhodl-thesis-engine",
             "justhodl-symbol-dictionary")
CURATE = {
    "ECONOMICS:DEGDPYY": [("DERIVED", "FRED~CLVMNACSCAB1GQDE~pct4",
                           "Germany real GDP YoY (pct4)")],
    "ECONOMICS:FRGDPYY": [("DERIVED", "FRED~CLVMNACSCAB1GQFR~pct4",
                           "France real GDP YoY (pct4)")],
    "ECONOMICS:DEIFOE": [("DBNOMICS",
                          "OECD/DSD_KEI@DF_KEI/DEU.M.BC.IX._T.AA._Z",
                          "Germany business confidence (OECD KEI, "
                          "Ifo-based)")],
}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3230_growth_wakes") as rep:
    fails, warns = [], []
    rep.heading("ops 3230 — pct4 ships; growth engines close")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}

    rep.section("1. Probe-gated curations")
    landed = 0
    for sym, cands in CURATE.items():
        if sym in mapped:
            rep.log(f"  = {sym} already mapped")
            continue
        for src, sid, note in cands:
            try:
                n = len(SS.fetch(src, sid))
            except Exception:
                n = 0
            rep.log(f"  {sym[:22]:<22} {sid[:44]:<44} {n} pts")
            if n >= 60:
                e = {"source": src, "id": sid, "confidence": 0.85,
                     "note": note + " (ops 3230)"}
                mapped[sym] = e
                curated[sym] = e
                landed += 1
                rep.ok(f"{sym} curated")
                break
    rep.kv(curations=landed)
    if not landed:
        fails.append("no curation landed — nothing to deploy against")

    if landed:
        rep.section("2. Write map + deploy consumers (shared changed)")
        wl = s3_json("data/tv-watchlists.json") or {}
        uniq = sorted({s.upper() for l in (wl.get("lists") or [])
                       if not str(l.get("id", "")).startswith("e2e-")
                       for s in (l.get("symbols") or [])})
        cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
        S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                      Body=json.dumps({**{k: prev.get(k) for k in
                                          ("licensed_econ",
                                           "usi_intraday_only", "dry",
                                           "retired", "search_cache")
                                          if k in prev},
                                       "generated_at":
                                           datetime.now(timezone.utc)
                                           .isoformat(),
                                       "coverage_pct": cov,
                                       "map": mapped, "curated": curated,
                                       "note": "ops 3230: growth wakes"}),
                      ContentType="application/json")
        rep.kv(coverage_now=cov)
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
                              source_dir=AWS_DIR / "lambdas" / fn
                              / "source",
                              env_vars=live, eb_rule_name=rule,
                              eb_schedule=cron,
                              timeout=cfg.get("timeout", 900),
                              memory=cfg.get("memory", 1024),
                              description=str(cfg.get("description",
                                                      ""))[:250],
                              smoke=False)
                LAM.get_waiter("function_updated_v2").wait(
                    FunctionName=fn,
                    WaiterConfig={"Delay": 2, "MaxAttempts": 30})
            except Exception as e:
                fails.append(f"deploy {fn}: {str(e)[:80]}")

        rep.section("3. Fleet run — wakes by name")
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
            for w in woken[:8]:
                nm = next((e.get("name") for e in eng2
                           if e.get("engine_id") == w), w)
                rep.log(f"  ⏰ WOKE: {nm}")
            if woken:
                rep.ok(f"{len(woken)} panels WOKEN")
            else:
                warns.append("no wakes — read the target engines' fresh "
                             "reasons next")
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
