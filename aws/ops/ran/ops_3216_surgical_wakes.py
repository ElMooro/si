"""ops 3216 — surgical wakes: the named one-symbol blockers closed, the
named zero-point templates dry-ledgered.

3215 named everything. Closed here:
  · ECONOMICS:USRR → FRED RRPONTSYD (ON reverse repo) — blocks THREE
    engines by itself.
  · TVC:BTPBUND → DERIVED IT10Y − DE10Y spread (new two-base 'minus'
    transform) — blocks two.
  · Micro e-minis (3215) already proven — Developed Markets unblocks.
  · The 8 zero-point entries 3215 exposed (Chile FRED-OECD mirrors that
    do not exist, SHAZ, CLP crosses) enter the DRY ledger and leave the
    map — 'resolved' must mean fetchable.
Both curated adds are probe-verified before the fleet re-runs; wakes
reported by name against the pre-3215 baseline of 115 ACTIVE.
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
AWS_DIR = Path(__file__).resolve().parents[2]
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SHARED_CONSUMERS = ("justhodl-wl-engines", "justhodl-thesis-engine",
                    "justhodl-symbol-dictionary")
DRY_NAMED = {
    "NASDAQ:SHAZ": "MARKET|SHAZ",
    "ECONOMICS:CLGDPYY": "FRED|NAEXKP01CHLQ657S",
    "ECONOMICS:CLINTR": "FRED|IR3TIB01CHLM156N",
    "ECONOMICS:CLBOT": "FRED|XTNTVA01CHLM667S",
    "ECONOMICS:CLUR": "FRED|LRHUTTTTCHLM156S",
    "ECONOMICS:CLSP": "FRED|SPASTT01CHLM657N",
    "FX_IDC:CLPHKD": "MARKET|CLPHKD=X",
    "FX_IDC:CLPSGD": "MARKET|CLPSGD=X",
}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3216_surgical_wakes") as rep:
    fails, warns = [], []
    rep.heading("ops 3216 — named blockers closed, named dries ledgered, "
                "wakes counted")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    dry = dict(prev.get("dry") or {})
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}

    rep.section("1. Probe + curate the two blockers")
    for sym in ("ECONOMICS:USRR", "TVC:BTPBUND"):
        src, sid, conf, note = SS.map_symbol(sym)
        try:
            n = len(SS.fetch(src, sid))
        except Exception:
            n = 0
        if n >= 150:
            e = {"source": src, "id": sid, "confidence": conf,
                 "note": note + " (ops 3216)"}
            mapped[sym] = e
            curated[sym] = e
            rep.ok(f"{sym} → {sid[:52]}  ({n} pts)")
        else:
            fails.append(f"{sym} probed {n} pts — not curated")

    rep.section("2. Dry-ledger the named zero-point entries")
    pruned = 0
    for sym, sid in DRY_NAMED.items():
        sid = sid.replace("|", "" if False else "")
        cur = (mapped.get(sym) or {}).get("id")
        if cur:
            dry[sym] = cur
            mapped.pop(sym, None)
            curated.pop(sym, None)
            pruned += 1
            rep.log(f"  ✗ {sym} → dry ledger ({cur[:40]})")
    rep.kv(dry_ledgered=pruned)

    rep.section("3. Write + fleet — wakes by name")
    wl = s3_json("data/tv-watchlists.json") or {}
    uniq = sorted({s.upper() for l in (wl.get("lists") or [])
                   if not str(l.get("id", "")).startswith("e2e-")
                   for s in (l.get("symbols") or [])})
    cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({**{k: prev.get(k) for k in
                                      ("licensed_econ", "usi_intraday_only",
                                       "retired", "search_cache")
                                      if k in prev},
                                   "generated_at":
                                       datetime.now(timezone.utc)
                                       .isoformat(),
                                   "coverage_pct": cov, "map": mapped,
                                   "curated": curated, "dry": dry,
                                   "note": "ops 3216: surgical wakes"}),
                  ContentType="application/json")
    rep.kv(coverage_now=cov)
    for fn in SHARED_CONSUMERS:
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
        except Exception as e:
            fails.append(f"deploy {fn}: {str(e)[:80]}")
    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"invoke: {str(e)[:70]}")
    idx2 = None
    for _ in range(60):
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
               woken=len(woken))
        for w in woken[:10]:
            nm = next((e.get("name") for e in eng2
                       if e.get("engine_id") == w), w)
            rep.log(f"  ⏰ WOKE: {nm}")
        if woken:
            rep.ok(f"{len(woken)} panels WOKEN by the surgical closes")
        else:
            warns.append("no wakes — verify member thresholds next run")
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
