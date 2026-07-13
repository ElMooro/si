"""ops 3215 — precision pass: every remaining blocker NAMED verbatim, the
certain wins curated, the dry members triaged.

3214 measured that broad machinery is exhausted: the residue is hard-class
everywhere probes can reach cheaply. What remains is surgical: several
near-wake engines are blocked on ONE symbol each, and two engines with 7
resolved members are dormant because those members probe DRY. This ops:

  1. Prints the VERBATIM blocking symbols per need<=1 engine — the
     hand-curation worklist, by name, no buckets.
  2. Micro e-mini roots (MES/MNQ/M2K/MYM/MGC/SIL/MCL) added — these are
     REAL Yahoo continuous contracts, not proxies. Remapped + probed.
  3. Dry-member triage for the two need-0 engines: each resolved member
     probed, the dry ones named with their source|id — that's tomorrow's
     fix list with evidence attached.
  4. Fleet re-run; wakes counted by name.
"""
import json
import re
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


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3215_name_blockers") as rep:
    fails, warns = [], []
    rep.heading("ops 3215 — blockers named, certain wins curated, dry "
                "members triaged")

    wl = s3_json("data/tv-watchlists.json") or {}
    lists = {str(l.get("id")): l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")}
    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    retired = dict(prev.get("retired") or {})
    idx = s3_json("data/wl-engines.json") or {}
    eng = idx.get("engines") or []
    prev_active = {e["engine_id"] for e in eng
                   if str(e.get("state")) == "ACTIVE"}

    # ── 1. verbatim blockers for need<=1 engines ──────────────────────
    rep.section("1. The one-symbol blockers, verbatim")
    for e in eng:
        if str(e.get("state")) != "DORMANT":
            continue
        res = int(e.get("members_resolved") or 0)
        if 6 - res > 1 or res >= 6:
            continue
        l = lists.get(str(e.get("tv_id"))) or {}
        un = [s.upper() for s in (l.get("symbols") or [])
              if s.upper() not in mapped and s.upper() not in retired]
        if un:
            rep.log(f"  {str(e.get('name'))[:36]:<36} needs 1 → "
                    + " | ".join(un[:4]))

    # ── 2. micro roots remap + probe ───────────────────────────────────
    rep.section("2. Micro e-mini roots (real Yahoo continuous)")
    uniq = sorted({s.upper() for l in lists.values()
                   for s in (l.get("symbols") or [])})
    new_m = {}
    for s in uniq:
        if s in mapped or s in retired:
            continue
        src, sid, conf, note = SS.map_symbol(s)
        if src == "MARKET" and "continuous future" in note \
                and sid.startswith(("MES", "MNQ", "M2K", "MYM", "MGC",
                                    "SIL", "MCL")):
            new_m[s] = {"source": src, "id": sid, "confidence": conf,
                        "note": note}
    probed_ok = 0
    for s, e in list(new_m.items()):
        try:
            if len(SS.fetch("MARKET", e["id"])) >= 150:
                mapped[s] = e
                curated[s] = e
                probed_ok += 1
                rep.log(f"    ✓ {s} → {e['id']}")
            else:
                new_m.pop(s)
        except Exception:
            new_m.pop(s)
    rep.kv(micro_candidates=len(new_m) + 0, micro_proven=probed_ok)

    # ── 3. dry-member triage (need-0 dormant engines) ─────────────────
    rep.section("3. Dry-member triage — WHY 7 resolved != 6 usable")
    tri = [e for e in eng if str(e.get("state")) == "DORMANT"
           and int(e.get("members_resolved") or 0) >= 6][:2]
    for e in tri:
        l = lists.get(str(e.get("tv_id"))) or {}
        members = [s.upper() for s in (l.get("symbols") or [])
                   if s.upper() in mapped][:10]
        rep.log(f"  ── {str(e.get('name'))[:44]}")

        def pr(sym):
            m = mapped[sym]
            try:
                return sym, m, len(SS.fetch(m["source"], m["id"]))
            except Exception:
                return sym, m, 0

        with ThreadPoolExecutor(max_workers=6) as ex:
            for sym, m, n in ex.map(pr, members):
                mark = "✓" if n >= 40 else "✗ DRY"
                rep.log(f"    {mark} {sym[:30]:<30} {m['source']}:"
                        f"{str(m['id'])[:34]:<34} {n} pts")

    # ── 4. write + fleet re-run: wakes by name ─────────────────────────
    rep.section("4. Fleet re-run")
    cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({**{k: prev.get(k) for k in
                                      ("licensed_econ", "usi_intraday_only",
                                       "dry", "search_cache")
                                      if k in prev},
                                   "generated_at":
                                       datetime.now(timezone.utc)
                                       .isoformat(),
                                   "coverage_pct": cov, "map": mapped,
                                   "curated": curated, "retired": retired,
                                   "note": "ops 3215: micro roots + "
                                           "blocker worklist"}),
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
        act2 = {e["engine_id"] for e in (idx2.get("engines") or [])
                if str(e.get("state")) == "ACTIVE"}
        woken = sorted(act2 - prev_active)
        rep.kv(active_now=len(act2), woken=len(woken))
        for w in woken[:8]:
            rep.log(f"  ⏰ WOKE: {w}")
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
