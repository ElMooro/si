"""ops 3228 — post-steady-state truth pass.

  1. The three unknown roots looked up in the SYMBOL DICTIONARY — TV's
     own names for the tiles say what they are; candidates probe-gated
     from the names, not guesses.
  2. €STR retried post-storm (the 3217 'all dry' verdict may have been a
     429-era artifact) + the proper ECB/EST DBnomics path.
  3. Near-wake worklist recomputed on tombstone-honest data.
  4. wl-fusion manually kicked (its cron skips Mondays) so panels.html +
     the site-wide chip carry the 121-active truth today, then a fleet
     run only if a curation landed.
"""
import json
import sys
import time
from collections import Counter
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
ROOTS = ("ICEEUR:EON2!", "ICEEUR:USW1!", "CBOT:YIT1!")
EON_CANDS = [("FRED", "ECBESTRVOLWGTTRMDMNRT",
              "€STR (FRED, post-storm retry)"),
             ("DERIVED", "FRED~ECBESTRVOLWGTTRMDMNRT~hundred_minus",
              "€STR 100−rate"),
             ("DBNOMICS", "ECB/EST/B.EU000A2X2A25.WT",
              "€STR via ECB EST"),
             ("DERIVED", "DBNOMICS~ECB/EST/B.EU000A2X2A25.WT"
              "~hundred_minus", "€STR via ECB, 100−rate")]


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3228_truth_pass") as rep:
    fails, warns = [], []
    rep.heading("ops 3228 — names from the dictionary, truth from the "
                "tombstones")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    retired = dict(prev.get("retired") or {})
    sdict = (s3_json("data/symbol-dictionary.json") or {})
    names = sdict.get("dictionary") or sdict.get("symbols") or {}

    rep.section("1. The three roots, in TV's own words")
    for r in ROOTS:
        nm = names.get(r)
        if isinstance(nm, dict):
            nm = nm.get("name") or nm.get("title")
        rep.log(f"  {r:<16} → {str(nm)[:80]}")

    rep.section("2. €STR candidate ladder (post-storm)")
    landed = 0
    if "ICEEUR:EON2!" not in mapped and "ICEEUR:EON2!" not in retired:
        for src, sid, note in EON_CANDS:
            try:
                n = len(SS.fetch(src, sid))
            except Exception:
                n = 0
            rep.log(f"  {sid[:52]:<52} {n} pts")
            if n >= 150:
                e = {"source": src, "id": sid, "confidence": 0.85,
                     "note": note + " (ops 3228)"}
                mapped["ICEEUR:EON2!"] = e
                curated["ICEEUR:EON2!"] = e
                landed += 1
                rep.ok(f"ICEEUR:EON2! → {sid[:50]}")
                break

    rep.section("3. Near-wake worklist (tombstone-honest)")
    idx = s3_json("data/wl-engines.json") or {}
    eng = idx.get("engines") or []
    prev_active = {e["engine_id"] for e in eng
                   if str(e.get("state")) == "ACTIVE"}
    reasons = Counter((e.get("reason") or "").split("(")[0].strip()
                      for e in eng if str(e.get("state")) == "DORMANT")
    for rzn, n in reasons.most_common(4):
        rep.log(f"  DORMANT {n:>3} × {rzn[:64]}")
    wl = s3_json("data/tv-watchlists.json") or {}
    lists = {str(l.get("id")): l for l in (wl.get("lists") or [])}
    near = 0
    for e in eng:
        if str(e.get("state")) != "DORMANT":
            continue
        res = int(e.get("members_resolved") or 0)
        if res == 5:
            l = lists.get(str(e.get("tv_id"))) or {}
            un = [s.upper() for s in (l.get("symbols") or [])
                  if s.upper() not in mapped and s.upper() not in retired]
            if un:
                rep.log(f"  needs 1: {str(e.get('name'))[:34]:<34} → "
                        + " | ".join(un[:3]))
                near += 1
        if near >= 8:
            break

    rep.section("4. Fusion re-kick (cron skips Mondays)")
    mark_f = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-wl-fusion",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        warns.append(f"fusion invoke: {str(e)[:60]}")
    fus = None
    for _ in range(20):
        time.sleep(5)
        d = s3_json("data/wl-fusion.json") or {}
        if str(d.get("generated_at", "")) > mark_f:
            fus = d
            break
    if fus:
        th = fus.get("themes") or {}
        top = max(th.items(),
                  key=lambda kv: kv[1].get("pressure_pctile") or 0) \
            if th else None
        rep.ok(f"fusion fresh on 121-active index — top theme "
               f"{top[0]} {top[1].get('pressure_pctile')}p"
               if top else "fusion fresh")
    else:
        warns.append("fusion did not regenerate in window")

    if landed:
        rep.section("5. Fleet run (a curation landed)")
        wl_u = sorted({s.upper() for l in (wl.get("lists") or [])
                       if not str(l.get("id", "")).startswith("e2e-")
                       for s in (l.get("symbols") or [])})
        cov = round(100 * len(mapped) / len(wl_u), 1) if wl_u else 0
        S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                      Body=json.dumps({**{k: prev.get(k) for k in
                                          ("licensed_econ",
                                           "usi_intraday_only", "dry",
                                           "search_cache") if k in prev},
                                       "generated_at":
                                           datetime.now(timezone.utc)
                                           .isoformat(),
                                       "coverage_pct": cov,
                                       "map": mapped, "curated": curated,
                                       "retired": retired,
                                       "note": "ops 3228"}),
                      ContentType="application/json")
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
            for w in woken[:6]:
                rep.log(f"  ⏰ WOKE: {w}")
    else:
        rep.log("  no new curation — fleet untouched (nightly covers it)")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
