"""ops_4081 — did the wall lift on backoff, and what is in a macro payload?

Two questions, both answerable from telemetry v1.8.0 ships:

  Q1 THE WALL. v1.7.8 finished with sc 591 ok / 9,568 err after being
     walled at roughly 600 requests. If AIMD is working, the error rate
     collapses and delay_ms settles somewhere finite. wall_events counts
     circuit-breaker trips; recoveries counts resumes. A high sc_ok with
     wall_events 0 means the starting rate was already under the ceiling.
     A trip followed by clean requests means the wall LIFTS on backoff —
     which is the question Khalid asked.

  Q2 THE PAYLOAD. econ_probe carries the real key list and a sample for
     the first clean ECONOMICS/FRED responses. This is the difference
     between "TradingView exposes no publisher for macro symbols" and
     "the publisher is in a field we never read" — the two hypotheses
     ops 4079 could not separate from a zero.
"""
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def gj(k, d=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception:
        return d


def main():
    with report("4081_backoff_readout") as rep:
        rep.heading("ops 4081 — AIMD readout: wall + macro payload")

        sr = gj("data/tv-sources.json", {}) or {}
        d = sr.get("last_harvest_diag") or {}
        srcs = sr.get("sources") or {}
        gen = sr.get("generated_at")

        rep.section("A. is the new build even running?")
        v18 = [k for k in ("delay_ms", "wall_events", "recoveries",
                           "econ_probe") if k in d]
        rep.log(f"  v1.8.0-only fields present: {v18 or 'NONE'}")
        try:
            age = (datetime.now(timezone.utc)
                   - datetime.fromisoformat(str(gen))).total_seconds() / 60
            rep.log(f"  sync age: {age:.1f} min")
        except Exception:
            age = None
        running = len(v18) >= 3
        if not running:
            rep.log("  ✗ no v1.8.0 telemetry yet — reload may not have "
                    "happened, or no sync has landed since (T+75s on a TV "
                    "tab, then every 15 min). Nothing to judge yet.")
        rep.kv(v180_running=running, sourced=len(srcs),
               age_min=(round(age, 1) if age else None))

        rep.section("B. Q1 — THE WALL")
        ok, err = d.get("sc_ok") or 0, d.get("sc_err") or 0
        tot = ok + err
        rep.log(f"  scanner   : {ok} ok / {err} err"
                + (f"  ({ok / tot * 100:.1f}% success)" if tot else ""))
        rep.log(f"  walked    : {d.get('done')}/{d.get('total')}")
        rep.log(f"  delay_ms  : {d.get('delay_ms')}   "
                f"max_delay {d.get('max_delay')}")
        rep.log(f"  wall trips: {d.get('wall_events')}   "
                f"recoveries {d.get('recoveries')}   "
                f"paused {d.get('paused_s')}s")
        rep.log(f"  streak_err: {d.get('streak_err')}")
        rep.log(f"  rate      : {d.get('rate_per_min')}/min  "
                f"elapsed {d.get('elapsed_s')}s")
        rep.log(f"  matched   : {d.get('matched')}")
        prev = 591 / 10159 * 100
        if tot:
            rep.log(f"  → v1.7.8 baseline was {prev:.1f}% success; "
                    f"this pass is {ok / tot * 100:.1f}%")
        rep.kv(sc_ok=ok, sc_err=err,
               success_pct=(round(ok / tot * 100, 1) if tot else None),
               delay_ms=d.get("delay_ms"), wall_events=d.get("wall_events"),
               recoveries=d.get("recoveries"))

        rep.section("B2. verdict on the wall")
        if not tot:
            rep.log("  no requests recorded yet")
        elif (d.get("wall_events") or 0) > 0 and (d.get("recoveries") or 0) > 0:
            rep.log("  ✓ WALL LIFTS ON BACKOFF — the breaker tripped and "
                    "clean requests resumed after the pause. Rate-limited, "
                    "not banned.")
        elif ok / tot > 0.8:
            rep.log("  ✓ no wall at this rate — the starting delay is under "
                    "TradingView's ceiling. The 94% failure was self-inflicted "
                    "by the 240ms step, confirmed.")
        elif ok / tot < 0.3:
            rep.log("  ✗ still being refused even at backoff — this looks "
                    "like a longer-lived block, not a burst limiter. Would "
                    "need a cooldown of hours, not minutes.")
        else:
            rep.log("  ◐ partial — still converging, re-read next sync")

        rep.section("C. Q2 — WHAT A MACRO PAYLOAD ACTUALLY CONTAINS")
        probe = d.get("econ_probe") or []
        if not probe:
            rep.log("  no ECONOMICS/FRED symbol has returned a CLEAN response "
                    "yet — the probe only fires on success, so this is empty "
                    "either because the pass has not reached them or because "
                    "they are still being refused.")
        for e in probe:
            rep.log(f"  ── {e.get('sym')}")
            rep.log(f"     keys  : {e.get('keys')}")
            rep.log(f"     sample: {str(e.get('sample'))[:380]}")
        if probe:
            allk = Counter()
            for e in probe:
                for k in (e.get("keys") or []):
                    allk[k] += 1
            rep.log(f"  union of keys seen: {sorted(allk)}")
            srcish = [k for k in allk if "source" in k.lower()
                      or "publisher" in k.lower() or "provider" in k.lower()]
            if srcish:
                rep.log(f"  ✓ publisher-ish fields DO exist: {srcish} — "
                        f"extraction can be fixed, the data is there")
            else:
                rep.log("  ✗ no source/publisher/provider field in any macro "
                        "payload — TradingView does not expose it on this "
                        "route. Attribution must come from elsewhere "
                        "(gov-sources registry join, not the harvester).")
        rep.kv(econ_probe_n=len(probe))

        rep.section("D. current attribution state")
        econ = [k for k in srcs if k.upper().startswith(("ECONOMICS:", "FRED:"))]
        rep.log(f"  ECONOMICS/FRED sourced: {len(econ)}")
        pref = Counter(s.split(":")[0].upper() for s in srcs if ":" in s)
        rep.log("  top prefixes: " + ", ".join(f"{p} {n}"
                                               for p, n in pref.most_common(8)))
        try:
            r = lam.invoke(FunctionName="justhodl-source-map",
                           InvocationType="RequestResponse",
                           Payload=b'{"source":"ops4081"}')
            rep.log(f"  rollup: {r['Payload'].read().decode()[:200]}")
        except Exception as e:
            rep.log(f"  rollup invoke failed: {str(e)[:80]}")

        rep.section("VERDICT")
        if not running:
            rep.log("○ v1.8.0 not observed yet — no judgement possible.")
        else:
            rep.log("readout above; no gate failure — this op reports truth, "
                    "it does not assert an outcome.")


if __name__ == "__main__":
    main()
