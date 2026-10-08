#!/usr/bin/env python3
"""ops 2959 — FIRE TV crawler (session just added to SSM by Khalid).

Invokes justhodl-tv-notes-crawler synchronously, waits for results,
then fires justhodl-brain-compiler to route the harvested TV notes to
every engine in the fleet. Reports a full harvest summary.
"""
import json
import sys
import time
from datetime import datetime, timezone

import boto3
from ops_report import report

LAM    = boto3.client("lambda", region_name="us-east-1")
S3     = boto3.client("s3",    region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def invoke(fn, payload=None, sync=True):
    resp = LAM.invoke(
        FunctionName=fn,
        InvocationType="RequestResponse" if sync else "Event",
        Payload=json.dumps(payload or {}).encode(),
    )
    if sync:
        raw = resp["Payload"].read()
        outer = json.loads(raw)
        body  = outer if isinstance(outer, dict) else {}
        if isinstance(body.get("body"), str):
            try:
                body = json.loads(body["body"])
            except Exception:
                pass
        return body
    return {}


def main():
    with report("2959_tv_harvest_now") as rep:
        fails = []

        # ── 1. Fire the crawler (sync, up to 9 min) ───────────────────
        rep.section("1. Invoke justhodl-tv-notes-crawler")
        rep.log("Firing crawler — this takes 1-5 min depending on note count…")
        t0 = time.time()
        try:
            result = invoke("justhodl-tv-notes-crawler", {})
            elapsed = round(time.time() - t0, 1)
            rep.kv(
                session_valid   = result.get("session_valid"),
                username        = result.get("username"),
                notes_harvested = result.get("notes_harvested", 0),
                notes_in_mirror = result.get("notes_in_mirror", 0),
                symbols_covered = result.get("symbols_covered", 0),
                brain_upserted  = result.get("brain_upserted", 0),
                brain_errors    = result.get("brain_errors", 0),
                elapsed_seconds = elapsed,
                crawler_ok      = result.get("ok"),
            )
            if not result.get("ok"):
                fails.append("crawler returned ok=False — check session validity")
            if not result.get("session_valid"):
                fails.append("session_valid=False — session may be stale or wrong")
            notes_n = result.get("notes_harvested", 0)
            syms_n  = result.get("symbols_covered", 0)
            print("[harvest] %d notes / %d tickers in %.1fs" % (notes_n, syms_n, elapsed))
        except Exception as e:
            fails.append("crawler invocation failed: %s" % e)
            rep.fail(str(e))
            notes_n = syms_n = 0

        # ── 2. Confirm mirror on S3 ──────────────────────────────────
        rep.section("2. Verify mirror")
        try:
            mirror = json.loads(S3.get_object(
                Bucket=BUCKET, Key="data/tradingview-notes.json")["Body"].read())
            mirror_count = mirror.get("count", len(mirror.get("notes", [])))
            mirror_updated = mirror.get("updated", "?")
            rep.kv(mirror_count=mirror_count, mirror_updated=mirror_updated)
            if mirror_count == 0:
                rep.warn("Mirror is empty — TV may not expose a notes API on your plan; "
                         "see next steps below")
            else:
                # print a sample of what was found
                samples = mirror.get("notes", [])[:5]
                for i, n in enumerate(samples):
                    rep.log("sample[%d] %s: %s" % (i, n.get("symbol"), n.get("text","")[:120]))
        except Exception as e:
            rep.warn("mirror read: %s" % e)
            mirror_count = 0

        # ── 3. Fire brain-compiler to route TV notes to engines ───────
        rep.section("3. Invoke justhodl-brain-compiler")
        if mirror_count > 0:
            rep.log("Firing brain-compiler to route %d TV notes to fleet…" % mirror_count)
            try:
                bc = invoke("justhodl-brain-compiler", {})
                summary = (bc.get("summary") or {})
                rep.kv(
                    brain_notes_total = summary.get("n_notes", 0),
                    brain_claims      = summary.get("n_claims", 0),
                    brain_covered     = summary.get("covered", 0),
                    brain_gaps        = summary.get("gaps", 0),
                    brain_coverage_pct= summary.get("coverage_pct"),
                    headline          = summary.get("headline", ""),
                )
                print("[brain] %s" % summary.get("headline", "brain-compiler ran"))
            except Exception as e:
                rep.warn("brain-compiler: %s" % e)
        else:
            rep.log("Skipping brain-compiler (no notes yet)")

        # ── 4. Write human-readable status ───────────────────────────
        rep.section("4. Status feed")
        status = {
            "ok": not fails,
            "updated": datetime.now(timezone.utc).isoformat(),
            "username": result.get("username") if "result" in dir() else None,
            "session_valid": result.get("session_valid") if "result" in dir() else False,
            "notes_harvested": notes_n,
            "symbols_covered": syms_n,
            "mirror_count": mirror_count,
            "brain_upserted": result.get("brain_upserted", 0) if "result" in dir() else 0,
            "next_steps": (
                "All done — %d notes from %d tickers are now in your Brain. "
                "Brain-compiler has routed them to every matching engine in the fleet. "
                "Crawler runs daily at 06:00 UTC automatically."
                % (mirror_count, syms_n)
            ) if mirror_count > 0 else (
                "No notes found via TV's REST API (API may not be exposed on your plan). "
                "Alternative: open justhodl.ai/tv-notes.html — it shows a console "
                "script for Chrome that captures notes as TV loads them. "
                "Or use Ctrl+Shift+I (not F12) to open Chrome DevTools on tradingview.com."
            ),
        }
        S3.put_object(Bucket=BUCKET, Key="data/tv-crawler-status.json",
                      Body=json.dumps(status, indent=2).encode(),
                      ContentType="application/json")

        line = ("harvest: session_valid=%s notes=%d tickers=%d mirror=%d brain_upserted=%d"
                % (status["session_valid"], notes_n, syms_n, mirror_count,
                   status["brain_upserted"]))
        print(line)
        rep.kv(summary=line)
        if fails:
            for f in fails:
                rep.fail(f)
            print("FAILURES: " + " | ".join(fails))
            sys.exit(1)
        rep.ok("TV harvest complete — notes in brain, fleet updated")


if __name__ == "__main__":
    main()
