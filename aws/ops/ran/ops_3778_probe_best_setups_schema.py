#!/usr/bin/env python3
"""ops 3778 — probe best-setups.json schema (my reader guessed the wrong key).

3777's pre-flight did its job: it refused to splice because forecast_overlap
was 0. But the cause was NOT disjoint populations — it was current_setups=0,
i.e. MY reader found no setups at all. I guessed `setups` / `all_setups`
without grepping the producer, which is the exact mistake this arc has now
made three times (backlog keys 3766, cap_rows field carry 3770, this).

So: probe first, wire second. This ops dumps the real top-level keys of the
live artifact AND the per-row ticker field name, then reports the true overlap
against capture_gap.all_rows. It writes NO code. The wiring ops that follows
will be built against verified names only.
"""
import sys, json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def main():
    with report("3778_probe_best_setups_schema") as rep:
        rep.heading("ops 3778 — probe best-setups.json (no code written)")

        bs = json.loads(s3.get_object(Bucket=BUCKET, Key="data/best-setups.json")["Body"].read())

        rep.section("Top-level keys of the LIVE artifact")
        for k, v in bs.items():
            kind = type(v).__name__
            size = len(v) if isinstance(v, (list, dict, str)) else v
            rep.log("  %-34s %-6s %s" % (k, kind, size))

        rep.section("Which top-level keys are LISTS OF SETUP-LIKE DICTS?")
        candidates = []
        for k, v in bs.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                tick_field = None
                for tf in ("ticker", "symbol", "sym"):
                    if tf in v[0]:
                        tick_field = tf
                        break
                candidates.append((k, len(v), tick_field, sorted(v[0].keys())[:12]))
                rep.log("  %-30s n=%-5d ticker_field=%-8s keys=%s" % (
                    k, len(v), tick_field, sorted(v[0].keys())[:10]))
        gate(rep, "PROBE.found_lists", len(candidates) > 0, "%d list-of-dict keys" % len(candidates))

        rep.section("True overlap vs capture_gap.all_rows")
        ck = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap_rows = (ck.get("capture_gap") or {}).get("all_rows") or []
        cap_t = {r.get("ticker") for r in cap_rows if r.get("ticker")}
        rep.kv(capture_names=len(cap_t))

        best_key, best_ov, best_tf = None, 0, None
        for k, n, tf, _ in candidates:
            if not tf:
                continue
            ts = {r.get(tf) for r in bs[k] if r.get(tf)}
            ov = ts & cap_t
            rep.log("  %-30s n=%-5d overlap=%-5d (%s)" % (k, n, len(ov), sorted(list(ov))[:6]))
            if len(ov) > best_ov:
                best_key, best_ov, best_tf = k, len(ov), tf

        gate(rep, "PROBE.overlap_found", best_ov > 0,
             "best key '%s' (field '%s') overlaps %d names" % (best_key, best_tf, best_ov))

        rep.section("VERDICT — what the wiring ops must use")
        if best_key:
            rep.kv(use_key=best_key, ticker_field=best_tf, expected_join=best_ov)
            rep.log("  Wire against bs['%s'], ticker field '%s', expect %d joins."
                    % (best_key, best_tf, best_ov))
            rep.log("  NOTE: if the producer builds this list under a different in-code")
            rep.log("  variable name, the splice must target THAT variable, not the")
            rep.log("  output key — grep the producer before writing the overlay.")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — schema probed, wiring target identified")


if __name__ == "__main__":
    main()
