"""
ops 3716 — estimate-revisions sidecar diagnosis + readthrough PAGE field-coverage audit

WHY THIS RUNS
═════════════
Per the PAGE CONTRACT / POST-DEPLOY BUG HUNT doctrine (AUTONOMY.md, added
2026-07-22): a non-empty `degraded` array is an OPEN BUG, not decoration.
After ops 3715 fixed the forward-orders join, readthrough still reports:

    degraded = ['fundamental sidecar missing: estimate-revisions']

[A] G0 KEY CONTRACT — already checked in-repo before writing this file, and
    unlike forward-orders the names MATCH:
        producer justhodl-estimate-revisions writes "upward_revisions",
          "downward_revisions" (+ top_picks, estimate_strength_leaders)
        readthrough reads ("upward_revisions", "downward_revisions",
          "signals", "strength_rows", "all")
    So this is NOT a naming mismatch. The failure is upstream: the artifact is
    stale/absent, or those arrays are empty.

    PRIME SUSPECT: the producer's own data_source line reads
      "FMP analyst-estimates (depth) + Benzinga consensus (freshness, via Massive)"
    and Benzinga has been DEAD since 2026-07-15 (403 NOT_AUTHORIZED across all
    three Massive keys; Benzinga absent from flatfiles). Several engines were
    re-sourced onto FMP /stable at ops 3311-3323 via aws/shared/fmp_analyst.py.
    If estimate-revisions was missed in that sweep it would silently produce
    empty revision arrays while still writing a healthy-looking document.

    This ops DIAGNOSES ONLY on the estimate-revisions side. No blind fix.

[B] PAGE FIELD-COVERAGE AUDIT on readthrough.html — the doctrine's step 2.
    readthrough v1.2.2 now publishes consensus_coverage, quadrant_counts,
    per-row pricing_quadrant / quadrant_note / fundamentals.consensus_* and
    analyst_actions. The page was built at v1.0.x. Any key with no render path
    is a gap: the engine's data exists but no human can see it. This is the
    exact defect class that hid 11 of 17 sectors.html fields and zeroed the
    capital-flow 13F counts.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

import boto3  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT.parent / "readthrough.html"

BUCKET = "justhodl-dashboard-live"
ER_KEY = "data/estimate-revisions.json"
RT_KEY = "data/readthrough.json"
ER_FN = "justhodl-estimate-revisions"

S3C = boto3.client("s3", region_name="us-east-1")
LAM = boto3.client("lambda", region_name="us-east-1")

# keys readthrough v1.2.2 publishes that a human should be able to SEE
PAGE_MUST_SHOW = [
    "consensus_coverage", "quadrant_counts", "pricing_quadrant", "quadrant_note",
    "consensus_observed", "consensus_moved", "consensus_dissenting",
    "analyst_actions", "rpo_representative", "book_to_bill_spread_pct",
    "days_since_catalyst", "implied_order_usd", "materiality_pct",
]


def main():
    with report("3716_est_revisions_pagecov") as rep:
        rep.heading("ops 3716 — estimate-revisions diagnosis + readthrough page coverage")
        fails = []
        out = {}

        def gate(n, ok, d):
            rep.log(f"{n} {bool(ok)}")
            print(f"  {n:34} {bool(ok)}  {d}")
            out[n] = {"ok": bool(ok), "detail": d}
            if not ok:
                fails.append(n)

        # ── A. estimate-revisions artifact ──────────────────────────────────
        rep.section("A — estimate-revisions sidecar")
        er = {}
        age_h = None
        try:
            h = S3C.head_object(Bucket=BUCKET, Key=ER_KEY)
            age_h = round((datetime.now(timezone.utc) - h["LastModified"]).total_seconds()
                          / 3600.0, 1)
            er = json.loads(S3C.get_object(Bucket=BUCKET, Key=ER_KEY)["Body"].read())
            gate("A0_artifact_exists", True, f"{ER_KEY} age={age_h}h")
        except Exception as e:  # noqa: BLE001
            gate("A0_artifact_exists", False, f"cannot read {ER_KEY}: {str(e)[:130]}")

        if er:
            up = er.get("upward_revisions") or []
            dn = er.get("downward_revisions") or []
            gate("A1_revision_arrays_nonempty", bool(up or dn),
                 f"upward={len(up)} downward={len(dn)} "
                 f"n_tracked={er.get('n_tracked')} n_fmp_enriched={er.get('n_fmp_enriched')} "
                 f"n_with_history={er.get('n_with_history')} status={er.get('status')}")

            # emulate readthrough's reader exactly
            est = {}
            for k in ("upward_revisions", "downward_revisions", "signals",
                      "strength_rows", "all"):
                for row in (er.get(k) or []):
                    if isinstance(row, dict) and row.get("ticker"):
                        est.setdefault(row["ticker"], row)
            gate("A2_reader_join_nonempty", bool(est),
                 f"emulated join -> {len(est)} tickers (sample={list(est)[:6]})")

            gate("A3_fresh", age_h is not None and age_h <= 48,
                 f"age={age_h}h (>48h = schedule dead, cf. ops 3642 pattern)")

            # is the Benzinga dependency still wired?
            try:
                src_p = (ROOT / "lambdas" / ER_FN / "source" / "lambda_function.py")
                src = src_p.read_text(encoding="utf-8")
                benz = ("benzinga" in src.lower())
                fmp_shared = "fmp_analyst" in src
                gate("A4_not_on_dead_benzinga", not benz or fmp_shared,
                     f"references_benzinga={benz} uses_shared_fmp_analyst={fmp_shared} "
                     "(Benzinga 403-dead since 2026-07-15; ops 3311-3323 re-sourced "
                     "peers onto aws/shared/fmp_analyst.py)")
            except Exception as e:  # noqa: BLE001
                gate("A4_not_on_dead_benzinga", False, f"source read failed: {str(e)[:90]}")

            print("\n  estimate-revisions top-level keys:")
            print(f"    {list(er.keys())}")
            for k in ("caveats", "status", "direction_map"):
                v = er.get(k)
                if v:
                    print(f"    {k}: {str(v)[:200]}")

        # ── B. readthrough PAGE field coverage ──────────────────────────────
        rep.section("B — readthrough.html field coverage")
        try:
            html = PAGE.read_text(encoding="utf-8")
            gate("B0_page_exists", True, f"readthrough.html {len(html)} bytes")
        except Exception as e:  # noqa: BLE001
            html = ""
            gate("B0_page_exists", False, f"page not found at {PAGE}: {str(e)[:90]}")

        if html:
            rendered, gaps = [], []
            for k in PAGE_MUST_SHOW:
                (rendered if k in html else gaps).append(k)
            gate("B1_all_engine_fields_rendered", not gaps,
                 f"rendered={len(rendered)}/{len(PAGE_MUST_SHOW)} MISSING={gaps}")

            print("\n  field coverage:")
            for k in PAGE_MUST_SHOW:
                print(f"    {'OK  ' if k in html else 'GAP '} {k}")

            # cross-check against what the live artifact actually carries
            try:
                d = json.loads(S3C.get_object(Bucket=BUCKET, Key=RT_KEY)["Body"].read())
                rows = d.get("beneficiaries") or []
                rowkeys = sorted({k for r in rows[:40] for k in r.keys()})
                fkeys = sorted({k for r in rows[:40]
                                for k in (r.get("fundamentals") or {}).keys()})
                unrendered = [k for k in rowkeys + fkeys
                              if k not in html and not k.startswith("_")]
                gate("B2_no_unrendered_row_fields", len(unrendered) <= 8,
                     f"row+fundamental keys with NO render path ({len(unrendered)}): "
                     f"{unrendered[:24]}")
                print(f"\n  artifact top-level keys: {list(d.keys())}")
                print(f"  row keys: {rowkeys}")
                print(f"  fundamentals keys: {fkeys}")
                print(f"  degraded: {d.get('degraded')}")
            except Exception as e:  # noqa: BLE001
                gate("B2_no_unrendered_row_fields", False, f"artifact read failed: {str(e)[:110]}")

        out["verdict"] = ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails))
        print("\nVERDICT:", out["verdict"])
        rep.log("VERDICT: " + out["verdict"])
        for _k, _v in out.items():
            if isinstance(_v, dict):
                rep.kv(gate=_k, ok=_v.get("ok"), detail=str(_v.get("detail"))[:170])

        if fails:
            sys.exit(1)


if __name__ == "__main__":
    main()
