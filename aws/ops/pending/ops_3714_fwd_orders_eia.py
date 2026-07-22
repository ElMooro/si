"""
ops 3714 — forward-orders sidecar diagnosis + EIA key revival

TWO REAL DEFECTS, BOTH SURFACED BY AUDIT (not guessed)
══════════════════════════════════════════════════════

[A] readthrough reports degraded=['fundamental sidecar missing: forward-orders']
    (proven live at ops 3713). Keys match exactly on both sides:
        forward-orders  OUTPUT_KEY = "data/forward-orders.json"   (line 54)
        readthrough     s3_json("data/forward-orders.json")       (line 869)
    and the reader is generous — it accepts by_ticker / results / rankings /
    top_picks / all / scored, dict OR list-of-dicts. So an empty join means
    either the artifact is STALE/ABSENT, or the producer writes a top-level
    shape none of those six names match.

    This ops DIAGNOSES before touching anything: age of the object, its
    top-level keys, and which (if any) of the six reader-shapes would hit.
    No fix is applied blind — a wrong guess here is how ops 3712 burned a run.

[B] eia-energy-agent honestly reports "needs a valid EIA_API_KEY". Its old
    hard-coded key was revoked (403) and it fell back to FRED for the core
    dashboard, leaving the STEO block (OPEC output, world supply/demand)
    permanently empty. A live key exists in the platform key index. Setting it
    revives the bonus block AND unblocks EIA industrial-load series, which the
    buildout-canary work needs.

BUILDOUT-CANARY AUDIT RESULT (recorded here so it is not re-researched)
══════════════════════════════════════════════════════════════════════
A prior session ALREADY researched utility interconnection queues and chose a
proxy rather than fabricate: justhodl-structural-pre-signals builds the
BUILDOUT pre-signal from SEC EDGAR full-text search ("megawatts", "gigawatt",
"power purchase agreement", "hyperscale") because ERCOT's queue needs a numeric
report ID with no keyword-search discovery path and PJM requires a registered
API key this platform does not hold. Direct queue integration is therefore
BLOCKED ON A CREDENTIAL (Khalid must register for a PJM key), not on
engineering. Do not rebuild it.

Likewise already built, do NOT duplicate:
    canary #1/#15 book-to-bill + backlog coverage -> justhodl-forward-orders
                                                     (score_book_to_bill, w=0.10)
    canary #5  patent velocity                    -> justhodl-patent-velocity
    canary #2  buildout disclosure proxy          -> justhodl-structural-pre-signals
    canary #8  energy/industrial load             -> eia-energy-agent (this ops)

NOTE justhodl-canary-grid is CRISIS-polarity (30 stress/recession leads, higher
= worse). A boom-polarity grid must NOT reuse that name.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

import boto3  # noqa: E402

BUCKET = "justhodl-dashboard-live"
FWD_KEY = "data/forward-orders.json"
FWD_FN = "justhodl-forward-orders"
EIA_FN = "eia-energy-agent"
EIA_KEY_VALUE = "trvODpgt2GdvBbLeIeVMyaQwsnNFQIYSueoVm4Fl"

READER_SHAPES = ("by_ticker", "results", "rankings", "top_picks", "all", "scored")

LAM = boto3.client("lambda", region_name="us-east-1")
S3C = boto3.client("s3", region_name="us-east-1")


def main():
    with report("3714_fwd_orders_eia") as rep:
        rep.heading("ops 3714 — forward-orders sidecar diagnosis + EIA key revival")
        fails = []
        out = {}

        def gate(n, ok, d):
            rep.log(f"{n} {bool(ok)}")
            print(f"  {n:32} {bool(ok)}  {d}")
            out[n] = {"ok": bool(ok), "detail": d}
            if not ok:
                fails.append(n)

        # ── [A] DIAGNOSE forward-orders ─────────────────────────────────────
        rep.section("A — forward-orders sidecar")
        age_h = None
        doc = {}
        try:
            h = S3C.head_object(Bucket=BUCKET, Key=FWD_KEY)
            age_h = round(
                (datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 3600.0, 1)
            doc = json.loads(S3C.get_object(Bucket=BUCKET, Key=FWD_KEY)["Body"].read())
        except Exception as e:  # noqa: BLE001
            gate("A0_artifact_exists", False, f"cannot read {FWD_KEY}: {str(e)[:120]}")

        if doc or age_h is not None:
            gate("A0_artifact_exists", True, f"{FWD_KEY} age={age_h}h")

            top = list(doc.keys())
            print(f"    top-level keys: {top[:20]}")
            rep.log(f"top-level keys: {top[:14]}")

            # which reader shape would actually hit?
            hits = {}
            for k in READER_SHAPES:
                v = doc.get(k)
                if isinstance(v, dict):
                    hits[k] = f"dict[{len(v)}]"
                elif isinstance(v, list):
                    n_t = sum(1 for r in v if isinstance(r, dict) and r.get("ticker"))
                    hits[k] = f"list[{len(v)}] with_ticker={n_t}"
            gate("A1_reader_shape_hits", bool(hits),
                 f"reader-compatible keys present: {hits or 'NONE — shape mismatch'} "
                 f"(reader tries {READER_SHAPES})")

            # emulate the reader exactly
            fwd = {}
            for k in READER_SHAPES:
                v = doc.get(k)
                if isinstance(v, dict):
                    fwd.update(v)
                elif isinstance(v, list):
                    for row in v:
                        if isinstance(row, dict) and row.get("ticker"):
                            fwd.setdefault(row["ticker"], row)
            gate("A2_join_nonempty", bool(fwd),
                 f"emulated readthrough join -> {len(fwd)} tickers "
                 f"(sample={list(fwd)[:6]})")

            gate("A3_artifact_fresh", (age_h is not None and age_h <= 48),
                 f"age={age_h}h (schedule is cron(0 11 * * ? *) daily; "
                 ">48h means the schedule is dead again, cf. ops 3642)")

            # candidate shapes the reader does NOT know about
            unknown = {k: (f"dict[{len(v)}]" if isinstance(v, dict)
                           else f"list[{len(v)}]" if isinstance(v, list) else type(v).__name__)
                       for k, v in doc.items()
                       if k not in READER_SHAPES and isinstance(v, (dict, list)) and v}
            print(f"    container keys reader IGNORES: {unknown}")
            rep.log(f"ignored containers: {list(unknown)[:10]}")

        # ── [B] EIA key revival ─────────────────────────────────────────────
        rep.section("B — EIA key revival")
        try:
            cfg = LAM.get_function_configuration(FunctionName=EIA_FN)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            had = bool(env.get("EIA_API_KEY"))
            env["EIA_API_KEY"] = EIA_KEY_VALUE
            LAM.update_function_configuration(
                FunctionName=EIA_FN, Environment={"Variables": env})

            t0 = time.time()
            while time.time() - t0 < 120:
                if LAM.get_function_configuration(
                        FunctionName=EIA_FN).get("LastUpdateStatus") == "Successful":
                    break
                time.sleep(5)

            r = LAM.invoke(FunctionName=EIA_FN, InvocationType="RequestResponse",
                           Payload=b"{}")
            body = r["Payload"].read().decode("utf-8", "ignore")
            ferr = r.get("FunctionError")
            key_live = ('"eia_key_present": true' in body.lower()
                        or '"eia_key_present":true' in body.lower())
            needs = "needs a valid EIA_API_KEY" in body
            gate("B1_eia_key_set", not ferr and key_live and not needs,
                 f"had_key_before={had} FunctionError={ferr} key_present={key_live} "
                 f"still_asking_for_key={needs} resp={body[:180]}")
        except Exception as e:  # noqa: BLE001
            gate("B1_eia_key_set", False, f"exception: {str(e)[:160]}")

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
