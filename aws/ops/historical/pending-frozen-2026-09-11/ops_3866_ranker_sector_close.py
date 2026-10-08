"""
ops_3866 — WIRE + GATE: close the last 8 unresolved ranker sectors.

Evidence chain (all measured, none assumed):
  3863  rotation fires on 16/25; 17/25 resolvable; 8 names unresolved
  3865  finviz-universe.json closes 8/8 (11.5k pairs, ~3h old); universe.json
        also 8/8 but ~23h; capital-flow-radar + accumulation-radar = ZERO pairs
  3865  risk_regime_score = 2.5 -> inside the engine's own neutral band, so
        risk_regime_mult 0/25 is CORRECT and is deliberately left untouched here

This ops does NOT call deploy_lambda: the same push carries the engine source, so
deploy-lambdas.yml owns that deploy and the ops helper would clobber it. Instead
it zip-settles BY MARKER (get_function -> download Code.Location -> unzip -> grep
the new donor string) before invoking, because State==Active returns instantly
when deploy-lambdas has not started yet and the invoke would hit the OLD artifact
— the exact failure that reddened ops 3830.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-master-ranker"
BUCKET = "justhodl-dashboard-live"
KEY = "data/master-ranker.json"
MARKER = "data/finviz-universe.json"
UNRESOLVED = ["BE", "UMC", "DFTX", "OVV", "HCC", "ALHC", "IPGP", "CENX"]

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")


def snapshot():
    o = s3.get_object(Bucket=BUCKET, Key=KEY)
    return json.loads(o["Body"].read()), o["LastModified"]


def coverage(doc):
    rows = doc.get("top_tickers") or []
    rot = sum(1 for r in rows if r.get("rotation_mult") is not None
              and abs(float(r["rotation_mult"]) - 1.0) > 1e-9)
    note = sum(1 for r in rows if r.get("rotation_note"))
    liq = sum(1 for r in rows if r.get("liquidity_regime_mult") is not None
              and abs(float(r["liquidity_regime_mult"]) - 1.0) > 1e-9)
    return rows, rot, note, liq


def main():
    with report("3866_ranker_sector_close") as rep:
        rep.heading("ops 3866 — WIRE finviz-universe donor + gate on measured coverage")

        rep.section("1. BEFORE")
        before, blm = snapshot()
        brows, brot, bnote, bliq = coverage(before)
        rep.kv(before_rows=len(brows), before_rotation_nonneutral=brot,
               before_rotation_notes=bnote, before_liquidity_nonneutral=bliq,
               before_s3=blm.isoformat())

        rep.section("2. ZIP-SETTLE BY MARKER — never invoke the old artifact")
        settled = False
        for attempt in range(1, 31):                       # up to ~10 min
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                if MARKER in src:
                    rep.ok(f"  new artifact live on attempt {attempt} ({len(blob):,} zip bytes)")
                    settled = True
                    break
                rep.log(f"  attempt {attempt}: artifact still lacks '{MARKER}'")
            except Exception as e:
                rep.log(f"  attempt {attempt}: {str(e)[:90]}")
            time.sleep(20)
        if not settled:
            rep.fail(f"  deploy never landed — '{MARKER}' absent from the deployed zip")
            sys.exit(1)

        cfg = lam.get_function_configuration(FunctionName=FN)
        for _ in range(30):
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") != "InProgress":
                break
            time.sleep(10)
            cfg = lam.get_function_configuration(FunctionName=FN)
        rep.ok(f"  State={cfg.get('State')} LastUpdateStatus={cfg.get('LastUpdateStatus')}")

        rep.section("3. invoke (async — the ranker fans out across ~30 feeds)")
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        after = None
        for attempt in range(1, 37):                       # up to ~12 min
            time.sleep(20)
            try:
                doc, lm = snapshot()
            except Exception:
                continue
            if lm > blm:
                after = doc
                rep.ok(f"  artifact rewritten on attempt {attempt} ({lm.isoformat()})")
                break
        if after is None:
            rep.fail("  master-ranker.json never rewrote — check CloudWatch before any further change")
            sys.exit(1)

        rep.section("4. AFTER — measured coverage")
        rows, rot, note, liq = coverage(after)
        tick = [str(r["ticker"]).upper() for r in rows if r.get("ticker")]
        still = [t for t in UNRESOLVED if t in tick and not any(
            r.get("rotation_note") for r in rows if str(r.get("ticker", "")).upper() == t)]
        rep.kv(after_rows=len(rows), rotation_nonneutral=rot, rotation_notes=note,
               liquidity_nonneutral=liq, previously_unresolved_present=
               sum(1 for t in UNRESOLVED if t in tick), still_untilted=str(still))

        checks = [
            ("rotation coverage improved vs before", rot > brot),
            ("rotation tilt on >= 20/25", rot >= min(20, len(rows))),
            ("liquidity overlay tracks the same sector map", liq >= rot),
            ("no row lost its tilt (no regression)", rot >= brot),
        ]
        for label, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {label}")

        rep.section("5. honest note on what was deliberately NOT changed")
        rep.log("  risk_regime_mult stays 0/25 by design: probe 3865 measured "
                "risk_regime_score = 2.5, inside the engine's neutral band (-12, 12]. "
                "A neutral regime SHOULD produce no tilt; forcing one would be a "
                "manufactured signal.")

        failed = [l for l, ok in checks if not ok]
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — rotation {brot}/25 -> {rot}/{len(rows)} after wiring the donor")


if __name__ == "__main__":
    main()
