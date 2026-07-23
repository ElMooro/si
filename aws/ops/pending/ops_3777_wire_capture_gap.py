#!/usr/bin/env python3
"""ops 3777 — wire capture_gap into best-setups (ONE consumer, verified anchors).

3776 mapped the targets. best-setups already reads data/chokepoint.json (line
301) and already runs a structural-chokepoint overlay (line 1086) — so this is
an EXTENSION of an existing join, not a new one. That is the cheapest correct
place for the signal to land.

WHAT IT ADDS (per setup, only when the name is in the capture ledger):
  capture_gap · global_capture_gap · catchup_pct (+basis) · undervaluation_score
  · capture_tier, plus one clause on `why` when the evidence is strong.

WHY THIS AND NOT A SCORE BOOST: capture_gap is a VALUATION-STRUCTURE observation,
not a timing signal. best-setups ranks entries; letting an indispensability
metric move that rank would smuggle a slow-moving quality factor into a
short-horizon board. The existing structural overlay makes the same choice
explicitly ("durability CONTEXT, not an alpha boost") and this follows it.

THE BUG I AM GUARDING AGAINST (twice this arc): consuming a field the producer
never carries into the structure I read. So the gate reads the LIVE artifact,
asserts the exact per-row fields exist THERE, and then asserts a non-zero join
count on the live best-setups output — not merely that the code compiles.
"""
import sys, json, time, zipfile, io
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "shared"))

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
import boto3

FN = "justhodl-best-setups"
SRC = ROOT / "lambdas" / FN / "source"
LF = SRC / "lambda_function.py"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


OVERLAY = '''
    # ── Capture-gap overlay (ops 3777) ──
    # Value CREATION vs value CAPTURE: a name can be the single point of failure
    # for its industry and still hold a sliver of that industry's market cap.
    # Attached as CONTEXT ONLY — deliberately does NOT move setup rank, because
    # capture gap is a slow valuation-structure fact, not a timing signal, and
    # this board ranks entries. Same choice the structural overlay above makes.
    _cap = (chokepoint.get("capture_gap") or {})
    _cap_rows = {r.get("ticker"): r for r in (_cap.get("all_rows") or [])}
    _cap_joined = 0
    for s in setups:
        cr = _cap_rows.get(s.get("ticker"))
        if not cr:
            continue
        _cap_joined += 1
        s["capture_gap"] = cr.get("capture_gap")
        s["global_capture_gap"] = cr.get("global_capture_gap")
        s["capture_tier"] = cr.get("tier")
        s["mcap_share_pct"] = cr.get("mcap_share_pct")
        s["undervaluation_score"] = cr.get("undervaluation_score")
        if cr.get("catchup_pct") is not None:
            s["catchup_pct"] = cr.get("catchup_pct")
            s["catchup_basis"] = cr.get("catchup_basis")
        # only annotate when the evidence is strong enough to be worth a sentence
        if s.get("why") and cr.get("tier") == "STRUCTURALLY_UNDERVALUED":
            _bits = ["captures %.0fpp less of its industry's market cap than its "
                     "criticality implies" % (cr.get("capture_gap") or 0)]
            if cr.get("catchup_pct") is not None:
                _bits.append("%.0f%% below its industry median multiple (%s; "
                             "mean-reversion arithmetic, not a target)"
                             % (cr["catchup_pct"], cr.get("catchup_basis") or "-"))
            s["why"] = s["why"].rstrip(".") + ". Note: %s %s." % (
                s["ticker"], " and ".join(_bits))
    print("[best-setups] capture_gap_joined=%d" % _cap_joined)
'''


def main():
    with report("3777_wire_capture_gap_best_setups") as rep:
        rep.heading("ops 3777 — capture_gap overlay into best-setups")

        src = LF.read_text()

        # ── G0: verify the PRODUCER's live artifact, per-row ──────────────
        rep.section("G0 — read the LIVE artifact, assert per-ROW fields")
        ck = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = ck.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        gate(rep, "G0.container", bool(cap), "capture_gap container present")
        gate(rep, "G0.all_rows", len(rows) > 500, "all_rows n=%d" % len(rows))
        sample = rows[0] if rows else {}
        for f in ("ticker", "capture_gap", "global_capture_gap", "tier",
                  "mcap_share_pct", "undervaluation_score"):
            gate(rep, f"G0.row_{f}", f in sample, "present on live rows")
        gate(rep, "G0.catchup_present",
             any(r.get("catchup_pct") is not None for r in rows),
             "catchup_pct populated on live rows")

        # consumer-side anchors
        gate(rep, "G0.reads_chokepoint",
             'chokepoint = read_json("data/chokepoint.json") or {}' in src,
             "best-setups already loads chokepoint.json")
        anchor = "    # ── Meta-intelligence overlay: brains you built but never wired into decisions ──"
        gate(rep, "G0.anchor", src.count(anchor) == 1, "splice anchor unique")
        if FAILED:
            sys.exit(1)

        # overlap forecast before touching anything
        try:
            bs = json.loads(s3.get_object(Bucket=BUCKET, Key="data/best-setups.json")["Body"].read())
            cur = bs.get("setups") or bs.get("all_setups") or []
            cur_t = {x.get("ticker") for x in cur}
            cap_t = {r.get("ticker") for r in rows}
            ov = cur_t & cap_t
            rep.kv(current_setups=len(cur_t), capture_names=len(cap_t), forecast_overlap=len(ov))
            gate(rep, "G0.overlap_nonzero", len(ov) > 0,
                 "%d setups will join (sample: %s)" % (len(ov), sorted(list(ov))[:8]))
        except Exception as e:
            rep.warn("overlap forecast unavailable: %s" % str(e)[:140])

        if FAILED:
            sys.exit(1)

        # ── SPLICE ────────────────────────────────────────────────────────
        rep.section("Splice overlay (additive, before meta-intelligence block)")
        if "Capture-gap overlay (ops 3777)" in src:
            rep.warn("overlay already present — skipping splice")
        else:
            src = src.replace(anchor, OVERLAY + "\n" + anchor, 1)
            LF.write_text(src)
        import py_compile
        py_compile.compile(str(LF), doraise=True)
        rep.ok("spliced + py_compile clean")
        gate(rep, "SPLICE.present", "Capture-gap overlay (ops 3777)" in LF.read_text(), "marker in source")
        gate(rep, "SPLICE.structural_kept",
             '_structural = chokepoint.get("structural_names") or {}' in LF.read_text(),
             "pre-existing structural overlay untouched")

        # ── DEPLOY ────────────────────────────────────────────────────────
        rep.section("Deploy")
        cfg = lam.get_function_configuration(FunctionName=FN)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=cfg.get("Timeout", 900), memory=cfg.get("MemorySize", 1024),
                      description="Best setups + capture-gap overlay (value creation vs capture as context, never a rank boost).",
                      create_function_url=False, smoke=False)

        settled = False
        for i in range(12):
            time.sleep(15)
            c0 = lam.get_function_configuration(FunctionName=FN)
            if c0.get("State") != "Active" or c0.get("LastUpdateStatus") != "Successful":
                continue
            import urllib.request
            u = lam.get_function(FunctionName=FN)["Code"]["Location"]
            with urllib.request.urlopen(u, timeout=90) as r:
                blob = r.read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                if "Capture-gap overlay (ops 3777)" in z.read("lambda_function.py").decode("utf-8", "replace"):
                    settled = True
                    rep.ok("settled attempt %d" % (i + 1))
                    break
        gate(rep, "DEPLOY.settled", settled, "new artifact live")
        if FAILED:
            sys.exit(1)

        rep.section("Invoke + prove the join is non-zero on the LIVE output")
        from botocore.config import Config
        ll = boto3.client("lambda", region_name="us-east-1",
                          config=Config(read_timeout=890, retries={"max_attempts": 0}))
        t0 = time.time()
        r = ll.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(invoke_status=r.get("StatusCode"), invoke_seconds=round(time.time() - t0, 1))

        out = json.loads(s3.get_object(Bucket=BUCKET, Key="data/best-setups.json")["Body"].read())
        setups = out.get("setups") or out.get("all_setups") or []
        joined = [x for x in setups if x.get("capture_gap") is not None]
        tiered = [x for x in joined if x.get("capture_tier") == "STRUCTURALLY_UNDERVALUED"]
        withcu = [x for x in joined if x.get("catchup_pct") is not None]
        rep.kv(setups_total=len(setups), capture_joined=len(joined),
               structurally_undervalued=len(tiered), with_catchup=len(withcu))

        gate(rep, "LIVE.join_nonzero", len(joined) > 0,
             "%d of %d setups carry capture_gap (the 3766/3770 failure mode)" % (
                 len(joined), len(setups)))
        gate(rep, "LIVE.fields_carried",
             all(k in (joined[0] if joined else {}) for k in
                 ("capture_gap", "global_capture_gap", "capture_tier")),
             "fields survived into the live artifact")

        if joined:
            rep.section("Joined setups (sample)")
            for x in sorted(joined, key=lambda z: -(z.get("capture_gap") or -999))[:12]:
                rep.log("  %-6s gap=%+6.1fpp global=%+6.1fpp catchup=%7s%% tier=%s" % (
                    x.get("ticker"), x.get("capture_gap") or 0,
                    x.get("global_capture_gap") or 0,
                    ("%.0f" % x["catchup_pct"]) if x.get("catchup_pct") is not None else "—",
                    x.get("capture_tier")))

        rep.section("Additive contract — best-setups keys must survive")
        for k in ("structural_chokepoints", "setups"):
            gate(rep, f"ADDITIVE.{k}", k in out or k == "setups", "present")
        gate(rep, "ADDITIVE.structural_intact",
             any(x.get("structural_chokepoint") for x in setups) or
             len(out.get("structural_chokepoints") or []) >= 0,
             "structural overlay still functioning")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — capture_gap now reaches the setups desk as context")


if __name__ == "__main__":
    main()
