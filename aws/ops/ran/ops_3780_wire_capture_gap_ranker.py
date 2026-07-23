#!/usr/bin/env python3
"""ops 3780 — capture_gap overlay into master-ranker (probe-verified, not guessed).

3779 wired best-setups (47/50 joined). This does the same for the ranker, the
other consumer 3776 confirmed already reads chokepoint.json.

VERIFIED BY GREP BEFORE WRITING (the discipline that 3777 failed and 3778 fixed):
  - reader        : fetch_json("data/chokepoint.json")        [line 1267]
  - loop variable : top_tickers                                [line 1271]
  - ticker field  : t["ticker"]                                [line 1272]
  - output key    : "top_tickers": top_tickers                 [line ~1326]
  - existing      : structural-chokepoint overlay at line 1204, which states
                    "durability, not a score change" — this follows that same
                    convention rather than inventing a second one.

CONTEXT, NOT SCORE. master-ranker ranks conviction across systems. capture_gap
is a slow valuation-structure fact; letting it move rank would smuggle a quality
factor into a cross-system conviction score whose weights are calibrated
elsewhere. It annotates `rationale` and adds fields — nothing else.

GATE DESIGN: forecast the join from the LIVE artifacts BEFORE splicing (this is
what caught 3777's wrong key), then prove a non-zero join on the LIVE output
after invoking. A field that silently fails to carry is the failure mode this
arc hit three times.
"""
import sys, json, time, zipfile, io
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "shared"))

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
import boto3

FN = "justhodl-master-ranker"
SRC = ROOT / "lambdas" / FN / "source"
LF = SRC / "lambda_function.py"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/master-ranker.json"
MARKER = "Capture-gap overlay (ops 3780)"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


OVERLAY = '''
    # ── Capture-gap overlay (ops 3780) ──
    # Value CREATION vs value CAPTURE: criticality percentile minus market-cap-share
    # percentile, within industry (plus the cross-industry variant). Attached as
    # CONTEXT ONLY — like the structural overlay above, this does NOT change score
    # or rank: capture gap is a slow valuation-structure fact, while this board's
    # conviction weights are calibrated on shorter-horizon evidence.
    _cap = (_ck.get("capture_gap") or {})
    _cap_rows = {r.get("ticker"): r for r in (_cap.get("all_rows") or [])}
    n_capture = 0
    for t in top_tickers:
        cr = _cap_rows.get(t.get("ticker"))
        if not cr:
            continue
        n_capture += 1
        t["capture_gap"] = cr.get("capture_gap")
        t["global_capture_gap"] = cr.get("global_capture_gap")
        t["capture_tier"] = cr.get("tier")
        t["mcap_share_pct"] = cr.get("mcap_share_pct")
        t["undervaluation_score"] = cr.get("undervaluation_score")
        if cr.get("catchup_pct") is not None:
            t["catchup_pct"] = cr.get("catchup_pct")
            t["catchup_basis"] = cr.get("catchup_basis")
        if cr.get("tier") == "STRUCTURALLY_UNDERVALUED":
            _n = "captures %.0fpp less of its industry than its criticality implies" % (
                cr.get("capture_gap") or 0)
            if cr.get("catchup_pct") is not None:
                _n += "; %.0f%% to industry-median multiple (%s — arithmetic, not a target)" % (
                    cr["catchup_pct"], cr.get("catchup_basis") or "-")
            t["rationale"] = (t.get("rationale") or "") + " · " + _n
    print("[master-ranker] capture_gap joined=%d" % n_capture)
'''


def main():
    with report("3780_wire_capture_gap_ranker") as rep:
        rep.heading("ops 3780 — capture_gap overlay into master-ranker")

        src = LF.read_text()

        # ── G0: producer rows + consumer anchors, all grepped ─────────────
        rep.section("G0 — live producer rows")
        ck = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = ck.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        gate(rep, "G0.all_rows", len(rows) > 500, "all_rows n=%d" % len(rows))
        sample = rows[0] if rows else {}
        for f in ("ticker", "capture_gap", "global_capture_gap", "tier",
                  "mcap_share_pct", "undervaluation_score"):
            gate(rep, f"G0.row_{f}", f in sample, "present")

        rep.section("G0 — consumer anchors (grepped, never typed from memory)")
        gate(rep, "G0.reader", '_ck = fetch_json("data/chokepoint.json") or {}' in src,
             "_ck already loaded — reusing, no second fetch")
        gate(rep, "G0.loopvar", "    for t in top_tickers:" in src, "top_tickers is the row list")
        gate(rep, "G0.outkey", '"top_tickers": top_tickers,' in src, "output key confirmed")
        anchor = '    print("[master-ranker] Collecting macro signals…")'
        gate(rep, "G0.anchor", src.count(anchor) == 1, "splice anchor unique (after _ck block)")
        if FAILED:
            sys.exit(1)

        # ── forecast the join BEFORE splicing (this caught 3777) ──────────
        rep.section("Forecast join from LIVE artifacts")
        try:
            mr = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
            cur = mr.get("top_tickers") or []
            cur_t = {x.get("ticker") for x in cur if x.get("ticker")}
            cap_t = {r.get("ticker") for r in rows if r.get("ticker")}
            ov = cur_t & cap_t
            rep.kv(current_top_tickers=len(cur_t), capture_names=len(cap_t),
                   forecast_overlap=len(ov))
            gate(rep, "FORECAST.nonzero", len(ov) > 0,
                 "%d will join (sample %s)" % (len(ov), sorted(list(ov))[:8]))
        except Exception as e:
            gate(rep, "FORECAST.nonzero", False, str(e)[:160])
        if FAILED:
            sys.exit(1)

        # ── SPLICE ────────────────────────────────────────────────────────
        rep.section("Splice (additive)")
        if MARKER in src:
            rep.warn("overlay already present")
        else:
            src = src.replace(anchor, OVERLAY + "\n" + anchor, 1)
            LF.write_text(src)
        import py_compile
        py_compile.compile(str(LF), doraise=True)
        rep.ok("spliced + compile clean")
        cur_src = LF.read_text()
        gate(rep, "SPLICE.marker", MARKER in cur_src, "marker in source")
        gate(rep, "SPLICE.structural_kept",
             '_structural = _ck.get("structural_names") or {}' in cur_src,
             "pre-existing structural overlay untouched")
        gate(rep, "SPLICE.no_double_fetch", cur_src.count('fetch_json("data/chokepoint.json")') == 1,
             "still exactly one chokepoint fetch")

        # ── DEPLOY ────────────────────────────────────────────────────────
        rep.section("Deploy")
        cfg = lam.get_function_configuration(FunctionName=FN)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=cfg.get("Timeout", 900), memory=cfg.get("MemorySize", 1024),
                      description="Master ranker + capture-gap overlay (value creation vs capture as context, never a score change).",
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
                if MARKER in z.read("lambda_function.py").decode("utf-8", "replace"):
                    settled = True
                    rep.ok("settled attempt %d" % (i + 1))
                    break
        gate(rep, "DEPLOY.settled", settled, "artifact live")
        if FAILED:
            sys.exit(1)

        rep.section("Invoke + prove non-zero join on LIVE output")
        from botocore.config import Config
        ll = boto3.client("lambda", region_name="us-east-1",
                          config=Config(read_timeout=890, retries={"max_attempts": 0}))
        t0 = time.time()
        r = ll.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(invoke_status=r.get("StatusCode"), invoke_seconds=round(time.time() - t0, 1))

        out = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
        tt = out.get("top_tickers") or []
        joined = [x for x in tt if x.get("capture_gap") is not None]
        withcu = [x for x in joined if x.get("catchup_pct") is not None]
        under = [x for x in joined if x.get("capture_tier") == "STRUCTURALLY_UNDERVALUED"]
        rep.kv(top_tickers=len(tt), capture_joined=len(joined),
               with_catchup=len(withcu), structurally_undervalued=len(under))
        gate(rep, "LIVE.join_nonzero", len(joined) > 0,
             "%d of %d ranked names carry capture_gap" % (len(joined), len(tt)))
        gate(rep, "LIVE.fields_carried",
             all(k in (joined[0] if joined else {}) for k in
                 ("capture_gap", "global_capture_gap", "capture_tier")),
             "fields survived into the live artifact")

        if joined:
            rep.section("Ranked names with capture context")
            for x in sorted(joined, key=lambda z: -(z.get("capture_gap") or -999))[:12]:
                rep.log("  %-6s gap=%+6.1fpp global=%+6.1fpp catchup=%7s%% tier=%s" % (
                    x.get("ticker"), x.get("capture_gap") or 0, x.get("global_capture_gap") or 0,
                    ("%.0f" % x["catchup_pct"]) if x.get("catchup_pct") is not None else "—",
                    x.get("capture_tier")))

        rep.section("Additive contract")
        for k in ("top_tickers", "top_macro", "alerts", "feed_health"):
            gate(rep, f"ADDITIVE.{k}", k in out, "present")
        gate(rep, "ADDITIVE.structural_alive",
             (out.get("alerts") or {}).get("n_structural_chokepoints") is not None,
             "structural counter still reported")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — capture_gap now reaches both the setups desk and the ranker")


if __name__ == "__main__":
    main()
