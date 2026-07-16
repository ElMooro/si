"""ops 3360 — capital-flow v2.0: ALL-institution dollar flows + counts.

Adds the layer Khalid asked for on capital-flow.html: per-name institutional
DOLLAR amounts and HOW MANY institutions (total, +/- QoQ, increased/reduced/
new/closed) across every 13F filer via FMP /stable positions-summary — plus a
famous-funds $ join from data/13f-flows-by-ticker.json, dollar in/out boards,
all-institution breadth sums, sector $ rotation, and a daily history ledger
that powers NEW-today + score-delta chips.

Gates (behavior, not spec):
  G1 raw FMP probe on AAPL returns the expected fields AND totalInvested is
     DOLLARS (>1e11 discriminates $2-3T dollars from ~15B shares).
  G2 deployed zip carries the v2 marker before invoke (3342 race pattern).
  G3 fresh feed: version 2.0, sources.inst_deep >= 40, dollar boards non-empty
     with investors + increased_pos populated, accumulating[0] carries
     detail.inst_deep, funds13f_join >= 20, inst_breadth sums numeric.
  G4 history ledger has today's entry with flagged_scores >= 40 names.
  G5 live page serves data-tab="dollars" (poll, advisory on CDN lag).
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-capital-flow"
REGION = "us-east-1"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240, retries={"max_attempts": 0}))


def _j(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def _http(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-ops/3360"})
    return urllib.request.urlopen(req, timeout=timeout).read()


with report("3360_capital_flow_dollars") as R:
    out = {}

    # ── G1: FMP schema + units probe ──
    R.section("G1 — FMP positions-summary probe (AAPL)")
    key = None
    try:
        env = lam.get_function_configuration(FunctionName=FN) \
            .get("Environment", {}).get("Variables", {}) or {}
    except Exception:
        env = {}
    key = env.get("FMP_API_KEY") or env.get("FMP_KEY")
    if not key:
        scr = lam.get_function_configuration(FunctionName="justhodl-stock-screener") \
            .get("Environment", {}).get("Variables", {}) or {}
        key = scr.get("FMP_API_KEY") or scr.get("FMP_KEY") \
            or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
    probe = None
    yy, qq = None, None
    import datetime as _dt
    now = _dt.datetime.now(_dt.timezone.utc)
    y, q = now.year, (now.month - 1) // 3 + 1
    cands = []
    for _ in range(5):
        q -= 1
        if q == 0:
            y, q = y - 1, 4
        cands.append((y, q))
    for cy, cq in cands:
        try:
            js = json.loads(_http(
                "https://financialmodelingprep.com/stable/institutional-ownership/"
                f"symbol-positions-summary?apikey={key}&symbol=AAPL&year={cy}&quarter={cq}"))
            if isinstance(js, list) and js:
                probe, yy, qq = js[0], cy, cq
                break
        except Exception as e:
            print(f"  probe Q{cq} {cy}: {e!r}")
    if not probe:
        R.fail("FMP positions-summary returned nothing for AAPL on any recent quarter")
        raise SystemExit(1)
    print(f"[G1] quarter Q{qq} {yy} · fields: {sorted(probe.keys())}")
    ti = probe.get("totalInvested")
    inv = probe.get("investorsHolding")
    incp = probe.get("increasedPositions")
    print(f"[G1] AAPL totalInvested={ti} investorsHolding={inv} increasedPositions={incp} "
          f"totalInvestedChange={probe.get('totalInvestedChange')} "
          f"investorsHoldingChange={probe.get('investorsHoldingChange')}")
    g1 = (isinstance(ti, (int, float)) and ti > 1e11
          and isinstance(inv, (int, float)) and inv > 1000
          and incp is not None)
    out["g1_units_dollars"] = g1
    if not g1:
        R.fail(f"G1 FAILED — units/fields off: totalInvested={ti} investors={inv} inc={incp}")
        raise SystemExit(1)
    R.ok(f"G1 ✓ dollars confirmed (AAPL ${ti/1e12:.2f}T, {int(inv):,} institutions)")

    # ── deploy v2.0 (env passthrough, re-assert daily rule) ──
    R.section("Deploy justhodl-capital-flow v2.0")
    deploy_lambda(
        report=R,
        function_name=FN,
        source_dir=Path("aws/lambdas/justhodl-capital-flow/source"),
        env_vars=env,
        eb_rule_name="justhodl-capital-flow-daily",
        eb_schedule="cron(30 16 * * ? *)",
        timeout=180,
        memory=512,
        description="Unified capital-flow tracker v2.0 — 13F + ETF + inst QoQ + ALL-institution $ flows",
        create_function_url=False,
        smoke=False,
    )

    # ── G2: settled + v2 marker in deployed zip ──
    R.section("G2 — deployed-code marker")
    for i in range(45):
        st = lam.get_function_configuration(FunctionName=FN)
        if st.get("LastUpdateStatus", "Successful") == "Successful" and st.get("State") == "Active":
            loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
            src = zipfile.ZipFile(io.BytesIO(_http(loc, timeout=60))) \
                .read("lambda_function.py").decode("utf-8", "ignore")
            if "inst_deep" in src and "dollar_flow_in" in src:
                break
        time.sleep(6)
    else:
        R.fail("v2 marker never appeared in deployed zip")
        raise SystemExit(1)
    R.ok("G2 ✓ v2 code deployed & settled")

    # ── invoke + poll fresh feed ──
    R.section("G3 — invoke + feed truth-bands")
    g0 = None
    try:
        g0 = _j("data/capital-flow.json").get("generated_at")
    except Exception:
        pass
    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    d = None
    for i in range(60):
        time.sleep(5)
        try:
            cand = _j("data/capital-flow.json")
            if cand.get("generated_at") != g0:
                d = cand
                break
        except Exception:
            pass
    if not d:
        R.fail("feed never refreshed after invoke")
        raise SystemExit(1)

    src_ = d.get("sources") or {}
    ib = d.get("inst_breadth") or {}
    din = d.get("dollar_flow_in") or []
    dout = d.get("dollar_flow_out") or []
    sdf = d.get("sector_dollar_flows") or []
    acc = d.get("accumulating") or []
    checks = {
        "version_2": d.get("version") == "2.0",
        "inst_deep_ge_40": (src_.get("inst_deep") or 0) >= 40,
        "funds13f_join_ge_20": (src_.get("funds13f_join") or 0) >= 20,
        "dollar_in_nonempty": len(din) >= 5,
        "dollar_out_nonempty": len(dout) >= 5,
        "din_has_counts": bool(din and din[0].get("investors")
                               and din[0].get("increased_pos") is not None
                               and din[0].get("invested_chg_usd")),
        "acc0_has_inst_deep": bool(acc and (acc[0].get("detail") or {}).get("inst_deep")),
        "breadth_numeric": isinstance(ib.get("usd_chg_net"), (int, float))
                           and isinstance(ib.get("increased_sum"), (int, float)),
        "sector_usd_nonempty": len(sdf) >= 3,
    }
    out["g3"] = checks
    print(json.dumps({"sources": src_, "inst_breadth": ib,
                      "quarter": d.get("quarter_13f")}, indent=1))
    print("[G3] TOP DOLLAR IN:")
    for r in din[:5]:
        print(f"   {r['ticker']:6s} {str(r.get('invested_chg_usd')):>16s} $chg · "
              f"{r.get('investors')} institutions ({r.get('investors_chg')}) · "
              f"↑{r.get('increased_pos')}/↓{r.get('reduced_pos')} · "
              f"famous {r.get('funds_net_usd')}")
    print("[G3] TOP DOLLAR OUT:")
    for r in dout[:5]:
        print(f"   {r['ticker']:6s} {str(r.get('invested_chg_usd')):>16s} $chg · "
              f"{r.get('investors')} institutions ({r.get('investors_chg')}) · "
              f"↑{r.get('increased_pos')}/↓{r.get('reduced_pos')}")
    if not all(checks.values()):
        R.fail(f"G3 FAILED: {json.dumps(checks)}")
        raise SystemExit(1)
    R.ok(f"G3 ✓ v2 feed live — {src_.get('inst_deep')} $-enriched, "
         f"net inst $ {ib.get('usd_chg_net')}, in/out {len(din)}/{len(dout)}")

    # ── G4: history ledger ──
    R.section("G4 — history ledger")
    hist = _j("data/capital-flow-history.json")
    entries = hist.get("entries") or []
    today = _dt.datetime.now(_dt.timezone.utc).date().isoformat()
    te = next((e for e in entries if e.get("date") == today), None)
    g4 = bool(te and len(te.get("flagged_scores") or {}) >= 40)
    out["g4_history"] = {"n_entries": len(entries), "today_present": bool(te),
                         "n_flagged_scores": len((te or {}).get("flagged_scores") or {})}
    if not g4:
        R.fail(f"G4 FAILED: {json.dumps(out['g4_history'])}")
        raise SystemExit(1)
    R.ok(f"G4 ✓ ledger {len(entries)} entries, today tracks "
         f"{len(te.get('flagged_scores'))} flagged names")

    # ── G5: live page marker (bare URL = client reality; CDN lag advisory) ──
    R.section("G5 — live page")
    live = False
    for i in range(16):
        try:
            html = _http("https://justhodl.ai/capital-flow.html", timeout=20).decode("utf-8", "ignore")
            if 'data-tab="dollars"' in html and "dollar_flow_in" in html:
                live = True
                break
        except Exception:
            pass
        time.sleep(15)
    out["g5_page_live"] = live
    if live:
        R.ok("G5 ✓ Dollar Flows tab serving on justhodl.ai")
    else:
        R.warn("G5 — page marker not yet on bare URL (pages.yml/CDN lag); "
               "feed verified, tab ships on propagation")

    out["verdict"] = "PASS"
    Path("aws/ops/reports/3360.json").write_text(json.dumps(out, indent=1))
    R.ok("VERDICT: PASS")

sys.exit(0)
