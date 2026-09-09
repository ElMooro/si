"""ops_5224 -- audit 2026-09-08 Release C1 gate: portfolio-admin + portfolio-snapshot (INST-07/08).

Direct-invokes from inside AWS (no browser token needed):
  A. portfolio-admin: update_position on a symbol that does not exist -> ok:false "does not exist"
     (no upsert); update_position with qty "nan" on a real position -> ok:false "finite"; NO real
     position is modified (read-only against the live book).
  B. portfolio-snapshot: invoke, read portfolio/snapshot.json, assert every position carries
     valuation_status in {PRICED, STALE_MARK, UNPRICED}, market_value is null iff not PRICED,
     position_type matches the sign of qty, cost_basis_total == qty x cost_basis_per_share,
     and the summary carries pnl_scope / unpriced_positions / stops_not_evaluable.
"""
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FAILS = []
# anchored to this release's own commit (not HEAD): a later ops-only push must not move the reference past the deploy
_rel = subprocess.run(["git", "log", "--format=%ct", "-1", "--grep=Release C1 (ops 5224)"], capture_output=True, text=True, cwd=ROOT).stdout.strip()
T_PUSH = int(_rel or subprocess.run(["git", "log", "-1", "--format=%ct", "HEAD"], capture_output=True, text=True, cwd=ROOT).stdout.strip() or "0")


def expect(cond, msg):
    (R.ok if cond else R.fail)(msg)
    if not cond:
        FAILS.append(msg)


def wait_deployed(lam, fn, budget=1500):
    t0 = time.time()
    cfg = None
    while time.time() - t0 < budget:
        cfg = lam.get_function_configuration(FunctionName=fn)
        lm = datetime.fromisoformat(cfg["LastModified"].replace("Z", "+00:00")).timestamp()
        if lm >= T_PUSH and cfg.get("LastUpdateStatus", "Successful") == "Successful":
            return cfg
        time.sleep(30)
    return cfg


with report("ops_5224_portfolio_gate") as R:
    R.heading("ops 5224 -- Release C1 gate: portfolio accounting truth (INST-07/08)")
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    R.section("A. portfolio-admin")
    cfg = wait_deployed(lam, "justhodl-portfolio-admin")
    R.log("justhodl-portfolio-admin LastModified %s" % cfg["LastModified"])
    r = lam.invoke(FunctionName="justhodl-portfolio-admin", InvocationType="RequestResponse",
                   Payload=json.dumps({"action": "update_position", "symbol": "ZZZZ-OPS5224", "qty": 5}).encode())
    out = json.loads(r["Payload"].read() or b"{}")
    body = json.loads(out.get("body") or "{}") if isinstance(out.get("body"), str) else out
    expect(body.get("ok") is False and "does not exist" in str(body.get("err")), "phantom symbol refused without upsert: %s" % json.dumps(body)[:160])
    # a real position (read-only): pick the first one from the live snapshot and send a non-finite qty
    snap = json.loads(s3.get_object(Bucket=BUCKET, Key="portfolio/snapshot.json")["Body"].read())
    syms = [p.get("symbol") for p in (snap.get("positions") or []) if p.get("symbol")]
    if syms:
        r = lam.invoke(FunctionName="justhodl-portfolio-admin", InvocationType="RequestResponse",
                       Payload=json.dumps({"action": "update_position", "symbol": syms[0], "qty": "nan"}).encode())
        out = json.loads(r["Payload"].read() or b"{}")
        body = json.loads(out.get("body") or "{}") if isinstance(out.get("body"), str) else out
        expect(body.get("ok") is False and "finite" in str(body.get("err")), "non-finite qty refused on %s: %s" % (syms[0], json.dumps(body)[:160]))
    else:
        R.warn("live book has no positions -- non-finite probe skipped")

    R.section("B. portfolio-snapshot")
    cfg = wait_deployed(lam, "justhodl-portfolio-snapshot")
    R.log("justhodl-portfolio-snapshot LastModified %s" % cfg["LastModified"])
    r = lam.invoke(FunctionName="justhodl-portfolio-snapshot", InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read()
    expect(r.get("StatusCode") == 200 and not r.get("FunctionError"), "snapshot invoke ok (FunctionError=%s) %s" % (r.get("FunctionError"), body[:160]))
    snap = json.loads(s3.get_object(Bucket=BUCKET, Key="portfolio/snapshot.json")["Body"].read())
    ps = snap.get("positions") or []
    summ = snap.get("portfolio_summary") or {}
    R.log("positions %d | summary keys %s" % (len(ps), sorted(summ.keys())))
    expect("pnl_scope" in summ and "unpriced_positions" in summ and "stops_not_evaluable" in summ, "summary carries pnl_scope / unpriced_positions / stops_not_evaluable")
    bad = []
    for p in ps:
        vs = p.get("valuation_status")
        if vs not in ("PRICED", "STALE_MARK", "UNPRICED"):
            bad.append((p.get("symbol"), "valuation_status", vs)); continue
        if (p.get("market_value") is None) != (vs != "PRICED"):
            bad.append((p.get("symbol"), "market_value/valuation mismatch", vs, p.get("market_value")))
        q = float(p.get("qty") or 0)
        if p.get("position_type") != ("LONG" if q >= 0 else "SHORT"):
            bad.append((p.get("symbol"), "side", p.get("position_type"), q))
        if abs(float(p.get("cost_basis_total") or 0) - q * float(p.get("cost_basis_per_share") or 0)) > 0.02:
            bad.append((p.get("symbol"), "basis", p.get("cost_basis_total"), q, p.get("cost_basis_per_share")))
        if p.get("stop_loss") is not None and vs != "PRICED" and p.get("stop_hit") is not None:
            bad.append((p.get("symbol"), "stop evaluated without a mark"))
    expect(not bad, "every position obeys the valuation/side/basis contract (%d violations) %s" % (len(bad), bad[:5]))
    R.kv(step="snapshot", positions=len(ps), priced=sum(1 for p in ps if p.get("valuation_status") == "PRICED"), unpriced=len(summ.get("unpriced_positions") or []),
         pnl=summ.get("total_pnl_dollars"), stops_hit=summ.get("stops_hit_count"), stops_not_evaluable=len(summ.get("stops_not_evaluable") or []))

    R.section("verdict")
    for f in FAILS:
        R.fail(f)
    if FAILS:
        sys.exit(1)
    R.ok("GREEN -- portfolio edits cannot fabricate P&L or invert stops; missing marks are unpriced exposure, never cost")
