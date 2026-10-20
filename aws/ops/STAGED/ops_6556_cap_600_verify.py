"""ops 6556 - verify governor v1.2.0 on the $600 cap, run the cadence sentinel live, retire the -500 budget.

  1. {"action":"status"}: version 1.2.0, cap 600 from budget JustHodl-Monthly-Hard-Cap-600, mode enforce, stage FREEZE
     (October stays frozen; the cap applies from the 2026-11-01 00:07 UTC tick), lines for November computed here.
  2. {"action":"cadence_check"}: the sentinel walks the live account (ops 6554 left it clean: expect 0 violators).
  3. {"action":"fuse_check"}: the hourly fuse path runs with the new diffuse logic (frozen fleet: expect nothing hot).
  4. Only when 1-3 pass: delete JustHodl-Monthly-Hard-Cap-500 (its five alerts duplicated the -600 budget's).
"""
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-spend-governor" / "source"))
from ops_report import report  # noqa: E402
import governor_core as core  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
OLD, NEW = "JustHodl-Monthly-Hard-Cap-500", "JustHodl-Monthly-Hard-Cap-600"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, read_timeout=600)


def gov(payload):
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    for attempt in range(8):                       # reserved concurrency 1: a running tick answers 429
        try:
            r = lam.invoke(FunctionName="justhodl-spend-governor", Payload=json.dumps(payload).encode())
            body = json.loads(r["Payload"].read() or b"{}")
            if r.get("FunctionError"):
                raise RuntimeError("governor error: %s" % json.dumps(body)[:400])
            return body
        except lam.exceptions.TooManyRequestsException:
            import time
            time.sleep(20)
    raise RuntimeError("governor busy for 160 s")


def main():
    with report("ops_6556_cap_600_verify") as r:
        r.heading("ops 6556 - governor v1.2.0 on the $600 cap: verify, sentinel live run, retire the -500 budget")
        st = gov({"action": "status"})
        th = st.get("thresholds") or {}
        r.kv(version=st.get("version"), mode=st.get("mode"), stage=st.get("stage"), cap=st.get("cap"),
             cap_source=st.get("cap_source"), budget_limit=st.get("budget_limit"), budget_actual=st.get("budget_actual"),
             held=json.dumps(st.get("held")))
        r.log("thresholds now (October override): %s" % json.dumps({k: th.get(k) for k in (
            "cap", "mtd", "freeze_at", "brake_at", "baseline_usd_day", "residual_usd_day", "days_left")}))
        ok = st.get("version") == "1.2.0" and float(st.get("budget_limit") or 0) == 600.0 and st.get("mode") == "enforce"
        # November lines on the $600 cap with the current baseline / residual
        from datetime import datetime, timezone
        nov = datetime(2026, 11, 1, 0, 7, tzinfo=timezone.utc)
        cfg = core.merged_cfg()
        base, resid = float(th.get("baseline_usd_day") or 20), float(th.get("residual_usd_day") or 3)
        lines = core.thresholds(600.0, 0.0, nov, base, resid, cfg)
        r.log("November on $600 at today's baseline $%.2f/day, residual $%.2f/day: brake at $%.0f, freeze at $%.0f, "
              "projected month $%.0f" % (base, resid, lines["brake_at"], lines["freeze_at"], lines["projected_eom"]))
        for b, rs in ((10.0, 3.0), (14.0, 3.0), (18.0, 3.0)):
            l2 = core.thresholds(600.0, 0.0, nov, b, rs, cfg)
            r.log("  if November runs at $%.0f/day: brake $%.0f, freeze $%.0f, month ~$%.0f" % (b, l2["brake_at"], l2["freeze_at"], l2["projected_eom"]))
        if not ok:
            r.fail("status does not show v1.2.0 on the $600 budget in enforce mode")
            raise SystemExit(1)
        r.ok("governor v1.2.0 reads %s (limit $600), mode enforce, stage %s" % (NEW, st.get("stage")))

        cad = gov({"action": "cadence_check"})
        r.kv(cadence_checked=cad.get("checked"), cadence_violators=cad.get("violators"), cadence_fixed=cad.get("fixed"),
             cadence_errors=cad.get("errors"))
        for l in cad.get("logs") or []:
            r.log(l)
        if cad.get("error") or cad.get("errors"):
            r.fail("cadence sentinel reported errors: %s" % json.dumps(cad)[:500])
            raise SystemExit(1)
        r.ok("cadence sentinel walked %s enabled triggers live: %s faster than daily" % (cad.get("checked"), cad.get("violators")))

        fz = gov({"action": "fuse_check"})
        r.log("fuse_check: %s" % json.dumps({k: fz.get(k) for k in ("checked", "firing", "runaway", "new", "suspects")}))
        if "error" in fz:
            r.fail("fuse path error: %s" % fz["error"])
            raise SystemExit(1)

        b = boto3.client("budgets", region_name=REGION, config=CFG)
        try:
            b.describe_budget(AccountId=ACCOUNT, BudgetName=OLD)
            b.delete_budget(AccountId=ACCOUNT, BudgetName=OLD)
            r.ok("retired %s (the governor and all five alerts now live on %s)" % (OLD, NEW))
        except b.exceptions.NotFoundException:
            r.log("%s already gone" % OLD)
        names = sorted(x["BudgetName"] for x in b.describe_budgets(AccountId=ACCOUNT)["Budgets"])
        r.kv(budgets=", ".join(names))
        r.ok("$600 cap live from 2026-11-01 00:07 UTC (October stays frozen under the $3,500 override)")


if __name__ == "__main__":
    main()
