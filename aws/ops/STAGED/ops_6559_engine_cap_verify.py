"""ops 6559 - verify governor v1.3.0 live: the per-engine $50/month kill switch (Khalid, 2026-10-20).

  1. status: version 1.3.0, cap $600, mode enforce, engine_cap_usd_month 50
  2. {"action":"meter"}: a forced month-to-date read of every function (the fleet is frozen, so the tick skips it)
  3. {"action":"cap_selftest"}: the canary is capped (held at 0, ledger stage CAP), then released through the only
     path that can release it; while the fleet is frozen it is handed back to the FREEZE hold
  4. {"action":"caps"}: nothing real is held by the cap yet
"""
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, read_timeout=600)


def gov(payload):
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    for attempt in range(8):
        try:
            r = lam.invoke(FunctionName="justhodl-spend-governor", Payload=json.dumps(payload).encode())
            body = json.loads(r["Payload"].read() or b"{}")
            if r.get("FunctionError"):
                raise RuntimeError("governor error: %s" % json.dumps(body)[:400])
            return body
        except lam.exceptions.TooManyRequestsException:
            time.sleep(20)
    raise RuntimeError("governor busy for 160 s")


def main():
    with report("ops_6559_engine_cap_verify") as r:
        r.heading("ops 6559 - governor v1.3.0: per-engine $50/month kill switch, verified live")
        st = gov({"action": "status"})
        r.kv(version=st.get("version"), mode=st.get("mode"), stage=st.get("stage"), cap=st.get("cap"),
             engine_cap_usd_month=st.get("engine_cap_usd_month"), capped=json.dumps(st.get("capped")))
        if st.get("version") != "1.3.0" or st.get("engine_cap_usd_month") != 50.0:
            r.fail("status does not show v1.3.0 with a $50 engine cap")
            raise SystemExit(1)
        m = gov({"action": "meter"})
        r.log("meter: ran=%s window %.1f h reconciled=%s active=%s reads=%s fleet $%s/day" % (
            m.get("ran"), m.get("window_h") or 0, m.get("reconciled"), m.get("active"), m.get("reads"), m.get("fleet_usd_day")))
        r.log("month-to-date top: %s" % json.dumps(m.get("month_to_date_top")))
        for l in m.get("logs") or []:
            r.log(l)
        if not m.get("ran"):
            r.fail("meter did not read")
            raise SystemExit(1)
        t = gov({"action": "cap_selftest"})
        r.log("cap_selftest: %s" % json.dumps(t)[:600])
        if not t.get("ok"):
            r.fail("cap self-test failed")
            raise SystemExit(1)
        r.ok("kill switch proven on the canary: held at 0 under CAP, released only by release_cap (%s)" % t["release"].get("status"))
        c = gov({"action": "caps"})
        r.kv(held_by_cap=json.dumps(c.get("held")), overrides=json.dumps(c.get("overrides")), meter_reads=(c.get("meter") or {}).get("reads"))
        r.ok("v1.3.0 live: every engine is metered; $50/month of Lambda cost switches it off until Khalid releases it")


if __name__ == "__main__":
    main()
