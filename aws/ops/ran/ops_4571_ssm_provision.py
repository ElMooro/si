"""ops 4571 — SSM provisioning for the FRED priority-drain v2 engine.
The v2 importer sources its key SSM-first (the current key is hardcoded
in the PUBLIC repo's config.json + source default — that is the leak
feeding stranger 429s). This op makes /justhodl/fred-api-key hold a
working value BEFORE the v2 deploy drops the key from config/env, and
creates the three live control knobs (rate ceiling, min-popularity cut,
pause). When Khalid mints a fresh key, one put_parameter swaps it with
zero deploys."""
import json
import sys
from datetime import datetime, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
WORKING_KEY = "2f057499936072679d8843d7fce99989"  # current (burned but live)
KNOBS = {"/justhodl/fred/rate-ceiling": "100",
         "/justhodl/fred/min-popularity": "-1",
         "/justhodl/fred/paused": "0"}

ssm = boto3.client("ssm", region_name=REGION)
R = {"ops": 4571, "at": datetime.now(timezone.utc).isoformat()}
FAILS = []

try:
    with report("4571_ssm_provision") as r:
        r.heading("ops 4571 — SSM key + knobs for priority-drain v2")
        cur = None
        try:
            cur = ssm.get_parameter(Name="/justhodl/fred-api-key",
                                    WithDecryption=True
                                    )["Parameter"]["Value"]
        except ssm.exceptions.ParameterNotFound:
            cur = None
        if cur and len(cur) == 32 and cur != "PLACEHOLDER":
            R["key"] = f"already set (len=32, tail ...{cur[-4:]})"
            r.ok(R["key"])
        else:
            ssm.put_parameter(Name="/justhodl/fred-api-key",
                              Value=WORKING_KEY, Type="SecureString",
                              Overwrite=True)
            back = ssm.get_parameter(Name="/justhodl/fred-api-key",
                                     WithDecryption=True
                                     )["Parameter"]["Value"]
            if back != WORKING_KEY:
                FAILS.append("key readback mismatch")
            R["key"] = f"written+verified (tail ...{back[-4:]})"
            r.ok(R["key"])
        for name, val in KNOBS.items():
            try:
                ex = ssm.get_parameter(Name=name)["Parameter"]["Value"]
                R[name] = f"exists={ex}"
                r.log(f"{name}: exists ({ex}) — left intact")
            except ssm.exceptions.ParameterNotFound:
                ssm.put_parameter(Name=name, Value=val,
                                  Type="String", Overwrite=False)
                R[name] = f"created={val}"
                r.ok(f"{name}: created = {val}")
        if FAILS:
            r.fail(" | ".join(FAILS))
        else:
            r.ok("SSM ready — v2 deploy can drop the key from "
                 "config/env safely")
except Exception as e:
    import os
    import traceback
    R["error"] = f"{type(e).__name__}: {e}"
    R["trace"] = traceback.format_exc()[-1200:]
    os.makedirs("aws/ops/reports", exist_ok=True)
    json.dump(R, open("aws/ops/reports/4571.json", "w"), indent=1,
              default=str)
    open("aws/ops/reports/4571.md", "w").write(
        "# 4571 FAIL — " + R["error"] + "\n")
    print("FAIL", R["error"])
    sys.exit(1)

import os

os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4571.json", "w"), indent=1,
          default=str)
verdict = "PASS" if not FAILS else "FAIL: " + " | ".join(FAILS)
open("aws/ops/reports/4571.md", "w").write("# 4571 — " + verdict +
                                           " " + json.dumps(
                                               {k: v for k, v in
                                                R.items() if k not in
                                                ("ops", "at")},
                                               default=str)[:400] +
                                           "\n")
print(verdict)
if FAILS:
    sys.exit(1)
sys.exit(0)
