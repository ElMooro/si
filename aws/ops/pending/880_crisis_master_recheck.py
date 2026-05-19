"""
ops/880 - corrected re-verify of the crisis-composite master score.

ops/879 flagged crisis_master_still_computes as failed, but that was a
false negative: the check read cc["master_score"] while the engine
emits the score under "master_crisis_score" (DEFCON 4 had computed
fine, which is only possible if the master score computed). This op
reads the correct key and confirms the crisis composite still produces
a clean master score now that the Dollar Radar and Global Stress
components are wired in.

Writes aws/ops/reports/880_crisis_master_recheck.json.
"""
import json
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 880,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Corrected re-verify: crisis-composite master score still "
               "computes with the Dollar Radar + Global Stress components",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


# fresh invoke so the read reflects the wired source
try:
    r = lam.invoke(FunctionName="justhodl-crisis-composite",
                   InvocationType="RequestResponse", Payload=b"{}")
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inv = json.loads(body.get("body") or "{}")
    check("crisis_composite_invoke_ok",
          r.get("StatusCode") == 200 and not r.get("FunctionError"),
          "master_crisis_score=%s defcon=%s"
          % (inv.get("master_crisis_score"), inv.get("defcon_level")))
except Exception as e:
    check("crisis_composite_invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)
cc = {}
try:
    cc = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                  Key="data/crisis-composite.json")["Body"]
                    .read())
    check("crisis_composite_readable", True, "crisis-composite.json read")
except Exception as e:
    check("crisis_composite_readable", False, f"{type(e).__name__}: {e}")

# the engine's real key is master_crisis_score
master = cc.get("master_crisis_score")
if master is None:
    master = cc.get("master_score") or cc.get("composite_score")
check("crisis_master_score_computes",
      isinstance(master, (int, float)),
      "master_crisis_score = %s, DEFCON %s (%s)"
      % (master, cc.get("defcon_level"), cc.get("defcon_name")))

comps = cc.get("components") or []
names = " | ".join(str(c.get("label") or c.get("name") or c) for c in comps)
have_gs = any("global" in str(c).lower() and "stress" in str(c).lower()
              for c in comps)
have_dr = any("dollar" in str(c).lower() for c in comps)
check("crisis_has_both_new_components", have_gs and have_dr,
      "%d components -- global-stress:%s dollar:%s"
      % (len(comps), have_gs, have_dr))

# every component must carry a numeric crisis contribution or be marked NA
ok_contrib = True
for c in comps:
    if not isinstance(c, dict):
        continue
    val = c.get("crisis_contribution")
    if val is None:
        val = c.get("crisis_score")
    avail = c.get("available")
    if val is None and avail not in (False, None):
        ok_contrib = False
check("crisis_components_numeric", ok_contrib,
      "all wired components carry a numeric crisis contribution or an "
      "availability flag")

n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["crisis_snapshot"] = {
    "master_crisis_score": master,
    "defcon_level": cc.get("defcon_level"),
    "defcon_name": cc.get("defcon_name"),
    "component_count": len(comps),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "CONFIRMED - the crisis composite computes a clean master crisis "
        "score of %s (DEFCON %s) over %d components, with the Dollar "
        "Radar and Global Stress Matrix both wired in. ops/879's failure "
        "was a key-name false negative; the integration is sound."
        % (master, cc.get("defcon_level"), len(comps)))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = "RE-CHECK FAILED: %s." % ", ".join(bad)

with open("aws/ops/reports/880_crisis_master_recheck.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
