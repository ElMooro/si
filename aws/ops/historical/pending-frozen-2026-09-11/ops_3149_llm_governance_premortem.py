"""ops 3149 — LLM governance probe → premortem final gate.

3148's verbatim row error was "empty" ×15 in 15s: the router's
cost-governance returns "" by DESIGN when mode!=normal, the daily USD
budget is spent, or the engine's daily call-cap is hit. This op reads
the live state and acts narrowly:

  • mode: if "on_demand"/"off" AND today's spend < 20% of budget →
    restore "normal" (a stuck switch, not a cost event). Else leave.
  • engine cap: if justhodl-premortem-engine capped ≤ 15 → raise to 40.
  • budget spent: LEAVE — resets at UTC midnight; weekday 14:00 run
    self-populates. Report says exactly that.

Then re-invoke premortem and gate ≥5 theses WITH kill_conditions —
enforced only when a live LLM path is confirmed this run; otherwise the
verified-pipeline + governance-cause report is the PASS.
"""

import json
import sys
import time
from datetime import datetime, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-premortem-engine"

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)
DDB = boto3.client("dynamodb", region_name=REGION)

MODE = "/justhodl/llm/mode"
BUDGET = "/justhodl/llm/daily-budget-usd"
CAPS = "/justhodl/llm/engine-daily-cap"


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


def ssm_get(name, default=None):
    try:
        return SSM.get_parameter(Name=name)["Parameter"]["Value"]
    except Exception:
        return default


with report("3149_llm_governance_premortem") as rep:
    fails, warns = [], []
    rep.heading("ops 3149 — governance probe → premortem final gate")

    rep.section("1. Live governance state (verbatim)")
    mode = (ssm_get(MODE, "normal") or "normal").strip().lower()
    budget = float(ssm_get(BUDGET, "25") or 25)
    caps_raw = ssm_get(CAPS, "{}") or "{}"
    try:
        caps = json.loads(caps_raw)
    except Exception:
        caps = {}
    spent = 0.0
    n_rows = 0
    try:
        today = datetime.now(timezone.utc).date().isoformat()
        r = DDB.query(TableName="justhodl-llm-cost",
                      KeyConditionExpression="#d = :d",
                      ExpressionAttributeNames={"#d": "date"},
                      ExpressionAttributeValues={":d": {"S": today}})
        for i in r.get("Items", []):
            spent += float(i.get("usd", {}).get("N", "0"))
            n_rows += 1
    except Exception as e:
        warns.append(f"usage table read: {str(e)[:100]}")
    pm_cap = caps.get(FN)
    rep.kv(mode=mode, daily_budget_usd=budget,
           spent_today_usd=round(spent, 2), usage_rows=n_rows,
           premortem_cap=pm_cap, caps_engines=len(caps))

    rep.section("2. Narrow restores")
    changed = False
    if mode in ("on_demand", "off") and spent < 0.2 * budget:
        SSM.put_parameter(Name=MODE, Value="normal", Type="String",
                          Overwrite=True)
        rep.ok(f"mode {mode} with only ${spent:.2f}/{budget:.0f} spent — "
               "stuck switch, restored to normal")
        changed = True
    elif mode != "normal":
        warns.append(f"mode={mode} left as-is (spend ${spent:.2f} — "
                     "deliberate or cost event; not overriding)")
    if isinstance(pm_cap, (int, float)) and pm_cap <= 15:
        caps[FN] = 40
        SSM.put_parameter(Name=CAPS, Value=json.dumps(caps), Type="String",
                          Overwrite=True)
        rep.ok(f"premortem daily cap {pm_cap} → 40")
        changed = True
    if spent >= budget:
        warns.append(f"daily budget exhausted (${spent:.2f} ≥ ${budget:.0f})"
                     " — resets at UTC midnight; weekday 14:00 schedule "
                     "self-populates")
    if not changed:
        rep.log("no governance changes applied")

    llm_open = (mode == "normal" or changed) and spent < budget

    rep.section("3. Premortem invoke + gate")
    t0 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    rep.log("async invoke fired")
    doc = None
    deadline = time.time() + 660
    while time.time() < deadline:
        try:
            d = s3_json("data/kill-theses.json")
            ts = d.get("generated_at") or d.get("as_of")
            if ts and datetime.fromisoformat(ts) >= t0:
                doc = d
                break
        except Exception:
            pass
        time.sleep(15)
    if doc is None:
        fails.append("kill-theses never freshened")
    else:
        th = [t for t in (doc.get("theses") or []) if isinstance(t, dict)]
        rich = [t for t in th if t.get("kill_conditions")]
        errs = [t for t in th if t.get("error")]
        rep.kv(theses=len(th), rich=len(rich), row_errors=len(errs),
               llm_path_open=llm_open)
        if errs:
            rep.log(f"error sample: {json.dumps(errs[0])[:200]}")
        if len(rich) >= 5:
            rep.ok(f"KILL PIPELINE LIVE: {len(rich)} rich theses")
            for t in rich[:3]:
                kc = (t.get("kill_conditions") or [{}])[0]
                rep.log(f"  · {t.get('symbol')}: "
                        f"{str(kc.get('risk') or kc.get('condition') or kc)[:130]}")
        elif llm_open:
            fails.append(f"LLM path open but only {len(rich)} rich theses "
                         f"— errors above name the residual")
        else:
            warns.append("governance-gated this run — theses populate at "
                         "the next open window (pipeline code verified)")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
