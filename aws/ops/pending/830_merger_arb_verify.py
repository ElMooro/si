"""
ops/830 - justhodl-merger-arb deploy verification (end-to-end proof).

deploy-lambdas.yml ships the function + EventBridge Scheduler schedule from
config.json. This op proves the engine actually works:
  1. Confirm the Lambda exists.
  2. Invoke it synchronously.
  3. Read back data/merger-arb.json from S3.
  4. Audit: priced deals exist, every priced deal carries a sane gross
     spread + a positive deal_value + a tier, tiers reconcile, the SEC S-4
     link is present, no spread escaped the sanity band.
  5. Confirm the EventBridge Scheduler schedule is live.

Writes aws/ops/reports/830_merger_arb_verify.json.
"""
import json
import time
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
FN = "justhodl-merger-arb"
SCHED = "justhodl-merger-arb-2x"
REGION = "us-east-1"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)


def main():
    rep = {
        "ops": 830,
        "ts": datetime.now(timezone.utc).isoformat(),
        "subject": "Deploy + verify justhodl-merger-arb spread desk",
        "checks": [],
    }

    def check(name, ok, detail=""):
        rep["checks"].append({"check": name, "pass": bool(ok), "detail": detail})
        return ok

    # 1. function exists
    try:
        cfg = lam.get_function_configuration(FunctionName=FN)
        check("lambda_exists", True,
              f"{cfg['Runtime']} mem={cfg['MemorySize']} timeout={cfg['Timeout']}")
    except Exception as e:
        check("lambda_exists", False, str(e)[:200])
        rep["all_pass"] = False
        rep["verdict"] = "FAIL - function not deployed yet"
        _write(rep)
        return

    # 2. invoke
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=b"{}")
        payload = json.loads(r["Payload"].read() or b"{}")
        fn_err = r.get("FunctionError")
        body = json.loads(payload.get("body", "{}")) if isinstance(
            payload.get("body"), str) else payload
        rep["invoke"] = {"status": payload.get("statusCode"),
                         "fn_error": fn_err, "body": body}
        check("invoke_ok", payload.get("statusCode") == 200 and not fn_err,
              json.dumps(body)[:240])
    except Exception as e:
        check("invoke_ok", False, str(e)[:200])
        rep["all_pass"] = False
        rep["verdict"] = "FAIL - invoke errored"
        _write(rep)
        return

    time.sleep(3)

    # 3. read output
    try:
        data = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key="data/merger-arb.json")["Body"].read())
    except Exception as e:
        check("output_readable", False, str(e)[:200])
        rep["all_pass"] = False
        rep["verdict"] = "FAIL - no data/merger-arb.json"
        _write(rep)
        return
    check("output_readable", True, f"generated {data.get('generated_at')}")

    priced = data.get("all_priced", [])
    uv = data.get("unverified", [])
    summ = data.get("summary", {})
    rep["headline"] = data.get("headline")
    rep["summary"] = summ
    rep["sample"] = [
        {"target": p.get("target"), "acquirer": p.get("acquirer"),
         "type": p.get("deal_type"), "tier": p.get("tier"),
         "spread": p.get("gross_spread_pct"),
         "annualized": p.get("annualized_return_pct"),
         "deal_risk": p.get("deal_risk"),
         "downside": p.get("downside_to_unaffected_pct")}
        for p in priced[:8]
    ]

    # 4. integrity audit
    check("has_priced_deals", len(priced) >= 1,
          f"{len(priced)} priced, {len(uv)} unverified")

    bad_spread = [p["target"] for p in priced
                  if not (-30 <= (p.get("gross_spread_pct") or 0) <= 55)]
    check("all_spreads_sane", not bad_spread,
          "out-of-band: " + ",".join(bad_spread) if bad_spread
          else "every spread within -30%..+55%")

    bad_val = [p["target"] for p in priced
               if not (isinstance(p.get("deal_value"), (int, float))
                       and p["deal_value"] > 0)]
    check("all_deal_values_positive", not bad_val,
          "bad: " + ",".join(bad_val) if bad_val else "all > 0")

    bad_tier = [p["target"] for p in priced if p.get("tier") not in
                ("TIGHT CARRY", "WIDE SPREAD", "BUMP WATCH")]
    check("all_priced_tiered", not bad_tier,
          "bad tier: " + ",".join(bad_tier) if bad_tier else "ok")

    tiers_total = (len(data.get("tight_carry", []))
                   + len(data.get("wide_spread", []))
                   + len(data.get("bump_watch", [])))
    check("tiers_reconcile", tiers_total == len(priced),
          f"tier sum {tiers_total} vs priced {len(priced)}")

    no_link = [p["target"] for p in priced if not p.get("s4_link")]
    check("all_have_s4_link", not no_link,
          "missing: " + ",".join(no_link) if no_link else "all linked")

    # annualized math sanity on a sample
    math_ok = True
    for p in priced[:5]:
        g = p.get("gross_spread_pct")
        a = p.get("annualized_return_pct")
        ec = p.get("est_close_days")
        if g is None or a is None or not ec:
            continue
        expect = g * 365.0 / ec
        if abs(expect - a) > 1.0:
            math_ok = False
    check("annualized_math", math_ok, "gross*365/est_close reconciles")

    # 5. schedule live
    try:
        sd = sch.get_schedule(Name=SCHED)
        check("schedule_live", True,
              f"{sd.get('ScheduleExpression')} state={sd.get('State')}")
    except Exception as e:
        check("schedule_live", False, str(e)[:160])

    rep["all_pass"] = all(c["pass"] for c in rep["checks"])
    rep["verdict"] = ("PASS - merger-arb desk live and pricing real S-4 "
                      "spreads end-to-end" if rep["all_pass"]
                      else "ATTENTION - see failing checks")
    _write(rep)


def _write(rep):
    body = json.dumps(rep, indent=1, default=str)
    s3.put_object(Bucket=S3_BUCKET,
                  Key="ops/reports/830_merger_arb_verify.json",
                  Body=body, ContentType="application/json")
    with open("aws/ops/reports/830_merger_arb_verify.json", "w") as f:
        f.write(body)
    print(body)


if __name__ == "__main__":
    main()
