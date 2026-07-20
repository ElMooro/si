"""ops 3583 — build justhodl-asia-leads v1 (MacroMicro-gap engine from free
primaries) via the ops deploy helper (dodges the config-stomping workflow),
schedule daily, invoke sync, gate on REAL values in every block."""
import json, sys, time
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
SCH = boto3.client("scheduler", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-asia-leads"

with report("3583_asia_leads") as rep:
    rep.heading("ops 3583 — asia-leads v1 (China TSF · KR/TW exports · US calendar)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:460]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:420]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G1 deploy (env FRED copied from donor pattern: explicit key via env of confluence-meta)
    try:
        donor_env = (LAM.get_function_configuration(FunctionName="justhodl-confluence-meta")
                     .get("Environment") or {}).get("Variables") or {}
        env = {"FRED_API_KEY": donor_env.get("FRED_API_KEY") or "2f057499936072679d8843d7fce99989"}
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / "justhodl-asia-leads" / "source",
                      env_vars=env, timeout=180, memory=512,
                      description="Asia leads from free primaries: China TSF (NBS/DBnomics), KR+TW exports (FRED), US release calendar. MacroMicro gap-analysis engine.",
                      create_function_url=False)
        gate("G1_deployed", True, "deployed via ops helper (no workflow stomp)")
    except Exception as e:
        gate("G1_deployed", False, str(e)[:300])

    # G2 schedule (Scheduler; classic rule cap saturated)
    try:
        names = []
        for pg in SCH.get_paginator("list_schedules").paginate():
            names += [s0["Name"] for s0 in pg.get("Schedules", []) if "asia-leads" in s0["Name"]]
        if not names:
            arn = LAM.get_function_configuration(FunctionName=FN)["FunctionArn"]
            SCH.create_schedule(Name="justhodl-asia-leads-daily",
                                ScheduleExpression="cron(20 10 * * ? *)",
                                FlexibleTimeWindow={"Mode": "OFF"},
                                Target={"Arn": arn,
                                        "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                                        "Input": "{}"},
                                State="ENABLED",
                                Description="Asia leads daily 10:20 UTC")
            gate("G2_schedule", True, "created justhodl-asia-leads-daily cron(20 10 * * ? *)")
        else:
            gate("G2_schedule", True, f"existing kept: {names}")
    except Exception as e:
        gate("G2_schedule", False, str(e)[:200])

    # G3 sync invoke → real values in every block
    try:
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G3_feed_real", False, f"fn error: {pl['errorMessage'][:220]}")
        else:
            j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/asia-leads.json")["Body"].read())
            tsf = j.get("china_tsf") or {}
            kr, tw = j.get("korea_exports") or {}, j.get("taiwan_exports") or {}
            cal = j.get("us_calendar") or {}
            heads = [s0 for s0 in tsf.get("series") or [] if s0.get("is_headline")]
            ok = ((tsf.get("n_series") or 0) >= 3
                  and isinstance(kr.get("yoy_pct"), (int, float))
                  and isinstance(tw.get("yoy_pct"), (int, float))
                  and len(cal.get("high_impact") or []) >= 3)
            gate("G3_feed_real", ok,
                 f"tsf_series={tsf.get('n_series')} headline={[(h['last_period'], h['last_value']) for h in heads][:2]} "
                 f"kr_yoy={kr.get('yoy_pct')}% ({kr.get('last_period')}) tw_yoy={tw.get('yoy_pct')}% "
                 f"({tw.get('last_period')}) cal_hi={len(cal.get('high_impact') or [])} "
                 f"next_hi={[(c['date'], c['release'][:28]) for c in (cal.get('high_impact') or [])[:4]]}")
            out["snapshot"] = {"tsf_headline": heads[:2], "kr": {k: kr.get(k) for k in ("yoy_pct", "chg_3m_pct", "last_period")},
                               "tw": {k: tw.get(k) for k in ("yoy_pct", "chg_3m_pct", "last_period")},
                               "cal_next": (cal.get("high_impact") or [])[:8]}
    except Exception as e:
        gate("G3_feed_real", False, str(e)[:300])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3583.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
