"""ops 3598 — FI+FX vol-migration barometer live: deploy, schedule 21:20 UTC,
invoke → gates demand REAL z-scored legs (MOVE or fallback + FX realized
composite + VIX), a coherent migration state, ratios, page card, MI marker."""
import io, json, sys, time, urllib.request, zipfile
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
FN = "justhodl-fifx-vol-migration"

with report("3598_fifx_vol") as rep:
    rep.heading("ops 3598 — vol migration barometer (FI+FX → equities)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:480]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:440]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    try:
        env = {"FRED_API_KEY": "2f057499936072679d8843d7fce99989"}
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / "justhodl-fifx-vol-migration" / "source",
                      env_vars=env, timeout=180, memory=512,
                      description="Cross-asset vol migration barometer: z-scored MOVE + FX realized (G3) vs VIX; spillover gauge (UPSTREAM_BREWING = the early warning).",
                      create_function_url=False)
        names = []
        for pg in SCH.get_paginator("list_schedules").paginate():
            names += [s0["Name"] for s0 in pg.get("Schedules", []) if "fifx" in s0["Name"]]
        if not names:
            arn = LAM.get_function_configuration(FunctionName=FN)["FunctionArn"]
            SCH.create_schedule(Name="justhodl-fifx-vol-daily",
                                ScheduleExpression="cron(20 21 ? * MON-FRI *)",
                                FlexibleTimeWindow={"Mode": "OFF"},
                                Target={"Arn": arn,
                                        "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                                        "Input": "{}"},
                                State="ENABLED", Description="FI+FX vol migration barometer daily")
        gate("G1_deploy_schedule", True, f"deployed; schedule={'kept' if names else 'created 21:20 M-F'}")
    except Exception as e:
        gate("G1_deploy_schedule", False, str(e)[:300])

    try:
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G2_legs_real", False, "fn error: " + pl["errorMessage"][:240])
        else:
            j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/fifx-vol.json")["Body"].read())
            L = j.get("legs") or {}
            fi, fx, eq = L.get("fixed_income") or {}, L.get("fx") or {}, L.get("equity") or {}
            m = j.get("migration") or {}
            ok2 = all(isinstance(x.get("z"), (int, float)) for x in (fi, fx, eq)) \
                  and m.get("state") in ("CALM", "UPSTREAM_BREWING", "MIGRATING", "BROAD_STRESS") \
                  and isinstance(m.get("spillover"), (int, float))
            gate("G2_legs_real", ok2,
                 f"FI {fi.get('measure','')[:14]} {fi.get('level')} z={fi.get('z')} · "
                 f"FX {fx.get('level_pct')}% z={fx.get('z')} (EUR {((fx.get('pairs') or {}).get('EURUSD') or {}).get('realized_20d_pct')} "
                 f"JPY {((fx.get('pairs') or {}).get('USDJPY') or {}).get('realized_20d_pct')} "
                 f"GBP {((fx.get('pairs') or {}).get('GBPUSD') or {}).get('realized_20d_pct')}) · "
                 f"VIX {eq.get('level')} z={eq.get('z')} · spill={m.get('spillover')} state={m.get('state')}")
            rt = j.get("ratios") or {}
            gate("G3_ratios", isinstance((rt.get("move_vix") or {}).get("last"), (int, float))
                 and isinstance((rt.get("fxvol_vix") or {}).get("last"), (int, float)),
                 f"MOVE/VIX {(rt.get('move_vix') or {}).get('last')} ({(rt.get('move_vix') or {}).get('pctile')}p) · "
                 f"FXvol/VIX {(rt.get('fxvol_vix') or {}).get('last')} ({(rt.get('fxvol_vix') or {}).get('pctile')}p)")
            out["snapshot"] = {"migration": m, "fi": {k: fi.get(k) for k in ("level", "z", "pctile")},
                               "fx": {k: fx.get(k) for k in ("level_pct", "z", "pctile")},
                               "eq": {k: eq.get(k) for k in ("level", "z", "pctile")}}
    except Exception as e:
        gate("G2_legs_real", False, str(e)[:320])

    ok4 = False; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/signal-board.html",
                    headers={"User-Agent": "Mozilla/5.0 (ops)"}), timeout=30) as r:
                if "Vol Migration Barometer" in r.read().decode("utf-8", "replace"):
                    ok4 = True; break
        except Exception:
            pass
        time.sleep(15)
    gate("G4_page_card", ok4, "served: Vol Migration Barometer card")

    try:
        info = LAM.get_function(FunctionName="justhodl-morning-intelligence")
        with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],
                headers={"User-Agent": "Mozilla/5.0"}), timeout=60) as r:
            src = zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")
        gate("G5_mi_feed", '"data/fifx-vol.json"' in src, "fifx_vol feed in deployed MI (zip marker)")
    except Exception as e:
        gate("G5_mi_feed", False, str(e)[:200])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3598.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
