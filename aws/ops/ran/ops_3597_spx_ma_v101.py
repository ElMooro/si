"""ops 3597 — justhodl-spx-ma v1 live: the dedicated S&P 500 MA engine (index
20/50/100/200 ladder from existing spx-history + live tape; true 503-member
breadth: 50/200 via batch quotes, 20/100 via self-building grouped-daily
ledger). Deploy + schedule + sync-invoke (bootstrap runs inside) + gates."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=920, retries={"max_attempts": 0}))
SCH = boto3.client("scheduler", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-spx-ma"

with report("3597_spx_ma_v101") as rep:
    rep.heading("ops 3597 — S&P 500 MA Command engine")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:480]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:440]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    try:
        src_env = (LAM.get_function_configuration(FunctionName="justhodl-deal-scanner")
                   .get("Environment") or {}).get("Variables") or {}
        env = {"FMP_API_KEY": src_env.get("FMP_API_KEY") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
               "POLYGON_API_KEY": src_env.get("POLYGON_API_KEY") or "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"}
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / "justhodl-spx-ma" / "source",
                      env_vars=env, timeout=900, memory=1024,
                      description="Dedicated S&P 500 MA engine: ^GSPC 20/50/100/200 ladder + true membership breadth (self-building 20/100 ledger).",
                      create_function_url=False)
        names = []
        for pg in SCH.get_paginator("list_schedules").paginate():
            names += [s0["Name"] for s0 in pg.get("Schedules", []) if "spx-ma" in s0["Name"]]
        if not names:
            arn = LAM.get_function_configuration(FunctionName=FN)["FunctionArn"]
            SCH.create_schedule(Name="justhodl-spx-ma-daily",
                                ScheduleExpression="cron(15 21 ? * MON-FRI *)",
                                FlexibleTimeWindow={"Mode": "OFF"},
                                Target={"Arn": arn,
                                        "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                                        "Input": "{}"},
                                State="ENABLED", Description="SPX MA ladder + breadth, daily post-close")
        gate("G1_deploy_schedule", True, f"deployed 1024/900; schedule={'kept' if names else 'created 21:15 M-F'}")
    except Exception as e:
        gate("G1_deploy_schedule", False, str(e)[:300])

    try:
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G2_index_ladder", False, "fn error: " + pl["errorMessage"][:240])
        else:
            j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/spx-ma.json")["Body"].read())
            ix = j.get("index") or {}
            mas = ix.get("sma") or {}
            ok2 = all(isinstance(mas.get(k), (int, float)) for k in ("20", "50", "100", "200")) \
                  and isinstance(ix.get("price"), (int, float))
            gate("G2_index_ladder", ok2,
                 f"px={ix.get('price')} stack={ix.get('stack')} sma200={mas.get('200')} "
                 f"dist200={((ix.get('distance_pct') or {}).get('200'))}% cross={(ix.get('cross_50x200') or {}).get('state')} "
                 f"{(ix.get('cross_50x200') or {}).get('days_since_flip')}d regime={ix.get('regime')} "
                 f"compression={ix.get('ma_compression_pct')}%")
            b = j.get("breadth") or {}
            ok3 = (b.get("above200_covered") or 0) >= 300 and isinstance(b.get("above200_pct"), (int, float))
            gate("G3_breadth_50_200", ok3,
                 f"cov200={b.get('above200_covered')}/{b.get('n_members')} above50={b.get('above50_pct')}% "
                 f"above200={b.get('above200_pct')}% spread={b.get('spread_50_200')} narrow={b.get('divergence_narrow_market')}")
            w = b.get("warming") or {}
            gate("G4_ledger_bootstrap", (w.get("ledger_days") or 0) >= 200,
                 f"ledger={w.get('ledger_days')}/{w.get('target_days')} added={w.get('added_this_run')} "
                 f"b20={b.get('above20_pct')}% (cov {b.get('above20_covered')}) b100={b.get('above100_pct')} "
                 f"(cov {b.get('above100_covered')})")
            out["snapshot"] = {"index": {k: ix.get(k) for k in ("price", "stack", "regime")},
                               "breadth": {k: b.get(k) for k in ("above20_pct", "above50_pct",
                                           "above100_pct", "above200_pct", "divergence_narrow_market")}}
    except Exception as e:
        gate("G2_index_ladder", False, str(e)[:320])

    ok5 = False; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/signal-board.html",
                    headers={"User-Agent": "Mozilla/5.0 (ops)"}), timeout=30) as r:
                if "MA Command" in r.read().decode("utf-8", "replace"):
                    ok5 = True; break
        except Exception:
            pass
        time.sleep(15)
    gate("G5_page_card", ok5, "served: S&P 500 MA Command card on signal-board")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3597.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
