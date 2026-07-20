"""ops 3590 — justhodl-industry-boom v1 live-proof: deploy via helper, schedule
daily, sync invoke → full-breadth league with REAL fused values (≥60 industries,
≥4 sources ok), history ledger seeded, page section served."""
import json, sys, time, urllib.request
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
FN = "justhodl-industry-boom"

with report("3590_boom_v11") as rep:
    rep.heading("ops 3590 — Industry Boom League v1")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:480]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:440]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / "justhodl-industry-boom" / "source",
                      env_vars={}, timeout=180, memory=1024,
                      description="Industry Boom League: full-breadth (~140 industries) fusion of internal alpha feeds — revisions, deal wins, backlog accel, 13F $, insider, census, dilution — into a daily boom score.",
                      create_function_url=False)
        names = []
        for pg in SCH.get_paginator("list_schedules").paginate():
            names += [s0["Name"] for s0 in pg.get("Schedules", []) if "industry-boom" in s0["Name"]]
        if not names:
            arn = LAM.get_function_configuration(FunctionName=FN)["FunctionArn"]
            SCH.create_schedule(Name="justhodl-industry-boom-daily",
                                ScheduleExpression="cron(50 10 * * ? *)",
                                FlexibleTimeWindow={"Mode": "OFF"},
                                Target={"Arn": arn,
                                        "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                                        "Input": "{}"},
                                State="ENABLED", Description="Industry boom league daily 10:50 UTC")
        gate("G1_deploy_schedule", True, f"deployed; schedule={'kept ' + str(names) if names else 'created daily 10:50'}")
    except Exception as e:
        gate("G1_deploy_schedule", False, str(e)[:300])

    try:
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G2_feed_real", False, "fn error: " + pl["errorMessage"][:240])
        else:
            j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/industry-boom.json")["Body"].read())
            L = j.get("league") or []
            srcok = sum(1 for v in (j.get("coverage") or {}).get("sources_ok", {}).values() if v)
            top = L[0] if L else {}
            comp_ok = bool(top) and top.get("n_component_families", 0) >= 3 and any(top.get("comp", {}).get(f) not in (None, 0)
                                        for f in ("rev_mean", "deal_wins_30d", "inst_net_bps", "census_conviction"))
            gate("G2_feed_real", len(L) >= 60 and srcok >= 4 and comp_ok,
                 f"industries={len(L)} sources_ok={srcok}/8 top3={[(x['industry'], x['boom_score']) for x in L[:3]]} "
                 f"top_comp={ {k: top.get('comp', {}).get(k) for k in ('rev_mean','deal_wins_30d','inst_net_bps','census_conviction','dilution_share')} }")
            out["top10"] = [(x["industry"], x["boom_score"]) for x in L[:10]]
            out["trouble"] = [(x["industry"],) for x in (j.get("trouble") or [])[:5]]
            h = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/industry-boom-history.json")["Body"].read())
            gate("G3_history_seeded", len(h.get("days") or {}) >= 1, f"ledger days={len(h.get('days') or {})}")
    except Exception as e:
        gate("G2_feed_real", False, str(e)[:320])

    ok4 = False; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/industry-rotation.html",
                    headers={"User-Agent": "Mozilla/5.0 (ops)"}), timeout=30) as r:
                if "Industry Boom League" in r.read().decode("utf-8", "replace"):
                    ok4 = True; break
        except Exception:
            pass
        time.sleep(15)
    gate("G4_page_section", ok4, "served: Industry Boom League section")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3590.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
