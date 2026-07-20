"""ops 3584 — asia-leads v1.1 arc: NBS monthly-TSF probe → env on
china-liquidity (which now carries the REAL TSF the engine's own docstring
said was missing), deploy both engines via helper, econ-calendar freshness
(existing engine, wired not rebuilt), macro-leads page cards, MI feed marker."""
import io, json, sys, time, urllib.parse, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
SCH = boto3.client("scheduler", "us-east-1")
EVT = boto3.client("events", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
UA = {"User-Agent": "JustHodl research contact@justhodl.ai", "Accept": "application/json"}

def gjson(url, timeout=25):
    try:
        return json.loads(urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout).read())
    except Exception:
        return None

with report("3584_asia_v11") as rep:
    rep.heading("ops 3584 — asia v1.1: real TSF into china-liquidity · lean tech-pulse · calendar wired")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:480]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:440]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G0 probe NBS monthly TSF dataset (official name: Aggregate Financing to the Real Economy)
    mcode = None; found = []
    for q in ("aggregate financing to the real economy", "aggregate financing", "social financing monthly"):
        j = gjson("https://api.db.nomics.world/v22/search?limit=10&q=" + urllib.parse.quote(q))
        docs = ((j or {}).get("results") or {}).get("docs") or (j or {}).get("docs") or []
        for d in docs:
            if d.get("provider_code") == "NBS":
                found.append({"code": d.get("code") or d.get("dataset_code"), "name": (d.get("name") or "")[:90]})
        if found:
            break
    # prefer monthly-looking codes
    for f in found:
        c = f.get("code") or ""
        if c and c != "A_A0L08" and (c.startswith("M_") or "month" in (f.get("name") or "").lower()):
            mcode = c; break
    if not mcode and found:
        mcode = next((f["code"] for f in found if f.get("code") and f["code"] != "A_A0L08"), None)
    gate("G0_nbs_monthly_probe", True, f"candidates={found[:4]} chosen={mcode or 'NONE (annual-only mode)'}")
    out["nbs_monthly"] = mcode

    # G1 deploy both engines (china-liquidity env preserved + NBS key merged)
    try:
        cl_env = (LAM.get_function_configuration(FunctionName="justhodl-china-liquidity")
                  .get("Environment") or {}).get("Variables") or {}
        if mcode:
            cl_env["NBS_TSF_MONTHLY"] = mcode
        deploy_lambda(report=rep, function_name="justhodl-china-liquidity",
                      source_dir=ROOT / "lambdas" / "justhodl-china-liquidity" / "source",
                      env_vars=cl_env, timeout=180, memory=512,
                      description="China liquidity + credit impulse — v2 adds REAL NBS Total Social Financing (annual composition + monthly when probed) alongside the money-acceleration proxy.",
                      create_function_url=False)
        al_env = {"FRED_API_KEY": cl_env.get("FRED_API_KEY") or "2f057499936072679d8843d7fce99989"}
        deploy_lambda(report=rep, function_name="justhodl-asia-leads",
                      source_dir=ROOT / "lambdas" / "justhodl-asia-leads" / "source",
                      env_vars=al_env, timeout=120, memory=512,
                      description="Asia tech-pulse v1.1 (lean): KR + TW export YoY from FRED primaries. TSF lives in china-liquidity; calendar lives in econ-calendar.",
                      create_function_url=False)
        gate("G1_deployed", True, f"both deployed via helper; NBS_TSF_MONTHLY={'set:'+mcode if mcode else 'unset'}")
    except Exception as e:
        gate("G1_deployed", False, str(e)[:300])

    # G2 invoke both → real values, no duplicated blocks
    try:
        r = LAM.invoke(FunctionName="justhodl-asia-leads", InvocationType="RequestResponse", Payload=b"{}")
        json.loads(r["Payload"].read() or b"{}")
        a = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/asia-leads.json")["Body"].read())
        r2 = LAM.invoke(FunctionName="justhodl-china-liquidity", InvocationType="RequestResponse", Payload=b"{}")
        json.loads(r2["Payload"].read() or b"{}")
        c = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/china-liquidity.json")["Body"].read())
        tsf = c.get("tsf") or {}
        mo = tsf.get("monthly") or {}
        ok = (a.get("version") == "1.1.0"
              and "china_tsf" not in a and "us_calendar" not in a
              and isinstance((a.get("korea_exports") or {}).get("yoy_pct"), (int, float))
              and isinstance((a.get("taiwan_exports") or {}).get("yoy_pct"), (int, float))
              and len(tsf.get("annual_composition") or []) >= 3)
        gate("G2_engines_real", ok,
             f"asia v{a.get('version')} kr={a['korea_exports'].get('yoy_pct')}% tw={a['taiwan_exports'].get('yoy_pct')}% "
             f"dup_blocks_removed={('china_tsf' not in a and 'us_calendar' not in a)} · "
             f"cl.tsf annual={len(tsf.get('annual_composition') or [])} monthly_series={mo.get('n_series')} "
             f"sample={((mo.get('series') or [{}])[0].get('name') or '')[:60]} "
             f"impulse={((mo.get('series') or [{}])[0]).get('credit_impulse_flow_yoy_pct')}")
        out["snapshot"] = {"kr": a["korea_exports"].get("yoy_pct"), "tw": a["taiwan_exports"].get("yoy_pct"),
                           "tsf_monthly": (mo.get("series") or [])[:2]}
    except Exception as e:
        gate("G2_engines_real", False, str(e)[:320])

    # G3 econ-calendar (existing engine) fresh + scheduled — resurrect if dead
    try:
        fresh = False; det = ""
        try:
            ec = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/econ-calendar.json")["Body"].read())
            gen = ec.get("generated_at") or ec.get("as_of") or ""
            age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(gen.replace("Z", "+00:00"))).total_seconds() / 3600 if gen else 9e9
            fresh = age_h < 36 and len(ec.get("upcoming") or []) >= 3
            det = f"age_h={round(age_h,1)} upcoming={len(ec.get('upcoming') or [])}"
        except Exception as e0:
            det = f"feed read fail: {str(e0)[:80]}"
        if not fresh:
            arn = LAM.get_function_configuration(FunctionName="justhodl-econ-calendar")["FunctionArn"]
            rules = EVT.list_rule_names_by_target(TargetArn=arn).get("RuleNames") or []
            scheds = []
            for pg in SCH.get_paginator("list_schedules").paginate():
                scheds += [s0["Name"] for s0 in pg.get("Schedules", []) if "econ-calendar" in s0["Name"]]
            if not rules and not scheds:
                SCH.create_schedule(Name="justhodl-econ-calendar-daily",
                                    ScheduleExpression="cron(40 10 * * ? *)",
                                    FlexibleTimeWindow={"Mode": "OFF"},
                                    Target={"Arn": arn,
                                            "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                                            "Input": "{}"},
                                    State="ENABLED", Description="Econ calendar daily (resurrected ops 3584)")
                det += " · UNSCHEDULED → Scheduler created"
            LAM.invoke(FunctionName="justhodl-econ-calendar", InvocationType="RequestResponse", Payload=b"{}")
            ec = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/econ-calendar.json")["Body"].read())
            fresh = len(ec.get("upcoming") or []) >= 3
            det += f" · re-invoked, upcoming={len(ec.get('upcoming') or [])} next_major={(ec.get('next_major') or {}).get('event')}"
        gate("G3_econ_calendar_alive", fresh, det)
    except Exception as e:
        gate("G3_econ_calendar_alive", False, str(e)[:300])

    # G4 served page cards
    ok4 = False; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/macro-leads.html", headers={"User-Agent": "Mozilla/5.0 (ops)"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            if all(m in html for m in ("Asia tech pulse", "Next prints — consensus watch", "asia-pulse")):
                ok4 = True; break
        except Exception:
            pass
        time.sleep(15)
    gate("G4_page_cards", ok4, "served: asia-pulse + next-prints cards")

    # G5 MI zip marker (never invoke MI)
    try:
        info = LAM.get_function(FunctionName="justhodl-morning-intelligence")
        with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],
                headers={"User-Agent": "Mozilla/5.0"}), timeout=60) as r:
            src = zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")
        gate("G5_mi_feed", '"data/asia-leads.json"' in src,
             f"asia_leads feed in deployed MI · econ_calendar already present={'data/econ-calendar.json' in src}")
    except Exception as e:
        gate("G5_mi_feed", False, str(e)[:200])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3584.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
