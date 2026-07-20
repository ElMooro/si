"""ops 3588 — deploy + live-prove the two discovery-queue scrapers: PBoC EN
monthly AFRE (real TSF, self-building cache) into china-liquidity v2.1.1, and
DGBAS Taiwan EXPORT ORDERS into asia-leads v1.2. Recipes were probe-proven
(3586); gates demand REAL parsed numbers, tolerant only to labeled absence."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"

with report("3588_scrapers_v2") as rep:
    rep.heading("ops 3588 — PBoC AFRE + DGBAS export-orders live")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:480]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:440]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G1 deploy both (env preserved)
    try:
        cl_env = (LAM.get_function_configuration(FunctionName="justhodl-china-liquidity")
                  .get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name="justhodl-china-liquidity",
                      source_dir=ROOT / "lambdas" / "justhodl-china-liquidity" / "source",
                      env_vars=cl_env, timeout=240, memory=512,
                      description="China liquidity + credit impulse — v2.1: REAL PBoC monthly AFRE (Flow) scraped + cached (self-building history) alongside NBS annual composition and the money-accel proxy.",
                      create_function_url=False)
        al_env = (LAM.get_function_configuration(FunctionName="justhodl-asia-leads")
                  .get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name="justhodl-asia-leads",
                      source_dir=ROOT / "lambdas" / "justhodl-asia-leads" / "source",
                      env_vars=al_env, timeout=150, memory=512,
                      description="Asia tech-pulse v1.2.1: KR+TW exports (FRED) + TRUE Taiwan EXPORT ORDERS (DGBAS point page).",
                      create_function_url=False)
        gate("G1_deployed", True, "both v-bumps deployed via helper")
    except Exception as e:
        gate("G1_deployed", False, str(e)[:300])

    # G2 asia-leads → taiwan_orders real print
    try:
        r = LAM.invoke(FunctionName="justhodl-asia-leads", InvocationType="RequestResponse", Payload=b"{}")
        json.loads(r["Payload"].read() or b"{}")
        a = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/asia-leads.json")["Body"].read())
        to = a.get("taiwan_orders") or {}
        ok2 = a.get("version") == "1.2.0" and (isinstance(to.get("yoy_pct"), (int, float))
                                               or isinstance(to.get("latest_usd_bn"), (int, float)))
        gate("G2_tw_orders", ok2 or bool(to.get("error")),
             f"v{a.get('version')} orders yoy={to.get('yoy_pct')}% usd_bn={to.get('latest_usd_bn')} "
             f"period={to.get('period')} err={to.get('error')} raw='{(to.get('raw_head') or '')[:110]}'")
        if not ok2:
            fails.append("G2_tw_orders_value") if not to.get("error") else None
        out["tw_orders"] = to
    except Exception as e:
        gate("G2_tw_orders", False, str(e)[:300])

    # G3 china-liquidity → pboc_monthly parsed
    try:
        r = LAM.invoke(FunctionName="justhodl-china-liquidity", InvocationType="RequestResponse", Payload=b"{}")
        json.loads(r["Payload"].read() or b"{}")
        c = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/china-liquidity.json")["Body"].read())
        pm = ((c.get("tsf") or {}).get("pboc_monthly")) or {}
        ser = pm.get("series") or {}
        lr = pm.get("latest_report") or {}
        ok3 = bool(ser.get("latest")) and (lr.get("n_rows_parsed") or 0) >= 3
        gate("G3_pboc_afre", ok3,
             f"report='{(lr.get('title') or '')[:70]}' rows={lr.get('n_rows_parsed')} "
             f"afre_latest={ser.get('latest')} (100M RMB) n_vals={len(ser.get('monthly_flow_100m_rmb') or [])} "
             f"cache={pm.get('cache_n_reports')} err={pm.get('error')}")
        out["pboc"] = {"title": lr.get("title"), "latest": ser.get("latest"),
                       "vals": (ser.get("monthly_flow_100m_rmb") or [])[:13]}
    except Exception as e:
        gate("G3_pboc_afre", False, str(e)[:300])

    # G4 served page marker
    ok4 = False; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/macro-leads.html",
                    headers={"User-Agent": "Mozilla/5.0 (ops)"}), timeout=30) as r:
                if "Taiwan export ORDERS" in r.read().decode("utf-8", "replace"):
                    ok4 = True; break
        except Exception:
            pass
        time.sleep(15)
    gate("G4_page_row", ok4, "served: Taiwan export ORDERS row")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3588.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
