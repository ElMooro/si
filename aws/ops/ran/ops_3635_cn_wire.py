"""ops 3635 — polish+wire: china v2.8.1 year resolution (relaxed title/
createDate + body-year near 上半年) → period '2026-H1'; macro-leads TW row
guard relaxed to usd_bn (row was still hidden on yoy=None) + NEW 🇨🇳 China
TSF row from tsf.pboc_cn. Served-gated."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=420, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")

with report("3635_cn_wire") as rep:
    rep.heading("ops 3635 — year fix + CN/TW rows")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:600]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:560]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    try:
        cfg = LAM.get_function_configuration(FunctionName="justhodl-china-liquidity")
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name="justhodl-china-liquidity",
                      source_dir=ROOT / "lambdas" / "justhodl-china-liquidity" / "source",
                      env_vars=env, timeout=max(300, cfg.get("Timeout", 120)),
                      memory=max(768, cfg.get("MemorySize", 256)),
                      description="china-liquidity v2.8.1: TSF year resolution", 
                      create_function_url=False)
        r = LAM.invoke(FunctionName="justhodl-china-liquidity",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        cn = ((json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",
                                         Key="data/china-liquidity.json")["Body"]
                          .read()).get("tsf") or {}).get("pboc_cn")) or {}
        per = str(cn.get("period") or "")
        ok1 = (not err) and isinstance(cn.get("flow_trn_cny"), (int, float)) \
              and per[:2] == "20" and "?" not in per
        gate("G1_year", ok1,
             f"err={err} flow={cn.get('flow_trn_cny')} period={per} "
             f"yoyΔ={cn.get('yoy_delta_trn')} att_title={str(cn.get('att_title'))[:44]} "
             f"att_year={cn.get('att_year')}")
        out["cn"] = {k: cn.get(k) for k in ("flow_trn_cny", "period", "yoy_delta_trn",
                                            "att_title")}
    except Exception as e:
        gate("G1_year", False, str(e)[:360])

    ok2 = False; det = ""; dl = time.time() + 480
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/macro-leads.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            mk = {"cn_row": "pboc_cn" in html and "China TSF flow" in html,
                  "tw_relax": "latest_usd_bn!=null" in html}
            det = str(mk)
            if all(mk.values()):
                ok2 = True; break
        except Exception as e:
            det = str(e)[:140]
        time.sleep(18)
    gate("G2_rows", ok2, det)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3635.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
# retrigger 051941 — prior run lost to push-race
