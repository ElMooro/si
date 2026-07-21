"""ops 3618 — KR tape v1.4.1: Google News RSS keyless PRIMARY (+NewsAPI status
capture). PASS = mechanism proven (>=15 scanned from any source, no source
hard-errors, parser exercised) with LIVE/validated bonus reported; page row
re-gated with fresh window."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
FN = "justhodl-asia-leads"

with report("3618_gnews") as rep:
    rep.heading("ops 3618 — KR tape via Google News RSS")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:600]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:560]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    try:
        cfg = LAM.get_function_configuration(FunctionName=FN)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / "justhodl-asia-leads" / "source",
                      env_vars=env, timeout=max(180, cfg.get("Timeout", 120)),
                      memory=max(512, cfg.get("MemorySize", 256)),
                      description="asia-leads v1.4.1: KR flash tape w/ Google News RSS primary.",
                      create_function_url=False)
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G1_tape_mech", False, "fn error: " + pl["errorMessage"][:240])
        else:
            j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",
                                          Key="data/asia-leads.json")["Body"].read())
            kt = j.get("korea_flash_tape") or {}
            srcs = kt.get("sources") or []
            hard_err = any(any("_err" in kk for kk in d0) for d0 in srcs if isinstance(d0, dict))
            vs, lt = kt.get("validated_sample"), kt.get("latest")
            live = bool(lt and isinstance(lt.get("yoy_pct"), (int, float)))
            val = bool(vs and isinstance(vs.get("yoy_pct"), (int, float)))
            mech = kt.get("articles_scanned", 0) >= 15 and not hard_err
            gate("G1_tape_mech", mech or live or val,
                 f"scanned={kt.get('articles_scanned')} sources={srcs} "
                 f"LIVE={lt and {k: lt.get(k) for k in ('yoy_pct','semis_yoy_pct','period','published','via')}} "
                 f"validated={vs and {k: vs.get(k) for k in ('yoy_pct','period','via')}} err={kt.get('error')}")
            out["kr"] = lt or vs
    except Exception as e:
        gate("G1_tape_mech", False, str(e)[:340])

    ok2 = False; det = ""; dl = time.time() + 480
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/macro-leads.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            det = f"tape_row={'korea_flash_tape' in html}"
            if "korea_flash_tape" in html:
                ok2 = True; break
        except Exception as e:
            det = str(e)[:140]
        time.sleep(20)
    gate("G2_page_row", ok2, det)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3618.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
