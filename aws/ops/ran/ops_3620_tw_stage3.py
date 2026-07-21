"""ops 3620 — TW export-orders STAGE-3: follow order-labeled hrefs from the
stage-2 menu through the /gov edge (chain proven 3617); honest-block on
postback-only. Also reports KR tape state and re-gates the macro-leads row
post-hotfix (the definitive served check)."""
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

with report("3620_tw_stage3") as rep:
    rep.heading("ops 3620 — TW stage-3 link-follow via edge")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:640]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    try:
        cfg = LAM.get_function_configuration(FunctionName=FN)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / "justhodl-asia-leads" / "source",
                      env_vars=env, timeout=max(240, cfg.get("Timeout", 120)),
                      memory=max(512, cfg.get("MemorySize", 256)),
                      description="asia-leads v1.5: TW orders stage-3 link-follow via /gov edge.",
                      create_function_url=False)
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G1_stage3", False, "fn error: " + pl["errorMessage"][:240])
        else:
            j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",
                                          Key="data/asia-leads.json")["Body"].read())
            tw = j.get("taiwan_orders") or {}
            got = isinstance(tw.get("latest_usd_bn"), (int, float)) or \
                  isinstance(tw.get("yoy_pct"), (int, float))
            tried = tw.get("stage3_tried") or []
            exercised = got or len(tried) >= 1 or "POST" in str(tw.get("error"))
            gate("G1_stage3", exercised,
                 f"VALUE={'usd_bn=%s yoy=%s period=%s' % (tw.get('latest_usd_bn'), tw.get('yoy_pct'), tw.get('period')) if got else 'none'} "
                 f"hit={tw.get('stage3_hit')} tried={[{k: t.get(k) for k in ('label','via','bytes','hit')} for t in tried]} "
                 f"cands={[(c.get('label') or '')[:28] for c in (tw.get('stage3_candidates') or [])[:4]]} "
                 f"err={tw.get('error')}")
            kt = j.get("korea_flash_tape") or {}
            rep.log("KR tape: scanned=%s latest=%s" % (kt.get("articles_scanned"),
                                                       json.dumps(kt.get("latest"))[:160]))
            out["kr_tape"] = {"scanned": kt.get("articles_scanned"),
                              "latest": kt.get("latest"), "sources": kt.get("sources")}
            out["tw"] = {k: tw.get(k) for k in ("latest_usd_bn", "yoy_pct", "period",
                                                "stage3_hit", "error")}
    except Exception as e:
        gate("G1_stage3", False, str(e)[:340])

    ok2 = False; det = ""; dl = time.time() + 420
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/macro-leads.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            det = f"tape_row={'korea_flash_tape' in html} colon_ok={chr(39)+'):(('+chr(40) not in html}"
            det = f"tape_row={'korea_flash_tape' in html}"
            if "korea_flash_tape" in html and "'):((a.korea_flash_tape" in html:
                ok2 = True; det += " ternary_ok=True"; break
        except Exception as e:
            det = str(e)[:140]
        time.sleep(18)
    gate("G2_page_row", ok2, det)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3620.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
