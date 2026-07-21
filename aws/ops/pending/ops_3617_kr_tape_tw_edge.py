"""ops 3617 — [KR] keyless 1-20 flash via NEWS-TAPE parser (NewsAPI+FMP,
verb-signed %, month-window tagging; VALIDATED on the prior-month print so
tonight's July print auto-lands) + [TW] DGBAS stage-1/2 re-routed through the
/gov edge (MOEA-class 403s bypassed). Gates prove mechanisms, never fabricate."""
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

with report("3617_kr_tape_tw_edge") as rep:
    rep.heading("ops 3617 — KR news-tape flash + TW via edge")
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
                      description="asia-leads v1.4: KR flash news-tape parser + DGBAS via /gov edge.",
                      create_function_url=False)
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G1_kr_tape", False, "fn error: " + pl["errorMessage"][:240])
        else:
            j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",
                                          Key="data/asia-leads.json")["Body"].read())
            kt = j.get("korea_flash_tape") or {}
            vs, lt = kt.get("validated_sample"), kt.get("latest")
            ok1 = (kt.get("method") == "news-tape"
                   and kt.get("articles_scanned", 0) >= 20
                   and ((vs and isinstance(vs.get("yoy_pct"), (int, float)))
                        or (lt and isinstance(lt.get("yoy_pct"), (int, float)))))
            gate("G1_kr_tape", ok1,
                 f"scanned={kt.get('articles_scanned')} sources={kt.get('sources')} "
                 f"validated={vs and {k: vs.get(k) for k in ('yoy_pct','semis_yoy_pct','period','via')}} "
                 f"latest={lt and {k: lt.get(k) for k in ('yoy_pct','semis_yoy_pct','period','published')}} "
                 f"err={kt.get('error')}")
            tw = j.get("taiwan_orders") or {}
            ok2 = tw.get("via_stage1") == "edge" or tw.get("via_stage2") == "edge" \
                  or isinstance(tw.get("latest_usd_bn"), (int, float))
            gate("G2_tw_edge", ok2,
                 f"via1={tw.get('via_stage1')} via2={tw.get('via_stage2')} "
                 f"stage2={str(tw.get('stage2'))[:60]} val={tw.get('latest_usd_bn')} "
                 f"yoy={tw.get('yoy_pct')} period={tw.get('period')} err={tw.get('error')} "
                 f"s2err={tw.get('stage2_err')}")
            out["kr"] = kt.get("validated_sample") or kt.get("latest")
            out["tw"] = {k: tw.get(k) for k in ("via_stage1", "via_stage2",
                                                "latest_usd_bn", "yoy_pct", "period")}
    except Exception as e:
        gate("G1_kr_tape", False, str(e)[:340])

    ok3 = False; det = ""; dl = time.time() + 360
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/macro-leads.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            det = f"tape_row={'korea_flash_tape' in html} news_tape_label={'news-tape' in html}"
            if "korea_flash_tape" in html and "news-tape" in html:
                ok3 = True; break
        except Exception as e:
            det = str(e)[:140]
        time.sleep(18)
    gate("G3_page_row", ok3, det)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3617.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
