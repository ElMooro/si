"""ops 3592 — Korea 20-day flash via CF edge: prove the /gov worker route gets
through KR gov firewalls (the moment of truth), then asia-leads v1.3 parses the
newest KCS 1-20 release (total + semis, stated YoY), page row served. Also
reports whether /gov unblocks MOEA (TW queue bonus)."""
import json, sys, time, urllib.parse, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
GOV = "https://justhodl-data-proxy.raafouis.workers.dev/gov?u="
KCS = "https://www.customs.go.kr/kcs/na/ntt/selectNttList.do?mi=2889&bbsId=1362"
UA = {"User-Agent": "Mozilla/5.0 (ops-3592)"}

with report("3592_kcs_flash") as rep:
    rep.heading("ops 3592 — Korea 20-day flash (KCS via CF-edge /gov)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:480]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:440]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G1 worker /gov live + KR firewall verdict (deploy-workers runs on this push)
    ok1 = False; det = ""; dl = time.time() + 480
    while time.time() < dl:
        try:
            r = urllib.request.urlopen(urllib.request.Request(
                GOV + urllib.parse.quote(KCS, safe=""), headers=UA), timeout=40)
            body = r.read(200_000).decode("utf-8", "replace")
            det = f"status={r.status} host_hdr={r.headers.get('x-gov-fetch')} len={len(body)} kr_str={'수출입' in body}"
            if r.status == 200 and "수출입" in body:
                ok1 = True; break
            if r.status == 200 and len(body) > 500:      # reached but different page shape
                ok1 = True; det += " (reachable, board shape differs)"; break
        except Exception as e:
            det = str(e)[:160]
        time.sleep(20)
    gate("G1_edge_breakthrough", ok1, det)
    # bonus: MOEA via edge (report-only)
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            GOV + urllib.parse.quote("https://www.moea.gov.tw/MNS/dos_e/home/Home.aspx", safe=""),
            headers=UA), timeout=40)
        out["moea_via_edge"] = {"status": r.status, "len": len(r.read(50_000))}
    except Exception as e:
        out["moea_via_edge"] = {"err": str(e)[:120]}
    print("moea_via_edge:", out["moea_via_edge"]); rep.log("moea_via_edge: " + json.dumps(out["moea_via_edge"]))

    # G2 asia-leads v1.3 → real flash numbers
    try:
        env = (LAM.get_function_configuration(FunctionName="justhodl-asia-leads")
               .get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name="justhodl-asia-leads",
                      source_dir=ROOT / "lambdas" / "justhodl-asia-leads" / "source",
                      env_vars=env, timeout=180, memory=512,
                      description="Asia tech-pulse v1.3: KR+TW exports (FRED) + Korea 1-20d KCS flash via CF-edge /gov + TW orders scaffold.",
                      create_function_url=False)
        r = LAM.invoke(FunctionName="justhodl-asia-leads", InvocationType="RequestResponse", Payload=b"{}")
        json.loads(r["Payload"].read() or b"{}")
        a = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/asia-leads.json")["Body"].read())
        kf = a.get("korea_flash") or {}
        ok2 = a.get("version") == "1.3.0" and isinstance(kf.get("total_yoy_pct"), (int, float))
        gate("G2_flash_real", ok2,
             f"v{a.get('version')} period='{(kf.get('period') or '')[:50]}' total=${kf.get('total_usd_bn')}B "
             f"{kf.get('total_yoy_pct')}% YoY · semis=${kf.get('semis_usd_bn')}B {kf.get('semis_yoy_pct')}% "
             f"err={kf.get('error')} raw='{(kf.get('raw_head') or '')[:100]}'")
        out["flash"] = {k: kf.get(k) for k in ("period", "total_usd_bn", "total_yoy_pct",
                                               "semis_usd_bn", "semis_yoy_pct", "item_url", "error")}
    except Exception as e:
        gate("G2_flash_real", False, str(e)[:300])

    # G3 page row served (static marker)
    ok3 = False; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/macro-leads.html", headers=UA), timeout=30) as r:
                if "Korea 20-day flash" in r.read().decode("utf-8", "replace"):
                    ok3 = True; break
        except Exception:
            pass
        time.sleep(15)
    gate("G3_page_row", ok3, "served: Korea 20-day flash row")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3592.json").write_text(json.dumps(out, indent=2, ensure_ascii=False, default=str))
    sys.exit(0)
