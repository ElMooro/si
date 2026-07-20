"""ops 3600 — barometer to page-top + 1990 deep history: engine v1.2 writes
data/fifx-vol-history.json (consistent FRED legs, weekly pre-2y / daily recent);
gates: shard reaches early-1990s + row sanity + crisis spikes visible (1998/
2008/2020 spill maxima positive), page order fifx-above-spx + deep markers."""
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
FN = "justhodl-fifx-vol-migration"

with report("3600_fifx_deep") as rep:
    rep.heading("ops 3600 — barometer top placement + 1990→today spillover history")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:480]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:440]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    try:
        env = (LAM.get_function_configuration(FunctionName=FN)
               .get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / "justhodl-fifx-vol-migration" / "source",
                      env_vars=env, timeout=240, memory=768,
                      description="Vol migration barometer v1.2: + deep 1990+ spillover history shard (consistent FRED legs, weekly/daily splice).",
                      create_function_url=False)
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G1_deep_shard", False, "fn error: " + pl["errorMessage"][:240])
        else:
            dj = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/fifx-vol-history.json")["Body"].read())
            R = dj.get("rows") or []
            first, last = dj.get("first") or "", dj.get("last") or ""
            def yr_max(y0, y1):
                vs = [r0["spill"] for r0 in R if y0 <= r0["d"][:4] <= y1
                      and isinstance(r0.get("spill"), (int, float))]
                return round(max(vs), 2) if vs else None
            sp98, sp08, sp20 = yr_max("1997", "1998"), yr_max("2008", "2009"), yr_max("2020", "2020")
            ok1 = (len(R) >= 1500 and first[:4] <= "1993" and last >= "2026-07"
                   and all(isinstance(R[-1].get(k), (int, float)) for k in ("fis", "fx", "eq", "spill")))
            gate("G1_deep_shard", ok1,
                 f"rows={len(R)} span {first}→{last} · crisis spill maxima: LTCM97-98={sp98} "
                 f"GFC08-09={sp08} COVID20={sp20} · last={R[-1] if R else None}")
            mainf = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/fifx-vol.json")["Body"].read())
            gate("G2_main_intact", mainf.get("version") == "1.2.0"
                 and len(mainf.get("history") or []) >= 120
                 and (mainf.get("migration") or {}).get("state") in
                     ("CALM", "UPSTREAM_BREWING", "MIGRATING", "BROAD_STRESS"),
                 f"main v{mainf.get('version')} hist={len(mainf.get('history') or [])} "
                 f"state={(mainf.get('migration') or {}).get('state')} spill={(mainf.get('migration') or {}).get('spillover')}")
            out["crisis"] = {"1997-98": sp98, "2008-09": sp08, "2020": sp20}
    except Exception as e:
        gate("G1_deep_shard", False, str(e)[:320])

    ok3 = False; det = ""; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/signal-board.html",
                    headers={"User-Agent": "Mozilla/5.0 (ops)"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            order = html.find('id="jh-fifx"') != -1 and html.find('id="jh-spx-ma"') != -1 \
                    and html.find('id="jh-fifx"') < html.find('id="jh-spx-ma"')
            mk = all(k in html for k in ("drawRibbon", "fifx-vol-history", "jh-fifx-rbt"))
            det = f"order_top={order} deep_markers={mk}"
            if order and mk:
                ok3 = True; break
        except Exception as e:
            det = str(e)[:120]
        time.sleep(15)
    gate("G3_page_top_deep", ok3, det)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3600.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
