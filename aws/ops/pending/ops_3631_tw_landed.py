"""ops 3631 — forensic + hardened retry. Unzipped-marker check on the
DEPLOYED asia-leads zip (v17/stage6 markers) + FULL taiwan_orders key dump
settles absent-vs-race. v1.7.1 writes stage-6 keys unconditionally + brute
SN 455-475 fallback; china-liquidity v2.5 follows W020 JS file refs."""
import io, json, sys, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=420, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

with report("3631_tw_landed") as rep:
    rep.heading("ops 3631 — deployed-zip forensic + v1.7.1/v2.5")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:700]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:660]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    def dep_inv(fn, tmo, mem, desc):
        cfg = LAM.get_function_configuration(FunctionName=fn)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=fn,
                      source_dir=ROOT / "lambdas" / fn / "source",
                      env_vars=env, timeout=max(tmo, cfg.get("Timeout", 120)),
                      memory=max(mem, cfg.get("MemorySize", 256)),
                      description=desc[:200], create_function_url=False)
        r = LAM.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        return pl.get("errorMessage") if isinstance(pl, dict) else None

    try:
        err = dep_inv("justhodl-asia-leads", 300, 512,
                      "asia-leads v1.8: million-USD level parse + self-building YoY cache")
        loc = LAM.get_function(FunctionName="justhodl-asia-leads")["Code"]["Location"]
        zf = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
        blob = b"".join(zf.read(n) for n in zf.namelist() if n.endswith(".py"))
        mk = {m: (m.encode() in blob) for m in ('out["v17"]', "stage6_brute",
                                                "GetPointData")}
        tw = (json.loads(S3C.get_object(Bucket=B, Key="data/asia-leads.json")["Body"]
                         .read()).get("taiwan_orders")) or {}
        got = isinstance(tw.get("latest_usd_bn"), (int, float)) or \
              isinstance(tw.get("yoy_pct"), (int, float))
        s6 = tw.get("stage6_tried") or []
        hits = [r0 for r0 in s6 if r0.get("hit")]
        gate("G1_tw", all(mk.values()) and not err and
             ("v17" in tw) and got,
             f"zip={mk} err={err} keys={sorted(tw.keys())[:18]} "
             f"VALUE={'usd_bn=%s yoy=%s per=%s' % (tw.get('latest_usd_bn'), tw.get('yoy_pct'), tw.get('period')) if got else 'none'} "
             f"brute={tw.get('stage6_brute')} sns={tw.get('stage6_sitesns')} "
             f"s6_hit={tw.get('stage6_hit')} "
             f"s6_sample={[{k: r0.get(k) for k in ('sn', 'hit', 'xerr', 'err', 'item_blob')} for r0 in s6[:3]]} "
             f"blob={next((r0.get('item_blob') for r0 in hits), None)}")
        out["tw"] = tw
    except Exception as e:
        gate("G1_tw", False, str(e)[:380])

    try:
        err = dep_inv("justhodl-china-liquidity", 300, 768,
                      "china-liquidity v2.5.1: +raw body_probe for offline shell analysis")
        cn = ((json.loads(S3C.get_object(Bucket=B, Key="data/china-liquidity.json")["Body"]
                          .read()).get("tsf") or {}).get("pboc_cn")) or {}
        got = isinstance(cn.get("flow_trn_cny"), (int, float))
        gate("G2_cn", (got or cn.get("js_files") is not None
                       or cn.get("candidates")) and not err,
             f"err={err} VALUE={'%s trn %s yoyΔ %s' % (cn.get('flow_trn_cny'), cn.get('period'), cn.get('yoy_delta_trn')) if got else 'none'} "
             f"title={str(cn.get('title'))[:44]} att={str(cn.get('attachment'))[:70]} "
             f"js_files={cn.get('js_files')} probe={str(cn.get('body_probe'))[:180]} via={cn.get('via')} note={cn.get('error')}")
        out["cn"] = cn
    except Exception as e:
        gate("G2_cn", False, str(e)[:380])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3631.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
