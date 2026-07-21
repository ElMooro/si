"""ops 3627 — clean regate: CN national-node/attachment (v2.3) + TW stage-5
scanning STAGE-4 FRAME bodies (v1.6.1). Fresh gates, no string-surgery."""
import json, sys
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=420, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

with report("3627_cn_tw_regate") as rep:
    rep.heading("ops 3627 — CN national + TW stage-5 (frame-body scan)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:660]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:620]); rep.log(n + " " + str(ok))
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
        err = dep_inv("justhodl-china-liquidity", 300, 768,
                      "china-liquidity v2.3: national TSF node + attachment hop")
        cn = ((json.loads(S3C.get_object(Bucket=B, Key="data/china-liquidity.json")["Body"]
                          .read()).get("tsf") or {}).get("pboc_cn")) or {}
        got = isinstance(cn.get("flow_trn_cny"), (int, float))
        monthly_item = bool(cn.get("title")) and "地区" not in str(cn.get("title"))
        probed = len(cn.get("candidates") or []) >= 1
        gate("G1_cn", (got or probed) and not err,
             f"err={err} VALUE={'%s trn %s yoyΔ %s' % (cn.get('flow_trn_cny'), cn.get('period'), cn.get('yoy_delta_trn')) if got else 'none'} "
             f"title={str(cn.get('title'))[:52]} monthly_pref={monthly_item} "
             f"att={str(cn.get('attachment'))[:60]} via={cn.get('via')} "
             f"cands={[(c.get('title') or '')[:26] for c in (cn.get('candidates') or [])[:4]]} "
             f"note={cn.get('error')} head={str(cn.get('body_head'))[:70]}")
        out["cn"] = cn
    except Exception as e:
        gate("G1_cn", False, str(e)[:360])

    try:
        err = dep_inv("justhodl-asia-leads", 240, 512,
                      "asia-leads v1.6.1: stage-5 scans stage-4 frame bodies")
        tw = (json.loads(S3C.get_object(Bucket=B, Key="data/asia-leads.json")["Body"]
                         .read()).get("taiwan_orders")) or {}
        got = isinstance(tw.get("latest_usd_bn"), (int, float)) or \
              isinstance(tw.get("yoy_pct"), (int, float))
        eps = tw.get("stage5_endpoints") or []
        s5 = tw.get("stage5_tried") or []
        gate("G2_tw5", (got or len(eps) >= 1 or len(s5) >= 1
                        or tw.get("stage4_block")) and not err,
             f"err={err} VALUE={'usd_bn=%s yoy=%s' % (tw.get('latest_usd_bn'), tw.get('yoy_pct')) if got else 'none'} "
             f"s5_hit={tw.get('stage5_hit')} eps={[str(e.get('u'))[-44:] for e in eps[:5]]} "
             f"s5={[{k: t.get(k) for k in ('via', 'json', 'hit', 'head')} for t in s5[:2]]} "
             f"block={tw.get('stage4_block')}")
        out["tw"] = {k: tw.get(k) for k in ("latest_usd_bn", "yoy_pct", "stage5_hit",
                                            "stage5_endpoints", "error")}
    except Exception as e:
        gate("G2_tw5", False, str(e)[:360])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3627.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
