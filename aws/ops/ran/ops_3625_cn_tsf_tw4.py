"""ops 3625 — [CN] PBoC TSF (社会融资规模增量) CN-side via /gov edge in
china-liquidity v2.2 (EN report lags ~11mo; CN is same-week) + [TW] asia-leads
v1.5.1 stage-4 nested-shell probe (jhxiaoQS/iframe/meta-refresh hop). Gates =
value OR honest hop-map with via=edge; never fabricates."""
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

with report("3625_cn_tsf_tw4") as rep:
    rep.heading("ops 3625 — CN TSF via edge + TW stage-4")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:640]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]); rep.log(n + " " + str(ok))
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
                      "china-liquidity v2.2: +CN-side PBoC TSF via /gov edge")
        j = json.loads(S3C.get_object(Bucket=B, Key="data/china-liquidity.json")["Body"].read())
        cn = ((j.get("tsf") or {}).get("pboc_cn")) or {}
        got = isinstance(cn.get("flow_trn_cny"), (int, float))
        probed = len(cn.get("candidates") or []) >= 1 or cn.get("body_head") or \
                 "listings" in str(cn.get("error"))
        gate("G1_cn_tsf", (got or probed) and not err,
             f"err={err} VALUE={'%s trn CNY %s (yoyΔ %s)' % (cn.get('flow_trn_cny'), cn.get('period'), cn.get('yoy_delta_trn')) if got else 'none'} "
             f"title={str(cn.get('title'))[:48]} via={cn.get('via')} "
             f"cands={[(c.get('title') or '')[:24] for c in (cn.get('candidates') or [])[:3]]} "
             f"err_note={cn.get('error')} head={str(cn.get('body_head'))[:80]}")
        out["cn"] = cn
    except Exception as e:
        gate("G1_cn_tsf", False, str(e)[:360])

    try:
        err = dep_inv("justhodl-asia-leads", 240, 512,
                      "asia-leads v1.5.1: TW stage-4 nested-shell probe via edge")
        j = json.loads(S3C.get_object(Bucket=B, Key="data/asia-leads.json")["Body"].read())
        tw = j.get("taiwan_orders") or {}
        got = isinstance(tw.get("latest_usd_bn"), (int, float)) or \
              isinstance(tw.get("yoy_pct"), (int, float))
        s4 = tw.get("stage4_tried") or []
        ok2 = (got or len(s4) >= 1 or tw.get("stage4_block")) and not err
        gate("G2_tw_stage4", ok2,
             f"err={err} VALUE={'usd_bn=%s yoy=%s' % (tw.get('latest_usd_bn'), tw.get('yoy_pct')) if got else 'none'} "
             f"s4_hit={tw.get('stage4_hit')} s4={[{k: t.get(k) for k in ('via','bytes','hit')} for t in s4]} "
             f"block={tw.get('stage4_block')} err_note={tw.get('error')}")
        out["tw"] = {k: tw.get(k) for k in ("latest_usd_bn", "yoy_pct", "stage4_hit",
                                            "stage4_block", "error")}
    except Exception as e:
        gate("G2_tw_stage4", False, str(e)[:360])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3625.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
