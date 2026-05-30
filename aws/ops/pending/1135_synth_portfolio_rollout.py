"""ops 1135 — Master synthesis + portfolio rollout.

Adds 2 contexts:
  desk-consensus           (brief_type=synthesis)  — reads all 23+6 briefs, master CIO note
  portfolio-manager-brief  (brief_type=portfolio)  — personalized to portfolio holdings

Router extended with synthesis + portfolio dispatch.
Invocation is SEQUENTIAL: synthesis first (so portfolio reads fresh consensus).
"""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-ai-brief-router"
BUCKET = "justhodl-dashboard-live"
REGISTRY_KEY = "config/ai-brief-contexts.json"

_cfg = Config(connect_timeout=10, read_timeout=300, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION)


def zip_src(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root: continue
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()


def wait_active(t=180):
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
                return True
            if c.get("LastUpdateStatus") == "Failed": return False
        except ClientError: pass
        time.sleep(2)
    return False


def invoke_single(ctx_id):
    """Invoke router for one context. Returns (body_resp, log_tail, fn_err)."""
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=json.dumps({"contexts": [ctx_id]}).encode(),
                     LogType="Tail")
    body_resp = json.loads(inv["Payload"].read() or b"{}")
    if isinstance(body_resp, dict) and "body" in body_resp:
        try: body_resp = json.loads(body_resp["body"])
        except Exception: pass
    log_tail = base64.b64decode(inv.get("LogResult", "")).decode("utf-8", "replace")[-2000:]
    return body_resp, log_tail, inv.get("FunctionError")


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Redeploy router
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        wait_active()
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active()
        rpt["redeploy"] = "OK"

        # 2) Upload 32-context registry
        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        with open(registry_path) as fh:
            body = fh.read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=body.encode("utf-8"), ContentType="application/json")
        registry = json.loads(body)
        rpt["registry"] = {"n_contexts": len(registry.get("contexts") or {})}

        # 3) Invoke SYNTHESIS first (so portfolio can read fresh consensus)
        print("[1135] invoking desk-consensus (synthesis)…")
        synth_resp, synth_log, synth_err = invoke_single("desk-consensus")
        rpt["synthesis_invoke"] = {"fn_err": synth_err, "body": synth_resp}
        rpt["synthesis_log_tail"] = synth_log

        # Brief pause to let S3 settle
        time.sleep(3)

        # Verify synthesis landed
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/desk-consensus.json")
            synth = json.loads(obj["Body"].read())
            cs = synth.get("consensus") or {}
            rpt["synthesis_brief"] = {
                "regime": cs.get("regime"),
                "confidence": cs.get("confidence"),
                "one_liner": cs.get("one_liner"),
                "n_supporting": cs.get("n_supporting_desks"),
                "n_dissent": len(synth.get("dissent") or []),
                "n_asymmetric": len(synth.get("asymmetric_setups") or []),
                "n_convergent_names": len(synth.get("convergent_names") or []),
                "sample_asymmetric": (synth.get("asymmetric_setups") or [])[:2],
                "today_action": synth.get("today_action"),
                "loudest_tripwire": synth.get("loudest_tripwire"),
                "n_regime_briefs_loaded": (synth.get("input_state") or {}).get("n_regime_briefs"),
                "n_name_briefs_loaded": (synth.get("input_state") or {}).get("n_name_briefs"),
            }
        except ClientError:
            rpt["synthesis_brief"] = "NOT_WRITTEN"

        # 4) Invoke PORTFOLIO (uses the just-written synthesis as cross-context)
        print("[1135] invoking portfolio-manager-brief…")
        port_resp, port_log, port_err = invoke_single("portfolio-manager-brief")
        rpt["portfolio_invoke"] = {"fn_err": port_err, "body": port_resp}
        rpt["portfolio_log_tail"] = port_log

        time.sleep(2)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/portfolio-manager-brief.json")
            port = json.loads(obj["Body"].read())
            rpt["portfolio_brief"] = {
                "regime_fit": port.get("regime_fit"),
                "headline": port.get("headline"),
                "thesis": (port.get("thesis") or "")[:300],
                "biggest_strength": port.get("biggest_strength"),
                "biggest_concern": port.get("biggest_concern"),
                "n_concentration_flags": len(port.get("concentration_flags") or []),
                "n_out_of_regime": len(port.get("out_of_regime_holdings") or []),
                "this_weeks_action": port.get("this_weeks_action"),
                "input_state": port.get("input_state"),
            }
        except ClientError:
            rpt["portfolio_brief"] = "NOT_WRITTEN"

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1135.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items()
                       if k not in ("synthesis_log_tail", "portfolio_log_tail", "traceback")},
                     indent=2, default=str)[:4500])


if __name__ == "__main__":
    main()
