"""ops 1124 — Deploy justhodl-auction-interpreter + register in scheduler 4hourly tick + invoke + verify."""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-auction-interpreter"
KEY_SOURCE_LAMBDA = "justhodl-ai-chat"  # source of ANTHROPIC_API_KEY
BUCKET = "justhodl-dashboard-live"
MANIFEST_KEY = "config/schedule-manifest.json"
LAYER_ARN = "arn:aws:lambda:us-east-1:857687956942:layer:justhodl-core:1"

_cfg = Config(connect_timeout=10, read_timeout=240, retries={"max_attempts": 2})
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


def wait_active(t=120):
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


def get_source_env():
    try:
        c = lam.get_function_configuration(FunctionName=KEY_SOURCE_LAMBDA)
        return (c.get("Environment") or {}).get("Variables", {}) or {}
    except ClientError:
        return {}


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        cfg = json.load(open(os.path.join(REPO_ROOT, "aws/lambdas", FN, "config.json")))
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")

        src_env = get_source_env()
        api_key = src_env.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            rpt["fatal_err"] = f"no ANTHROPIC_API_KEY on {KEY_SOURCE_LAMBDA}"
            _save(rpt); return
        rpt["env_pull"] = f"OK (key {len(api_key)} chars)"
        env_vars = {"ANTHROPIC_API_KEY": api_key}

        try:
            lam.get_function_configuration(FunctionName=FN); exists = True
        except ClientError:
            exists = False

        if not exists:
            lam.create_function(
                FunctionName=FN, Runtime=cfg["runtime"], Role=cfg["role"],
                Handler=cfg["handler"], Code={"ZipFile": zip_src(src_dir)},
                Description=cfg["description"][:255], Timeout=cfg["timeout"],
                MemorySize=cfg["memory"], Architectures=cfg["architectures"],
                Layers=cfg.get("layers") or [LAYER_ARN],
                Environment={"Variables": env_vars},
            )
            rpt["deploy"] = "CREATED"
        else:
            wait_active()
            lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
            wait_active()
            lam.update_function_configuration(
                FunctionName=FN, Timeout=cfg["timeout"], MemorySize=cfg["memory"],
                Layers=cfg.get("layers") or [LAYER_ARN],
                Environment={"Variables": env_vars},
                Description=cfg["description"][:255],
            )
            rpt["deploy"] = "SYNCED"
        wait_active()

        # Register in 4hourly tick
        try:
            m = json.loads(s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read())
            t4 = m.setdefault("ticks", {}).setdefault("4hourly", [])
            if FN not in t4:
                t4.append(FN); t4[:] = sorted(set(t4))
                m["generated_at"] = datetime.now(timezone.utc).isoformat()
                s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY,
                              Body=json.dumps(m, indent=2).encode("utf-8"),
                              ContentType="application/json")
                rpt["manifest"] = f"ADDED to 4hourly (now {len(t4)} jobs)"
            else:
                rpt["manifest"] = f"ALREADY in 4hourly ({len(t4)} jobs)"
        except Exception as e:
            rpt["manifest_err"] = str(e)[:200]

        # Invoke
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=b"{}", LogType="Tail")
        body = json.loads(inv["Payload"].read() or b"{}")
        if isinstance(body, dict) and "body" in body:
            try: body = json.loads(body["body"])
            except Exception: pass
        rpt["invoke_status"] = inv["StatusCode"]
        rpt["invoke_body"] = body
        rpt["invoke_fn_err"] = inv.get("FunctionError")
        rpt["log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1500:]

        # Verify brief written + show a few key fields
        time.sleep(2)
        try:
            brief = json.loads(s3.get_object(Bucket=BUCKET, Key="data/auction-decisive-call.json")["Body"].read())
            rpt["brief"] = {
                "regime": brief.get("regime"),
                "confidence": brief.get("confidence"),
                "one_liner": brief.get("one_liner"),
                "n_evidence": len(brief.get("supporting_evidence") or []),
                "n_analogs": len(brief.get("historical_analogs") or []),
                "n_cross_asset": len(brief.get("cross_asset") or []),
                "n_trades": len(brief.get("trade_ideas") or []),
                "n_tripwires": len(brief.get("tripwires") or []),
                "n_next_auctions": len(brief.get("next_auctions_to_watch") or []),
                "input_state": brief.get("input_state"),
            }
            # Sample one trade idea + one tripwire for visibility
            if brief.get("trade_ideas"): rpt["brief"]["sample_trade"] = brief["trade_ideas"][0]
            if brief.get("tripwires"): rpt["brief"]["sample_tripwire"] = brief["tripwires"][0]
        except ClientError:
            rpt["brief"] = "NOT_WRITTEN"
    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    _save(rpt)


def _save(rpt):
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1124.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items() if k not in ("log_tail","traceback")}, indent=2, default=str)[:2500])


if __name__ == "__main__":
    main()
