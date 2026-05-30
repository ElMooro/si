"""ops 1127 — Deploy justhodl-ai-brief-router + upload context registry to S3 +
register in scheduler 4hourly tick + invoke + verify all 6 briefs."""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-ai-brief-router"
KEY_SOURCE_LAMBDA = "justhodl-ai-chat"
BUCKET = "justhodl-dashboard-live"
MANIFEST_KEY = "config/schedule-manifest.json"
REGISTRY_KEY = "config/ai-brief-contexts.json"
LAYER_ARN = "arn:aws:lambda:us-east-1:857687956942:layer:justhodl-core:1"

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


def get_source_env():
    try:
        c = lam.get_function_configuration(FunctionName=KEY_SOURCE_LAMBDA)
        return (c.get("Environment") or {}).get("Variables", {}) or {}
    except ClientError:
        return {}


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Upload the context registry to S3
        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        with open(registry_path) as fh:
            registry_body = fh.read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=registry_body.encode("utf-8"),
                       ContentType="application/json")
        registry = json.loads(registry_body)
        ctx_ids = sorted((registry.get("contexts") or {}).keys())
        rpt["registry_uploaded"] = {"key": REGISTRY_KEY, "n_contexts": len(ctx_ids), "contexts": ctx_ids}

        # 2) Deploy Lambda
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

        # 3) Register in scheduler 4hourly tick
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

        # 4) Invoke for first run
        print(f"[1127] invoking — should generate {len(ctx_ids)} briefs in parallel")
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=b"{}", LogType="Tail")
        body = json.loads(inv["Payload"].read() or b"{}")
        if isinstance(body, dict) and "body" in body:
            try: body = json.loads(body["body"])
            except Exception: pass
        rpt["invoke_status"] = inv["StatusCode"]
        rpt["invoke_body"] = body
        rpt["invoke_fn_err"] = inv.get("FunctionError")
        rpt["log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-2000:]

        # 5) Verify each output file written + sample one prediction per context
        time.sleep(3)
        verify = []
        for ctx_id in ctx_ids:
            ctx_cfg = (registry.get("contexts") or {}).get(ctx_id, {})
            out_key = f"data/{ctx_cfg.get('output_key', ctx_id)}.json"
            row = {"context": ctx_id, "output_key": out_key}
            try:
                brief = json.loads(s3.get_object(Bucket=BUCKET, Key=out_key)["Body"].read())
                row["regime"] = brief.get("regime")
                row["confidence"] = brief.get("confidence")
                row["one_liner"] = (brief.get("one_liner") or "")[:140]
                row["n_predictions"] = len(brief.get("historical_predictions") or [])
                row["n_trades"] = len(brief.get("trade_ideas") or [])
                row["n_tripwires"] = len(brief.get("tripwires") or [])
                row["generated_at"] = brief.get("generated_at")
                # Sample one prediction (the BTC one if present)
                for p in (brief.get("historical_predictions") or []):
                    if p.get("ticker") == "BTC":
                        row["btc_pred"] = {
                            "dir": p.get("prediction_direction"),
                            "range": f"{p.get('prediction_range_low_pct')}% to {p.get('prediction_range_high_pct')}%",
                            "horizon_wk": p.get("prediction_horizon_weeks"),
                            "prob": p.get("probability_pct"),
                            "analog": p.get("best_analog_period"),
                        }
                        break
                row["status"] = "OK"
            except ClientError:
                row["status"] = "NOT_WRITTEN"
            except Exception as e:
                row["status"] = "ERR"; row["err"] = str(e)[:200]
            verify.append(row)
        rpt["verify"] = verify
        rpt["n_briefs_written"] = sum(1 for r in verify if r.get("status") == "OK")

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    _save(rpt)


def _save(rpt):
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1127.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items() if k not in ("log_tail","traceback")},
                     indent=2, default=str)[:3500])


if __name__ == "__main__":
    main()
