"""ops 1129 — Upload 12-context registry (Tier 1 + Tier 2 + auction migration) to S3,
invoke router (12 contexts in parallel-batched), verify all 12 briefs land."""
import json, os, time, traceback, base64
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


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        with open(registry_path) as fh:
            body = fh.read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=body.encode("utf-8"),
                       ContentType="application/json")
        registry = json.loads(body)
        ctx_ids = sorted((registry.get("contexts") or {}).keys())
        rpt["registry_uploaded"] = {"key": REGISTRY_KEY, "n_contexts": len(ctx_ids), "contexts": ctx_ids}

        # Invoke router — runs all 12 contexts in parallel (6 workers, so 2 batches)
        print(f"[1129] invoking router for {len(ctx_ids)} contexts")
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=b"{}", LogType="Tail")
        body_resp = json.loads(inv["Payload"].read() or b"{}")
        if isinstance(body_resp, dict) and "body" in body_resp:
            try: body_resp = json.loads(body_resp["body"])
            except Exception: pass
        rpt["invoke_status"] = inv["StatusCode"]
        rpt["invoke_fn_err"] = inv.get("FunctionError")
        rpt["invoke_summary"] = {
            "duration_s": body_resp.get("duration_s") if isinstance(body_resp, dict) else None,
            "n_contexts": body_resp.get("n_contexts") if isinstance(body_resp, dict) else None,
            "n_ok": body_resp.get("n_ok") if isinstance(body_resp, dict) else None,
        }
        rpt["log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-2500:]

        # Verify each output file landed + sample BTC prediction per context
        time.sleep(3)
        verify = []
        for ctx_id in ctx_ids:
            ctx_cfg = (registry.get("contexts") or {}).get(ctx_id, {})
            out_key = f"data/{ctx_cfg.get('output_key', ctx_id)}.json"
            row = {"context": ctx_id, "output_key": out_key}
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=out_key)
                brief = json.loads(obj["Body"].read())
                last_mod = obj.get("LastModified")
                # Only count as freshly-generated if LastModified within last ~5 min
                age_sec = (datetime.now(timezone.utc) - last_mod).total_seconds() if last_mod else 9e9
                row["regime"] = brief.get("regime")
                row["confidence"] = brief.get("confidence")
                row["one_liner"] = (brief.get("one_liner") or "")[:140]
                row["n_predictions"] = len(brief.get("historical_predictions") or [])
                row["n_trades"] = len(brief.get("trade_ideas") or [])
                row["n_tripwires"] = len(brief.get("tripwires") or [])
                row["generated_at"] = brief.get("generated_at")
                row["age_sec"] = round(age_sec, 1)
                row["fresh"] = age_sec < 600
                for p in (brief.get("historical_predictions") or []):
                    if p.get("ticker") == "BTC":
                        row["btc"] = {
                            "dir": p.get("prediction_direction"),
                            "range": f"{p.get('prediction_range_low_pct')}% to {p.get('prediction_range_high_pct')}%",
                            "wk": p.get("prediction_horizon_weeks"),
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
        rpt["n_fresh"] = sum(1 for r in verify if r.get("fresh"))
        rpt["n_briefs_exist"] = sum(1 for r in verify if r.get("status") == "OK")

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1129.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items() if k not in ("log_tail","traceback")},
                     indent=2, default=str)[:3500])


if __name__ == "__main__":
    main()
