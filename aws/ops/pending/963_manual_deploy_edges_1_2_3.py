"""
ops 963 -- manually create-function edges 1, 2, 3 Lambdas
==========================================================

The deploy-lambdas.yml CI workflow is not picking up these three Lambdas
despite source + config.json being in the repo. Manually deploy via Boto3.

Edges:
  #1 justhodl-vix-backwardation-trigger
  #2 justhodl-insider-buys-enriched
  #3 justhodl-breadth-thrust

For each: read config.json, zip source/ contents, create_function or
update_function_code, then invoke + verify S3 output.
"""
import datetime as dt
import io
import json
import os
import time
import zipfile

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=600, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

EDGES = [
    {"edge": 1, "fn": "justhodl-vix-backwardation-trigger",
     "src_dir": "aws/lambdas/justhodl-vix-backwardation-trigger/source",
     "cfg_path": "aws/lambdas/justhodl-vix-backwardation-trigger/config.json",
     "s3_key": "data/vix-backwardation-trigger.json"},
    {"edge": 2, "fn": "justhodl-insider-buys-enriched",
     "src_dir": "aws/lambdas/justhodl-insider-buys-enriched/source",
     "cfg_path": "aws/lambdas/justhodl-insider-buys-enriched/config.json",
     "s3_key": "data/insider-buys-enriched.json"},
    {"edge": 3, "fn": "justhodl-breadth-thrust",
     "src_dir": "aws/lambdas/justhodl-breadth-thrust/source",
     "cfg_path": "aws/lambdas/justhodl-breadth-thrust/config.json",
     "s3_key": "data/breadth-thrust.json"},
]

CHECKS = []


def add(name, ok, detail=""):
    CHECKS.append({"name": name, "passed": bool(ok), "detail": str(detail)[:300]})


def zip_dir(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(src_dir):
            for fname in files:
                if fname.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, fname)
                arc = os.path.relpath(full, src_dir)
                zf.write(full, arc)
    buf.seek(0)
    return buf.getvalue()


def deploy_one(cfg_entry):
    edge = cfg_entry["edge"]
    fn = cfg_entry["fn"]
    src_dir = cfg_entry["src_dir"]
    cfg_path = cfg_entry["cfg_path"]
    s3_key = cfg_entry["s3_key"]

    print(f"\n--- Edge #{edge}: deploying {fn} ---")

    # Read config
    try:
        with open(cfg_path) as f:
            cfg = json.load(f)
        add(f"e{edge}.config_loaded", True, f"runtime={cfg.get('runtime')} mem={cfg.get('memory')}")
    except Exception as ex:
        add(f"e{edge}.config_loaded", False, str(ex)[:200])
        return

    # Zip source
    try:
        zip_bytes = zip_dir(src_dir)
        add(f"e{edge}.zip_built", True, f"size={len(zip_bytes)}B")
    except Exception as ex:
        add(f"e{edge}.zip_built", False, str(ex)[:200])
        return

    # Try update_function_code first; if fn doesn't exist, create_function
    env_vars = cfg.get("env", {})
    desc = (cfg.get("description") or "")[:255]
    try:
        lam.get_function(FunctionName=fn)
        # Exists: update code
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=zip_bytes,
                                      Publish=False)
            add(f"e{edge}.code_updated", True, "code updated on existing function")
        except ClientError as ex:
            add(f"e{edge}.code_updated", False, str(ex)[:200])
            return
        # Wait for code update active
        for _ in range(15):
            v = lam.get_function_configuration(FunctionName=fn)
            if v.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        # Update config (env, runtime, mem, timeout)
        try:
            lam.update_function_configuration(
                FunctionName=fn,
                Runtime=cfg.get("runtime", "python3.12"),
                MemorySize=int(cfg.get("memory", 512)),
                Timeout=int(cfg.get("timeout", 60)),
                Handler=cfg.get("handler", "lambda_function.lambda_handler"),
                Description=desc,
                Environment={"Variables": env_vars},
            )
            add(f"e{edge}.config_applied", True,
                f"runtime/mem/timeout/env updated")
        except ClientError as ex:
            add(f"e{edge}.config_applied", False, str(ex)[:200])
    except ClientError as ex:
        if "ResourceNotFoundException" in str(ex):
            # Create function
            try:
                lam.create_function(
                    FunctionName=fn,
                    Runtime=cfg.get("runtime", "python3.12"),
                    Role=cfg.get("role_arn",
                                 "arn:aws:iam::857687956942:role/lambda-execution-role"),
                    Handler=cfg.get("handler", "lambda_function.lambda_handler"),
                    Code={"ZipFile": zip_bytes},
                    MemorySize=int(cfg.get("memory", 512)),
                    Timeout=int(cfg.get("timeout", 60)),
                    Description=desc,
                    Environment={"Variables": env_vars},
                    Publish=False,
                )
                add(f"e{edge}.created", True, "new function created")
            except ClientError as ex2:
                add(f"e{edge}.created", False, str(ex2)[:240])
                return
        else:
            add(f"e{edge}.lookup_error", False, str(ex)[:200])
            return

    # Wait for ready
    print(f"  waiting for {fn} LastUpdateStatus=Successful...")
    for _ in range(30):
        try:
            v = lam.get_function_configuration(FunctionName=fn)
            state = v.get("State", "")
            status = v.get("LastUpdateStatus", "")
            if state == "Active" and status == "Successful":
                break
        except ClientError:
            pass
        time.sleep(2)

    # Invoke
    print(f"  invoking {fn}...")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                       Payload=b"{}")
        dur = round(time.time() - t0, 1)
        payload = r["Payload"].read().decode()
        try:
            body = json.loads(payload)
            inner = body.get("statusCode", 200)
        except Exception:
            inner = "n/a"
        ok = r["StatusCode"] == 200 and not r.get("FunctionError") and inner == 200
        add(f"e{edge}.invoke", ok,
            f"dur={dur}s outer={r['StatusCode']} inner={inner} body={payload[:200]}")
    except ClientError as ex:
        add(f"e{edge}.invoke", False, str(ex)[:200])

    # Verify S3 output
    time.sleep(2)
    try:
        h = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        age = (dt.datetime.now(dt.timezone.utc) - h["LastModified"]).total_seconds()
        add(f"e{edge}.s3_output_fresh",
            h["ContentLength"] > 500 and age < 600,
            f"size={h['ContentLength']}B age_s={int(age)}")
    except ClientError as ex:
        add(f"e{edge}.s3_output_fresh", False, str(ex)[:200])


def main():
    print(f"ops 963 -- manual deploy edges 1/2/3 at {dt.datetime.utcnow().isoformat()}Z")
    for cfg in EDGES:
        try:
            deploy_one(cfg)
        except Exception as ex:
            add(f"e{cfg['edge']}.unhandled", False, str(ex)[:300])

    rep = {
        "ops": 963,
        "title": "manual deploy edges 1, 2, 3 Lambdas via Boto3 (CI not picking them up)",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {"total": len(CHECKS),
                    "passed": sum(1 for c in CHECKS if c["passed"]),
                    "failed": sum(1 for c in CHECKS if not c["passed"])},
        "overall_ok": all(c["passed"] for c in CHECKS),
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/963_manual_deploy_edges_1_2_3.json", "w") as f:
        json.dump(rep, f, indent=2)
    print(f"\n=== TOTAL {rep['summary']['passed']}/{rep['summary']['total']} ===")
    for c in CHECKS:
        flag = "OK " if c["passed"] else "FAIL"
        print(f"  [{flag}] {c['name']:30} {c['detail'][:140]}")


if __name__ == "__main__":
    main()
