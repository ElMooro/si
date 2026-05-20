"""
ops 970 -- force-deploy + verify justhodl-crypto-opportunities
================================================================

CI may have failed to deploy on first commit (same issue as edges 1-3
before the deploy-lambdas.yml fixes). This ops:

1. Checks current AWS state of the Lambda
2. If not deployed: zip source + Boto3 create_function with config.json + inherit env from buyback-scanner
3. If deployed but env empty: update env vars
4. Invoke the Lambda (240s timeout)
5. Verify S3 output schema
6. Verify live page (justhodl.ai/crypto-opportunities.html)

Same pattern as ops 963 (which successfully created edges 1-3 manually).
"""
import datetime as dt
import io
import json
import os
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
FN = "justhodl-crypto-opportunities"
SRC_DIR = "aws/lambdas/justhodl-crypto-opportunities/source"
CFG_PATH = "aws/lambdas/justhodl-crypto-opportunities/config.json"
S3_KEY = "data/crypto-opportunities.json"
PAGE_URL = "https://justhodl.ai/crypto-opportunities.html"

# Standard secrets bundle that's inherited (mirror what deploy-lambdas.yml does for inherit_env:true)
STANDARD_KEYS = ["CMC_KEY", "FMP_KEY", "FRED_KEY", "POLYGON_KEY",
                 "ALPHA_VANTAGE_KEY", "ANTHROPIC_API_KEY",
                 "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "TELEGRAM_TOKEN",
                 "NEWSAPI_KEY", "BLS_KEY", "BEA_KEY", "CENSUS_KEY"]
DONOR = "justhodl-buyback-scanner"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

CHECKS = []

def add(n, ok, d=""):
    CHECKS.append({"name": n, "passed": bool(ok), "detail": str(d)[:300]})


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


def get_donor_env():
    """Pull standard secrets bundle from buyback-scanner."""
    try:
        info = lam.get_function_configuration(FunctionName=DONOR)
        donor_env = info.get("Environment", {}).get("Variables", {})
        env = {"S3_BUCKET": S3_BUCKET}
        for k in STANDARD_KEYS:
            v = donor_env.get(k)
            if v:
                env[k] = v
        return env
    except ClientError as e:
        print(f"  WARN: donor lookup failed: {e}")
        return {"S3_BUCKET": S3_BUCKET}


def deploy_or_update():
    """Create or update the Lambda. Returns config dict if successful."""
    # Read repo config
    with open(CFG_PATH) as f:
        cfg = json.load(f)
    desc = (cfg.get("description") or "")[:255]
    runtime = cfg.get("runtime", "python3.12")
    memory = int(cfg.get("memory", 512))
    timeout = int(cfg.get("timeout", 240))
    handler = cfg.get("handler", "lambda_function.lambda_handler")
    role = cfg.get("role_arn",
                   "arn:aws:iam::857687956942:role/lambda-execution-role")

    # Build zip
    zip_bytes = zip_dir(SRC_DIR)
    add("zip_built", True, f"{len(zip_bytes)}B")

    # Inherited env
    env = get_donor_env()
    add("donor_env_loaded", "CMC_KEY" in env,
        f"keys={sorted(env.keys())} CMC_KEY_present={('CMC_KEY' in env)}")

    # Check if exists
    try:
        lam.get_function(FunctionName=FN)
        exists = True
    except ClientError as e:
        exists = "ResourceNotFoundException" not in str(e)
        if "ResourceNotFoundException" in str(e):
            exists = False
        else:
            raise

    if exists:
        # Update code
        lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes, Publish=False)
        for _ in range(15):
            v = lam.get_function_configuration(FunctionName=FN)
            if v.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        # Update config
        lam.update_function_configuration(
            FunctionName=FN,
            Runtime=runtime, MemorySize=memory, Timeout=timeout,
            Handler=handler, Description=desc,
            Environment={"Variables": env},
        )
        add("update_function", True, "updated existing")
    else:
        # Create
        lam.create_function(
            FunctionName=FN, Runtime=runtime, Role=role,
            Handler=handler, Code={"ZipFile": zip_bytes},
            MemorySize=memory, Timeout=timeout, Description=desc,
            Environment={"Variables": env}, Publish=False,
        )
        add("create_function", True, "new function created")

    # Wait active
    for _ in range(30):
        try:
            v = lam.get_function_configuration(FunctionName=FN)
            if v.get("State") == "Active" and v.get("LastUpdateStatus") == "Successful":
                return v
        except ClientError:
            pass
        time.sleep(2)
    return lam.get_function_configuration(FunctionName=FN)


def main():
    print(f"ops 970 -- force deploy + verify {FN} at {dt.datetime.utcnow().isoformat()}Z")

    # 1. Deploy (create or update)
    try:
        cfg = deploy_or_update()
        env = cfg.get("Environment", {}).get("Variables", {})
        add("lambda.deployed", True,
            f"runtime={cfg.get('Runtime')} mem={cfg.get('MemorySize')} timeout={cfg.get('Timeout')} env_keys={len(env)}")
        add("lambda.has_cmc_key", "CMC_KEY" in env and len(env.get("CMC_KEY", "")) > 10,
            f"CMC_KEY_present={('CMC_KEY' in env)}")
    except Exception as e:
        add("lambda.deployed", False, str(e)[:300])
        write_report()
        return

    # 2. Invoke (240s timeout, this engine fetches ~40 CoinGecko + ~40 CMC pairs)
    print(f"  invoking {FN} (may take 60-180s)...")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       LogType="Tail", Payload=b"{}")
        dur = round(time.time() - t0, 1)
        payload_raw = r["Payload"].read().decode()
        try:
            payload = json.loads(payload_raw)
            inner = payload.get("statusCode", 200)
            body = payload.get("body", "")
            try:
                body_json = json.loads(body) if isinstance(body, str) else body
            except Exception:
                body_json = {}
        except Exception:
            inner = "n/a"
            body_json = {}
        ok = r["StatusCode"] == 200 and not r.get("FunctionError") and inner == 200
        # Decode log tail for debugging
        import base64
        log = ""
        if r.get("LogResult"):
            try:
                log = base64.b64decode(r["LogResult"]).decode("utf-8", errors="ignore")
            except Exception:
                log = ""
        relevant_log = [l for l in log.split("\n") if any(k in l.lower() for k in
                                                          ["error","fetched","scan","enriching","report"])]
        add("invoke.success", ok,
            f"dur={dur}s outer={r['StatusCode']} inner={inner} "
            f"state={body_json.get('state')} conv={body_json.get('n_convergence')} "
            f"vol={body_json.get('n_volume_surge')} soc={body_json.get('n_social_velocity')} "
            f"stb={body_json.get('n_stable_inflows')} log_tail={relevant_log[-4:]}")
    except ClientError as e:
        add("invoke.success", False, str(e)[:240])

    # 3. S3 schema
    time.sleep(3)
    try:
        h = s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        age_s = (dt.datetime.now(dt.timezone.utc) - h["LastModified"]).total_seconds()
        add("s3.fresh", h["ContentLength"] > 1000 and age_s < 600,
            f"size={h['ContentLength']}B age_s={int(age_s)}")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        d = json.loads(obj["Body"].read())
        required = ["engine", "as_of", "state", "summary", "convergence",
                    "top_volume_surge", "top_social_velocity", "top_stable_inflows",
                    "why_now_explainer", "methodology"]
        missing = [k for k in required if k not in d]
        add("s3.schema_complete", len(missing) == 0,
            f"missing={missing} engine={d.get('engine')} version={d.get('version')}")
        sm = d.get("summary", {})
        add("s3.scan_executed", sm.get("universe_size", 0) >= 100,
            f"universe={sm.get('universe_size')} filtered={sm.get('filtered_universe_size')} "
            f"enriched={sm.get('n_enriched')} state={d.get('state')}")
        # Trade tickets check
        sample_rows = (d.get("top_volume_surge") or [])[:2] + (d.get("convergence") or [])[:2]
        all_have_tickets = all(isinstance(r.get("trade_ticket"), dict) for r in sample_rows)
        add("s3.trade_tickets",
            all_have_tickets or len(sample_rows) == 0,
            f"sample_rows={len(sample_rows)} all_have_tickets={all_have_tickets}")
    except ClientError as e:
        add("s3.fresh", False, str(e)[:200])
    except Exception as e:
        add("s3.schema_complete", False, str(e)[:200])

    # 4. Live page
    try:
        req = urllib.request.Request(PAGE_URL, headers={"User-Agent": "ops/970"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="ignore")
        markers = ["tbodyConv", "tbodyVol", "tbodySoc", "tbodyStb",
                   "Convergence", "Volume Surge", "Social Velocity",
                   "Stablecoin Inflows", "crypto-opportunities.json"]
        missing = [m for m in markers if m not in body]
        add("page.live_and_wired",
            r.status == 200 and len(body) > 5000 and len(missing) == 0,
            f"status={r.status} size={len(body)} missing={missing}")
    except Exception as e:
        add("page.live_and_wired", False, str(e)[:200])

    # 5. dex.html nav
    try:
        req = urllib.request.Request("https://justhodl.ai/dex.html",
                                     headers={"User-Agent": "ops/970"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="ignore")
        add("dex.nav_link",
            "/crypto-opportunities.html" in body and "OPPORTUNITIES" in body,
            "link in topnav" if "/crypto-opportunities.html" in body else "missing")
    except Exception as e:
        add("dex.nav_link", False, str(e)[:200])

    write_report()


def write_report():
    rep = {
        "ops": 970,
        "title": "force-deploy + verify justhodl-crypto-opportunities",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {"total": len(CHECKS),
                    "passed": sum(1 for c in CHECKS if c["passed"]),
                    "failed": sum(1 for c in CHECKS if not c["passed"])},
        "overall_ok": all(c["passed"] for c in CHECKS),
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/970_crypto_opportunities_deploy_verify.json", "w") as f:
        json.dump(rep, f, indent=2)
    p, t = rep["summary"]["passed"], rep["summary"]["total"]
    print(f"\n=== {p}/{t} ({100*p//max(t,1)}%) ===")
    for c in CHECKS:
        flag = "OK  " if c["passed"] else "FAIL"
        print(f"  [{flag}] {c['name']:32} {c['detail'][:140]}")


if __name__ == "__main__":
    main()
