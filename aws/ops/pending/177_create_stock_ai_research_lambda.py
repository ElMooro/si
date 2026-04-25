#!/usr/bin/env python3
"""
Step 177 — Create justhodl-stock-ai-research Lambda + URL.

The deploy-lambdas.yml workflow only UPDATES existing Lambdas. New
Lambdas need creating via boto3. This step is idempotent: checks if
already exists, only creates if not.

Steps:
  A. Read config.json
  B. Build deployment zip from source/
  C. Get ANTHROPIC_KEY from a sibling Lambda (so we don't have to
     plumb it through GitHub secrets)
  D. Create function (or update if it exists)
  E. Create Function URL (or skip if exists)
  F. Smoke test with ?ticker=AAPL
"""
import io
import json
import os
import time
import zipfile
from pathlib import Path
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-stock-ai-research"
LAMBDA_DIR = Path(__file__).resolve().parents[3] / "aws" / "lambdas" / LAMBDA_NAME

lam = boto3.client("lambda", region_name=REGION)


def build_zip():
    """Zip the source/ directory."""
    src = LAMBDA_DIR / "source"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in src.rglob("*"):
            if path.is_file():
                zf.write(path, arcname=str(path.relative_to(src)))
    buf.seek(0)
    return buf.read()


def get_anthropic_key():
    """Pull ANTHROPIC_KEY from existing investor-agents Lambda env."""
    cfg = lam.get_function_configuration(FunctionName="justhodl-investor-agents")
    env = cfg.get("Environment", {}).get("Variables", {})
    key = env.get("ANTHROPIC_KEY") or env.get("ANTHROPIC_API_KEY")
    if not key:
        raise Exception("No Anthropic key on investor-agents Lambda")
    return key


with report("create_stock_ai_research_lambda") as r:
    r.heading("Create justhodl-stock-ai-research Lambda")

    # ─── A. Read config ─────────────────────────────────────────────────
    r.section("A. Read config")
    cfg = json.loads((LAMBDA_DIR / "config.json").read_text())
    r.log(f"  function_name: {cfg['function_name']}")
    r.log(f"  runtime:       {cfg['runtime']}")
    r.log(f"  memory:        {cfg['memory_size']}MB  timeout: {cfg['timeout']}s")
    r.log(f"  reserved_conc: {cfg['reserved_concurrency']}")

    # ─── B. Build zip ───────────────────────────────────────────────────
    r.section("B. Build deployment zip")
    zip_bytes = build_zip()
    r.log(f"  zip size: {len(zip_bytes)} bytes")

    # ─── C. Get keys from sibling Lambda ───────────────────────────────
    r.section("C. Pull ANTHROPIC_KEY from investor-agents Lambda")
    ant_key = get_anthropic_key()
    r.log(f"  Anthropic key prefix: {ant_key[:14]}...")

    fmp_key = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"  # consistent with sibling Lambdas

    # ─── D. Create function (or update if exists) ──────────────────────
    r.section("D. Create or update function")
    exists = False
    try:
        existing = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
        exists = True
        r.log(f"  Function already exists; will update code + config")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        r.log(f"  Function does not exist; will create")

    env_vars = {
        "ANTHROPIC_KEY": ant_key,
        "FMP_KEY": fmp_key,
    }

    if not exists:
        resp = lam.create_function(
            FunctionName=LAMBDA_NAME,
            Runtime=cfg["runtime"],
            Role=cfg["role"],
            Handler=cfg["handler"],
            Code={"ZipFile": zip_bytes},
            Description=cfg.get("description", ""),
            Timeout=cfg["timeout"],
            MemorySize=cfg["memory_size"],
            Architectures=cfg["architectures"],
            Environment={"Variables": env_vars},
            Publish=False,
        )
        r.ok(f"  ✅ Created (CodeSha256={resp.get('CodeSha256','?')[:12]}...)")

        # reserved concurrency
        if cfg.get("reserved_concurrency") is not None:
            lam.put_function_concurrency(
                FunctionName=LAMBDA_NAME,
                ReservedConcurrentExecutions=cfg["reserved_concurrency"],
            )
            r.log(f"  reserved concurrency: {cfg['reserved_concurrency']}")
    else:
        # Update code
        lam.update_function_code(
            FunctionName=LAMBDA_NAME,
            ZipFile=zip_bytes,
            Publish=False,
        )
        for _ in range(30):
            c = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        # Update config
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Timeout=cfg["timeout"],
            MemorySize=cfg["memory_size"],
            Environment={"Variables": env_vars},
        )
        for _ in range(30):
            c = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        r.ok(f"  ✅ Updated")

    # ─── E. Create Function URL ────────────────────────────────────────
    r.section("E. Create Function URL")
    try:
        existing_url = lam.get_function_url_config(FunctionName=LAMBDA_NAME)
        url = existing_url["FunctionUrl"]
        r.log(f"  Function URL already exists: {url}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        url_cfg = cfg["function_url"]
        resp = lam.create_function_url_config(
            FunctionName=LAMBDA_NAME,
            AuthType=url_cfg["auth_type"],
            Cors=url_cfg["cors"],
        )
        url = resp["FunctionUrl"]
        r.ok(f"  ✅ Created Function URL: {url}")

        # Add resource policy for public access
        try:
            lam.add_permission(
                FunctionName=LAMBDA_NAME,
                StatementId="PublicFunctionUrlInvoke",
                Action="lambda:InvokeFunctionUrl",
                Principal="*",
                FunctionUrlAuthType="NONE",
            )
            r.log(f"  Added public invoke permission")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                r.log(f"  Public invoke permission already exists")
            else:
                raise

    # ─── F. Smoke test ──────────────────────────────────────────────────
    r.section("F. Smoke test with ticker=AAPL")
    time.sleep(3)  # let DNS/cold-start settle
    test_event = {
        "queryStringParameters": {"ticker": "AAPL"},
        "requestContext": {"http": {"method": "GET"}},
    }
    t0 = time.time()
    resp = lam.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps(test_event),
    )
    elapsed = time.time() - t0
    payload = resp.get("Payload").read().decode()

    if resp.get("FunctionError"):
        r.fail(f"  ✗ FunctionError ({elapsed:.1f}s): {payload[:600]}")
        raise SystemExit(1)

    body = json.loads(payload)
    if body.get("statusCode") != 200:
        r.fail(f"  ✗ Status {body.get('statusCode')} ({elapsed:.1f}s): {body.get('body','')[:400]}")
        raise SystemExit(1)

    inner = json.loads(body.get("body", "{}"))
    r.ok(f"  ✅ Returned 200 in {elapsed:.1f}s")
    r.log(f"")
    r.log(f"  Company: {inner.get('company',{}).get('name','?')}")
    r.log(f"  Sector:  {inner.get('company',{}).get('sector','?')}")
    r.log(f"  Price:   ${inner.get('snapshot',{}).get('price','?')}")
    r.log(f"  Mcap:    ${(inner.get('snapshot',{}).get('market_cap',0) or 0)/1e9:.1f}B")
    r.log(f"  Model:   {inner.get('model','?')}")
    r.log(f"  Cached:  {inner.get('from_cache')}")
    r.log(f"  Lambda runtime: {inner.get('elapsed_seconds')}s")
    r.log(f"")

    ai = inner.get("ai", {}) or {}
    r.log(f"  AI Description: {(ai.get('description') or '?')[:150]}...")
    r.log(f"")
    bull = ai.get("bull_case", {}) or {}
    r.log(f"  Bull thesis:  {(bull.get('thesis') or '?')[:120]}")
    r.log(f"  Bull drivers: {bull.get('key_drivers', [])[:3]}")
    r.log(f"")
    bear = ai.get("bear_case", {}) or {}
    r.log(f"  Bear thesis:  {(bear.get('thesis') or '?')[:120]}")
    r.log(f"  Bear risks:   {bear.get('key_risks', [])[:3]}")
    r.log(f"")
    sc = ai.get("scenarios", {}) or {}
    for h in ("horizon_1m", "horizon_1q", "horizon_1y"):
        sh = sc.get(h, {}) or {}
        r.log(f"  {h:14}: bull=${sh.get('bull')}  base=${sh.get('base')}  bear=${sh.get('bear')}")

    r.kv(
        url=url,
        smoke_test_pass=True,
        elapsed_seconds=round(elapsed, 1),
        company=inner.get("company", {}).get("name", "?"),
    )
    r.log(f"\n  Function URL: {url}")
    r.log(f"  Use in frontend: const AI_URL = '{url}';")
    r.log("Done")
