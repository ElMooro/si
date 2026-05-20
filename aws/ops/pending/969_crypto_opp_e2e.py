"""
ops 969 -- crypto-opportunities engine end-to-end verification
================================================================

Engine shipped in commit 2ae40c91. Verify:
  1. Lambda deployed (or deploy via Boto3 if CI missed it)
  2. Invoke succeeds
  3. S3 output has expected schema
  4. Page is live and wired
  5. Linked from dex.html and other crypto entry points
  6. Schedule configured (4h cadence per config description)

If Lambda not yet deployed, manually deploy via Boto3 (same pattern as ops 963).
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

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=320, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)

FN = "justhodl-crypto-opportunities"
SRC = "aws/lambdas/justhodl-crypto-opportunities/source"
CFG = "aws/lambdas/justhodl-crypto-opportunities/config.json"
S3_KEY = "data/crypto-opportunities.json"
PAGE = "crypto-opportunities.html"

CHECKS = []


def add(n, ok, d):
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


# ── Step 1: Lambda deployment status ──
deployed = False
try:
    info = lam.get_function(FunctionName=FN)
    cfg = info["Configuration"]
    add("lambda.deployed", True,
        f"runtime={cfg.get('Runtime')} mem={cfg.get('MemorySize')} timeout={cfg.get('Timeout')} mod={cfg.get('LastModified','')[:19]}")
    deployed = True
except ClientError as e:
    if "ResourceNotFoundException" in str(e):
        add("lambda.deployed", False, "NOT DEPLOYED -- will create via Boto3 fallback")
    else:
        add("lambda.deployed", False, str(e)[:200])

if not deployed:
    print("Lambda not deployed -- creating via Boto3 fallback")
    try:
        with open(CFG) as f:
            config = json.load(f)
        add("config.loaded", True, f"runtime={config.get('runtime')}")
    except Exception as ex:
        add("config.loaded", False, str(ex)[:200])
        write_report()
        return

    # Inherit standard secrets from buyback-scanner
    try:
        src_env = lam.get_function_configuration(
            FunctionName="justhodl-buyback-scanner"
        ).get("Environment", {}).get("Variables", {})
        std_keys = ["FMP_KEY", "FRED_KEY", "POLYGON_KEY", "ALPHA_VANTAGE_KEY",
                    "CMC_KEY", "ANTHROPIC_API_KEY", "TELEGRAM_BOT_TOKEN",
                    "TELEGRAM_CHAT_ID", "TELEGRAM_TOKEN", "NEWSAPI_KEY",
                    "BLS_KEY", "BEA_KEY", "CENSUS_KEY"]
        env_vars = {k: src_env[k] for k in std_keys if k in src_env}
        env_vars.update(config.get("env", {}))
        add("inherit_env.bundle", True, f"n={len(env_vars)} vars")
    except ClientError as ex:
        add("inherit_env.bundle", False, str(ex)[:200])
        env_vars = config.get("env", {})

    # Build zip
    try:
        zip_bytes = zip_dir(SRC)
        add("zip.built", True, f"size={len(zip_bytes)}B")
    except Exception as ex:
        add("zip.built", False, str(ex)[:200])
        write_report()
        return

    # Create
    desc = (config.get("description") or "")[:255]
    try:
        lam.create_function(
            FunctionName=FN,
            Runtime=config.get("runtime", "python3.12"),
            Role=config.get("role_arn",
                            "arn:aws:iam::857687956942:role/lambda-execution-role"),
            Handler=config.get("handler", "lambda_function.lambda_handler"),
            Code={"ZipFile": zip_bytes},
            MemorySize=int(config.get("memory", 512)),
            Timeout=int(config.get("timeout", 240)),
            Description=desc,
            Environment={"Variables": env_vars},
            Publish=False,
        )
        add("lambda.created", True, "ok")
        # Wait active
        for _ in range(30):
            v = lam.get_function_configuration(FunctionName=FN)
            if v.get("State") == "Active" and v.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        deployed = True
    except ClientError as ex:
        add("lambda.created", False, str(ex)[:240])
        write_report()
        return

# ── Step 2: Invoke ──
print(f"Invoking {FN}...")
t0 = time.time()
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    dur = round(time.time() - t0, 1)
    payload = r["Payload"].read().decode()
    try:
        body = json.loads(payload)
        inner = body.get("statusCode", 200)
    except Exception:
        inner = "n/a"
    ok = r["StatusCode"] == 200 and not r.get("FunctionError") and inner == 200
    add("lambda.invoke", ok,
        f"dur={dur}s outer={r['StatusCode']} inner={inner} body={payload[:240]}")
except ClientError as ex:
    add("lambda.invoke", False, str(ex)[:200])

# ── Step 3: S3 output schema ──
time.sleep(3)
try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    size = obj["ContentLength"]
    d = json.loads(obj["Body"].read())
    age_h = -1
    try:
        ts = dt.datetime.fromisoformat(d.get("as_of","").replace("Z","+00:00"))
        age_h = (dt.datetime.now(dt.timezone.utc) - ts).total_seconds() / 3600
    except Exception:
        pass
    add("s3.output_present", size > 1000, f"size={size}B age_h={round(age_h,1)}")

    # Schema checks
    add("schema.engine_field", d.get("engine") == "crypto-opportunities",
        d.get("engine"))
    add("schema.state_machine", d.get("state") in
        ("OPPORTUNITY_RICH", "ACTIVE", "NORMAL", "QUIET"),
        f"state={d.get('state')} signal={d.get('signal_strength')}")
    add("schema.summary_present", isinstance(d.get("summary"), dict),
        f"keys={list((d.get('summary') or {}).keys())[:8]}")
    s = d.get("summary") or {}
    add("schema.universe_built",
        (s.get("filtered_universe_size") or 0) >= 50,
        f"universe_size={s.get('universe_size')} filtered={s.get('filtered_universe_size')} n_enriched={s.get('n_enriched')}")
    add("schema.three_signals",
        all(k in s for k in ("n_volume_surge", "n_social_velocity", "n_stable_inflows")),
        f"vol={s.get('n_volume_surge')} soc={s.get('n_social_velocity')} stable={s.get('n_stable_inflows')} conv={s.get('n_convergence')}")
    add("schema.convergence_table",
        isinstance(d.get("convergence"), list),
        f"n_convergence_rows={len(d.get('convergence') or [])}")
    add("schema.top_volume_surge",
        isinstance(d.get("top_volume_surge"), list),
        f"n={len(d.get('top_volume_surge') or [])}")
    add("schema.top_social_velocity",
        isinstance(d.get("top_social_velocity"), list),
        f"n={len(d.get('top_social_velocity') or [])}")
    add("schema.top_stable_inflows",
        isinstance(d.get("top_stable_inflows"), list),
        f"n={len(d.get('top_stable_inflows') or [])}")
    add("schema.forward_expectations",
        isinstance(d.get("forward_expectations"), dict),
        f"states={list((d.get('forward_expectations') or {}).keys())}")
    add("schema.trigger_conditions",
        isinstance(d.get("trigger_conditions"), list) and len(d.get("trigger_conditions",[])) >= 3,
        f"n={len(d.get('trigger_conditions') or [])}")
    add("schema.why_now",
        bool(d.get("why_now_explainer")),
        f"len={len(d.get('why_now_explainer') or '')}")

    # Sanity: first convergence coin if any
    conv = d.get("convergence") or []
    if conv:
        c0 = conv[0]
        add("schema.trade_ticket",
            "trade_ticket" in c0 and "entry_zone" in (c0.get("trade_ticket") or {}),
            f"first_coin={c0.get('symbol')} n_signals={c0.get('n_signals')} ticket_keys={list((c0.get('trade_ticket') or {}).keys())[:6]}")
    else:
        add("schema.trade_ticket", True, "no convergence coins right now (acceptable)")
except ClientError as ex:
    add("s3.output_present", False, str(ex)[:200])

# ── Step 4: Page live ──
try:
    req = urllib.request.Request(f"https://justhodl.ai/{PAGE}",
                                 headers={"User-Agent": "ops/969"})
    with urllib.request.urlopen(req, timeout=15) as r:
        body = r.read().decode("utf-8", errors="ignore")
    add("page.live", r.status == 200 and len(body) > 5000,
        f"status={r.status} size={len(body)}")
    add("page.wired_to_s3",
        "crypto-opportunities.json" in body,
        "S3 key referenced in page")
    # Check it has the four expected tables / state banner
    expected_markers = ["convergence", "volume", "social", "stable", "OPPORTUNITY"]
    found = [m for m in expected_markers if m.lower() in body.lower()]
    add("page.markers_present", len(found) >= 4,
        f"found={found}")
except Exception as ex:
    add("page.live", False, str(ex)[:200])

# ── Step 5: Linked from dex.html ──
try:
    req = urllib.request.Request("https://justhodl.ai/dex.html",
                                 headers={"User-Agent": "ops/969"})
    with urllib.request.urlopen(req, timeout=15) as r:
        body = r.read().decode("utf-8", errors="ignore")
    add("nav.linked_from_dex",
        "crypto-opportunities.html" in body or "crypto-opportunities" in body.lower(),
        "OPPORTUNITIES link present in dex.html nav")
except Exception as ex:
    add("nav.linked_from_dex", False, str(ex)[:200])

# ── Step 6: Schedule configured ──
try:
    paginator = events.get_paginator("list_rule_names_by_target")
    fn_arn = f"arn:aws:lambda:{REGION}:857687956942:function:{FN}"
    rules = []
    for page in paginator.paginate(TargetArn=fn_arn):
        rules.extend(page.get("RuleNames", []))
    add("schedule.eventbridge_rule",
        len(rules) >= 1,
        f"rules={rules}")
except ClientError as ex:
    add("schedule.eventbridge_rule", False, str(ex)[:200])

write_report()


def write_report():
    rep = {
        "ops": 969,
        "title": "crypto-opportunities engine end-to-end verification",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {"total": len(CHECKS),
                    "passed": sum(1 for c in CHECKS if c["passed"]),
                    "failed": sum(1 for c in CHECKS if not c["passed"])},
        "overall_ok": all(c["passed"] for c in CHECKS),
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/969_crypto_opp_e2e.json", "w") as f:
        json.dump(rep, f, indent=2)
    p = rep["summary"]["passed"]
    t = rep["summary"]["total"]
    print(f"\n=== {p}/{t} ({100*p//max(t,1)}%) ===")
    for c in CHECKS:
        flag = "OK " if c["passed"] else "FAIL"
        print(f"  [{flag}] {c['name']:32} {c['detail'][:160]}")


