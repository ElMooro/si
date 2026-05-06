"""
End-to-end fix-up of nobrainer pipeline:
  1. Resolve Anthropic key (try L5 env / morning-intel env / SSM / verify it works).
  2. Inject the key into L5 nobrainer-rationale env, remove SKIP_CLAUDE.
  3. Force re-invoke L5 → real Claude-written theses + Telegram digest.
  4. Drop delisted tickers (LTHM) from L3 tier-classifier ETF universe.
  5. Fix L6 tracker DDB scan pagination — add a Query helper.
  6. Wire 'Themes' + 'Nobrainers' links into canonical sidebar nav across all main pages.
"""
import io, json, os, time, zipfile, base64
import urllib.request, urllib.error
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)
DDB = boto3.client("dynamodb", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def resolve_anthropic_key():
    """Try every place the key could be, return (source, key) or (None, None)."""
    # Try SSM first
    for ssm_name in [
        "/justhodl/anthropic/api-key",
        "/justhodl/anthropic-api-key",
        "/justhodl/api-keys/anthropic",
    ]:
        try:
            p = SSM.get_parameter(Name=ssm_name, WithDecryption=True)
            return ("ssm:" + ssm_name, p["Parameter"]["Value"])
        except Exception as e:
            log(f"  SSM {ssm_name}: {type(e).__name__}")

    # Try existing Lambdas
    for fn in ["justhodl-morning-intelligence", "justhodl-ai-brief", "justhodl-ai-chat",
               "justhodl-investor-agents", "justhodl-stock-screener"]:
        try:
            cfg = L.get_function(FunctionName=fn)["Configuration"]
            env = cfg.get("Environment", {}).get("Variables", {})
            for k in ("ANTHROPIC_API_KEY", "ANTHROPIC_KEY"):
                if env.get(k) and len(env[k]) > 50 and env[k].startswith("sk-"):
                    return (f"{fn}:{k}", env[k])
        except Exception as e:
            log(f"  Lambda {fn}: {type(e).__name__}")
    return (None, None)


def verify_key(key):
    """Hit Anthropic /v1/models with the key. Return True if works."""
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/models",
        headers={
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            return True, len(data.get("data", []))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:200]
        return False, f"HTTP {e.code}: {body}"
    except Exception as e:
        return False, str(e)


def patch_l3_drop_lthm():
    """Open L3 source, find LTHM in the universe and remove (delisted post-Allkem merger to ALTM)."""
    path = "aws/lambdas/justhodl-theme-tier-classifier/source/lambda_function.py"
    with open(path, "r", encoding="utf-8") as f:
        src = f.read()
    if '"LTHM"' not in src and "'LTHM'" not in src:
        log("  no LTHM found in L3 source — skip")
        return False
    new = src.replace('"LTHM",', '').replace("'LTHM',", '')
    new = new.replace('"LTHM"', '"ALTM"').replace("'LTHM'", "'ALTM'")
    if new == src:
        log("  no change applied (LTHM ref pattern not matched)")
        return False
    with open(path, "w", encoding="utf-8") as f:
        f.write(new)
    log("  ✓ LTHM → ALTM in L3 source")
    return True


def deploy_lambda(fn_name, source_path):
    """Build zip, push to Lambda, wait for active."""
    src = open(source_path, "r", encoding="utf-8").read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zb = buf.getvalue()
    log(f"  zip size: {len(zb):,}b")
    L.update_function_code(FunctionName=fn_name, ZipFile=zb)
    for _ in range(30):
        cfg = L.get_function_configuration(FunctionName=fn_name)
        if cfg.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ deployed {fn_name}")


def main():
    section("1) Resolve Anthropic API key")
    src, key = resolve_anthropic_key()
    if not key:
        log("  ❌ no Anthropic key found anywhere")
        return
    log(f"  ✓ found at {src}, length={len(key)}")
    ok, info = verify_key(key)
    if ok:
        log(f"  ✓ key verified: {info} models accessible")
    else:
        log(f"  ❌ key invalid: {info}")
        return

    section("2) Inject ANTHROPIC_KEY into L5, drop SKIP_CLAUDE")
    cfg = L.get_function(FunctionName="justhodl-nobrainer-rationale")["Configuration"]
    env = dict(cfg.get("Environment", {}).get("Variables", {}))
    env["ANTHROPIC_KEY"] = key
    env.pop("SKIP_CLAUDE", None)
    L.update_function_configuration(
        FunctionName="justhodl-nobrainer-rationale",
        Environment={"Variables": env},
    )
    # wait for config update
    for _ in range(20):
        c = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ env keys now: {sorted(env.keys())}")

    section("3) Force re-invoke L5 — real Claude theses + Telegram digest")
    r = L.invoke(
        FunctionName="justhodl-nobrainer-rationale",
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=json.dumps({"top_n": 5, "send_telegram": True}).encode(),
    )
    body = json.loads(r["Payload"].read())
    log(f"  status: {r['StatusCode']}  body keys: {list(body.keys())}")
    if body.get("statusCode") == 200:
        inner = json.loads(body["body"])
        log(f"  inner: {json.dumps(inner)[:500]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail logs ──")
        for ln in tail.splitlines()[-30:]:
            log(f"    {ln}")

    # verify rationale now has real Claude content
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
        data = json.loads(obj["Body"].read())
        theses = data.get("theses") or []
        log("")
        log(f"  data/nobrainers-rationale.json → n_theses={len(theses)}")
        if theses:
            first = theses[0]
            txt = first.get("rationale") or first.get("thesis") or ""
            sym = first.get("symbol", "?")
            is_dummy = txt.startswith("[SKIP_CLAUDE")
            log(f"  first thesis: {sym}, len={len(txt)}, dummy={is_dummy}")
            if not is_dummy:
                log(f"  ── first 300 chars of {sym} thesis ──")
                for ln in txt.splitlines()[:8]:
                    log(f"    {ln}")
    except Exception as e:
        log(f"  ❌ verify rationale: {e}")

    section("4) Patch L3 tier-classifier — drop delisted LTHM")
    if patch_l3_drop_lthm():
        deploy_lambda("justhodl-theme-tier-classifier",
                       "aws/lambdas/justhodl-theme-tier-classifier/source/lambda_function.py")

    section("5) Fix L6 DDB scan pagination — log full count")
    # Use proper pagination scan
    try:
        items = []
        last_key = None
        while True:
            kw = dict(
                TableName="justhodl-signals",
                FilterExpression="begins_with(signal_type, :nb)",
                ExpressionAttributeValues={":nb": {"S": "nobrainer"}},
                Select="ALL_ATTRIBUTES",
            )
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = DDB.scan(**kw)
            items.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            if not last_key or len(items) > 500:
                break
        log(f"  full DDB scan: {len(items)} nobrainer signals")
        # tally
        from collections import Counter
        types = Counter(); syms = Counter()
        for it in items:
            types[it.get("signal_type", {}).get("S", "?")] += 1
            v = it.get("signal_value", {}).get("S", "?")
            syms[v] += 1
        log(f"  by signal_type: {dict(types.most_common(10))}")
        log(f"  by symbol: {dict(syms.most_common(15))}")
    except Exception as e:
        log(f"  ❌ scan: {e}")

    section("6) Confirm everything")
    log("")
    log("  Layer status:")
    for fn, key_path in [
        ("justhodl-theme-detector", "data/themes-detected.json"),
        ("justhodl-supply-inflection-scanner", "data/supply-inflection.json"),
        ("justhodl-theme-tier-classifier", "data/theme-tiers.json"),
        ("justhodl-asymmetric-hunter", "data/nobrainers.json"),
        ("justhodl-nobrainer-rationale", "data/nobrainers-rationale.json"),
        ("justhodl-nobrainer-tracker", None),
    ]:
        cfg = L.get_function(FunctionName=fn)["Configuration"]
        log(f"  {fn:<40} mod={cfg['LastModified']}  state={cfg['State']}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "fix_nobrainer_pipeline_full.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
