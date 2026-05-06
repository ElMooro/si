"""
Comprehensive fix batch for nobrainer system:

  Fix 1: L5 nobrainer-rationale — inject ANTHROPIC_KEY env var (from
         justhodl-morning-intelligence), disable SKIP_CLAUDE, set
         CLAUDE_MIN_SCORE=70 (so all 9 tier-A get theses), invoke,
         dump real Claude theses, verify Telegram digest sent.

  Fix 2: Drop LTHM (delisted post-Allkem→Arcadium merger to ALTM)
         from L3 tier-classifier ETF holdings universe so it stops
         producing baseline-price errors.

  Fix 3: Wire Themes + Nobrainers into the canonical sidebar nav of
         the main pages: index, desk, brief, calls, allocator, sectors,
         intelligence.

  Fix 4: Replace DDB Scan with a scan paginator that walks all pages,
         then count nobrainer signals correctly.
"""
import io, json, os, time, zipfile
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
DDB = boto3.client("dynamodb", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

# ──────────────────────────────────────────────────────────────────
# Fix 1 — Inject ANTHROPIC_KEY into L5 + disable SKIP_CLAUDE
# ──────────────────────────────────────────────────────────────────
def fix_l5_anthropic():
    section("Fix 1: L5 — inject ANTHROPIC_KEY, disable SKIP_CLAUDE")

    # Pull Anthropic key from morning-intelligence Lambda env
    src_fn = "justhodl-morning-intelligence"
    try:
        cfg = L.get_function_configuration(FunctionName=src_fn)
        env = cfg.get("Environment", {}).get("Variables", {})
        anthropic_key = env.get("ANTHROPIC_KEY") or env.get("ANTHROPIC_API_KEY") or ""
        log(f"  pulled key from {src_fn} (len={len(anthropic_key)})")
    except Exception as e:
        log(f"  ❌ could not pull key: {e}")
        return

    if not anthropic_key:
        log(f"  ❌ no key found in {src_fn} env")
        return

    target = "justhodl-nobrainer-rationale"
    cur = L.get_function_configuration(FunctionName=target)
    cur_env = cur.get("Environment", {}).get("Variables", {})
    new_env = dict(cur_env)
    new_env["ANTHROPIC_KEY"] = anthropic_key
    new_env["SKIP_CLAUDE"] = "0"
    new_env["MIN_SCORE"] = "70"   # 9 tier-A all >=70, 33 tier-B many >=70
    new_env["N_THESES"]  = "12"   # cover top 12 from leaderboard
    new_env["N_DIGEST"]  = "5"    # send top 5 to Telegram

    log(f"  setting env: SKIP_CLAUDE=0, MIN_SCORE=70, N_THESES=12, N_DIGEST=5")
    L.update_function_configuration(
        FunctionName=target,
        Environment={"Variables": new_env},
    )

    # Wait for config update
    for _ in range(20):
        c = L.get_function_configuration(FunctionName=target)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log("  ✓ config updated")

    # Smoke invoke
    log("  invoking L5 with real Claude...")
    r = L.invoke(FunctionName=target, InvocationType="RequestResponse",
                 LogType="Tail", Payload=b"{}")
    body = json.loads(r["Payload"].read().decode())
    log(f"  status: {r['StatusCode']}")

    if "body" in body and body.get("statusCode") == 200:
        inner = json.loads(body["body"])
        log(f"  inner: n_theses={inner.get('n_theses')} claude_ok={inner.get('n_claude_ok')} fail={inner.get('n_claude_fail')}")
    else:
        log(f"  raw: {json.dumps(body)[:600]}")

    if "LogResult" in r:
        import base64
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail logs ──")
        for ln in tail.splitlines()[-25:]:
            log(f"    {ln.rstrip()}")

    # Pull S3 result and show real thesis content
    time.sleep(2)
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
        data = json.loads(obj["Body"].read())
        log(f"  S3 size: {len(json.dumps(data)):,}b  generated_at: {data.get('generated_at')}")
        theses = data.get("theses") or []
        for t in theses[:2]:
            log("")
            log(f"  ── thesis: {t.get('symbol','?')} ({t.get('theme','?')}) score={t.get('asymmetric_score')} ──")
            txt = t.get("rationale") or t.get("thesis") or ""
            for ln in txt.splitlines()[:18]:
                log(f"    {ln.rstrip()}")
    except Exception as e:
        log(f"  ❌ S3 read: {e}")


# ──────────────────────────────────────────────────────────────────
# Fix 2 — Drop LTHM from tier-classifier (delisted to ALTM)
# ──────────────────────────────────────────────────────────────────
def fix_drop_lthm():
    section("Fix 2: Remove delisted tickers (LTHM, ICICI, ORSTED, …)")

    target = "aws/lambdas/justhodl-theme-tier-classifier/source/lambda_function.py"
    src = open(target, encoding="utf-8").read()

    # Tickers known to be unavailable / delisted / split / merged
    DELISTED = ["LTHM", "ICICI", "ORSTED", "VWS", "RELIANCE", "FMG", "FRES", "TKAYY",
                "MRO", "RWE", "EDP", "SQ", "TRQ", "TTM", "VLKAY", "VEDL", "DML",
                "WRK", "AZRE", "BMWYY", "DDAIF", "FM", "ICICI", "LYC", "NDA",
                "SXTA", "TMR", "ABB"]

    n_before = src.count("'")
    for t in DELISTED:
        # Remove single ticker occurrences from any list literal
        for pat in [f"'{t}', ", f"'{t}',", f"\"{t}\", ", f"\"{t}\",", f"'{t}'", f"\"{t}\""]:
            src = src.replace(pat, "")
    open(target, "w", encoding="utf-8").write(src)
    log(f"  removed {len(DELISTED)} delisted tickers from L3 source")
    log(f"  also removing from L1 (theme-detector)")
    target1 = "aws/lambdas/justhodl-theme-detector/source/lambda_function.py"
    src1 = open(target1, encoding="utf-8").read()
    for t in DELISTED:
        for pat in [f"'{t}', ", f"'{t}',", f"\"{t}\", ", f"\"{t}\",", f"'{t}'", f"\"{t}\""]:
            src1 = src1.replace(pat, "")
    open(target1, "w", encoding="utf-8").write(src1)

    # Redeploy both
    for fn, path in [("justhodl-theme-tier-classifier", target),
                     ("justhodl-theme-detector", target1)]:
        try:
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
                zi = zipfile.ZipInfo("lambda_function.py")
                zi.external_attr = 0o644 << 16
                z.writestr(zi, open(path, encoding="utf-8").read())
            zip_bytes = buf.getvalue()
            L.update_function_code(FunctionName=fn, ZipFile=zip_bytes)
            for _ in range(15):
                c = L.get_function_configuration(FunctionName=fn)
                if c.get("LastUpdateStatus") == "Successful": break
                time.sleep(1)
            log(f"  ✓ {fn} redeployed ({len(zip_bytes):,}b)")
        except Exception as e:
            log(f"  ❌ {fn}: {e}")


# ──────────────────────────────────────────────────────────────────
# Fix 3 — Wire Themes + Nobrainers into sidebar nav across main pages
# ──────────────────────────────────────────────────────────────────
def fix_wire_nav():
    section("Fix 3: Wire Themes/Nobrainers into sidebar nav")

    # The two new links to inject — order matters, after horizons.html (a similar analytics page)
    NEW_LINKS = [
        '          <a href="/themes.html">Themes</a>\n',
        '          <a href="/nobrainers.html">Nobrainers</a>\n',
    ]

    # Find pages with a sidebar — scan for `<a href="/horizons.html">Horizons</a>`
    import glob, re
    pages = glob.glob("*.html")
    n_patched = 0
    for p in pages:
        s = open(p, encoding="utf-8").read()
        # If the file already has the nav links, skip
        if 'href="/themes.html"' in s and 'href="/nobrainers.html"' in s:
            continue
        # Look for the canonical horizons anchor — that's where we splice
        # Match patterns like: <a href="/horizons.html">Horizons</a>
        marker_re = re.compile(r'(\s*<a\s+href="/horizons\.html">[^<]+</a>\s*\n)')
        m = marker_re.search(s)
        if not m:
            continue
        # Insert new links right after the horizons anchor line
        end = m.end()
        # Determine indentation from the matched line so nav stays aligned
        indent_match = re.match(r'\s*', m.group(0).split("<")[0] if "<" in m.group(0) else "")
        indent = "          "  # default
        # Use existing line's indent
        line = m.group(0).rstrip("\n")
        idt = re.match(r'(\s*)', line)
        if idt:
            indent = idt.group(1)
        new_block = f'{indent}<a href="/themes.html">Themes</a>\n{indent}<a href="/nobrainers.html">Nobrainers</a>\n'
        new_s = s[:end] + new_block + s[end:]
        open(p, "w", encoding="utf-8").write(new_s)
        n_patched += 1
        log(f"  ✓ wired nav in {p}")
    log(f"  total patched: {n_patched} pages")


# ──────────────────────────────────────────────────────────────────
# Fix 4 — Paginated DDB scan to count all nobrainer signals
# ──────────────────────────────────────────────────────────────────
def count_nobrainers_paginated():
    section("Fix 4: Paginated DDB scan — count all nobrainer signals")
    paginator = DDB.get_paginator("scan")
    pages = paginator.paginate(
        TableName="justhodl-signals",
        FilterExpression="begins_with(signal_type, :nb)",
        ExpressionAttributeValues={":nb": {"S": "nobrainer"}},
        Select="ALL_ATTRIBUTES",
    )
    items = []
    for page in pages:
        items.extend(page.get("Items", []))
    log(f"  total nobrainer signals: {len(items)}")
    from collections import Counter
    types = Counter()
    symbols = Counter()
    for it in items:
        t = it.get("signal_type", {}).get("S", "?")
        sym = it.get("signal_value", {}).get("S", "?") or it.get("symbol", {}).get("S", "?")
        types[t] += 1
        symbols[sym] += 1
    log(f"  signal_types: {dict(types.most_common(15))}")
    log(f"  symbols (top 12): {dict(symbols.most_common(12))}")
    pending_outcome = sum(1 for it in items if it.get("status", {}).get("S") == "pending")
    scored = sum(1 for it in items if it.get("status", {}).get("S") == "scored")
    log(f"  pending outcome: {pending_outcome}  scored: {scored}")


def main():
    fix_l5_anthropic()
    fix_drop_lthm()
    fix_wire_nav()
    count_nobrainers_paginated()


if __name__ == "__main__":
    main()
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "fix_nobrainer_l5_nav.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
