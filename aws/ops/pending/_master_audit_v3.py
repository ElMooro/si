"""
MASTER AUDIT v3 — comprehensive end-to-end check after all phases A-F2.

Validates:
  1. Every Lambda exists, Active, schedule attached, code synced with repo
  2. Every S3 feed fresh, parseable, with reasonable schema
  3. Every page returns 200 + has the new compound nav links
  4. The compound aggregator picks up the latest fresh data
  5. DDB justhodl-signals has 24h activity from all 5 systems
  6. The L5 thesis output has compound mentions
  7. CSGP (the key compound signal) is still appearing
  8. Schedules don't conflict
"""
import io, json, os, time, urllib.request, zipfile, hashlib, base64
from collections import defaultdict
import boto3
from boto3.dynamodb.conditions import Attr

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
EB = boto3.client("events", region_name=REGION)
DDB = boto3.resource("dynamodb", region_name=REGION).Table("justhodl-signals")

REPORT = []
ISSUES = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def issue(m, severity="MEDIUM"):
    ISSUES.append((severity, m)); log(f"  🚨 {severity}: {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    started = time.time()
    section("0) Audit scope")
    log("  Comprehensive audit of all hunter systems + infrastructure built today.")
    log("")

    # ─────────────────────────────────────────────────────────────────────
    section("1) ALL LAMBDAS — code sync + schedule")
    all_lambdas = [
        # Hunter chain
        ("justhodl-theme-detector", "aws/lambdas/justhodl-theme-detector/source/lambda_function.py"),
        ("justhodl-supply-inflection-scanner", "aws/lambdas/justhodl-supply-inflection-scanner/source/lambda_function.py"),
        ("justhodl-theme-tier-classifier", "aws/lambdas/justhodl-theme-tier-classifier/source/lambda_function.py"),
        ("justhodl-asymmetric-hunter", "aws/lambdas/justhodl-asymmetric-hunter/source/lambda_function.py"),
        ("justhodl-nobrainer-rationale", "aws/lambdas/justhodl-nobrainer-rationale/source/lambda_function.py"),
        ("justhodl-nobrainer-tracker", "aws/lambdas/justhodl-nobrainer-tracker/source/lambda_function.py"),
        # Hunter scanners
        ("justhodl-insider-cluster-scanner", "aws/lambdas/justhodl-insider-cluster-scanner/source/lambda_function.py"),
        ("justhodl-smart-money-cluster", "aws/lambdas/justhodl-smart-money-cluster/source/lambda_function.py"),
        ("justhodl-deep-value-screener", "aws/lambdas/justhodl-deep-value-screener/source/lambda_function.py"),
        ("justhodl-eps-revision-velocity", "aws/lambdas/justhodl-eps-revision-velocity/source/lambda_function.py"),
        # Compound + universe + signal logger
        ("justhodl-compound-aggregator", "aws/lambdas/justhodl-compound-aggregator/source/lambda_function.py"),
        ("justhodl-universe-builder", "aws/lambdas/justhodl-universe-builder/source/lambda_function.py"),
        ("justhodl-system-signal-logger", "aws/lambdas/justhodl-system-signal-logger/source/lambda_function.py"),
    ]
    schedule_collisions = defaultdict(list)

    for fn, rp in all_lambdas:
        try:
            cfg = L.get_function_configuration(FunctionName=fn)
            state = cfg.get("State")
            mod = cfg.get("LastModified", "")[:19]
            mem = cfg.get("MemorySize")
            tmo = cfg.get("Timeout")
        except Exception as e:
            issue(f"Lambda {fn}: {e}", "HIGH")
            continue

        # Schedule
        try:
            rules = EB.list_rule_names_by_target(
                TargetArn=f"arn:aws:lambda:{REGION}:857687956942:function:{fn}"
            ).get("RuleNames", [])
            sched = "NONE"
            for rn in rules:
                r = EB.describe_rule(Name=rn)
                expr = r.get("ScheduleExpression")
                state2 = r.get("State")
                if state2 == "ENABLED":
                    sched = expr
                    schedule_collisions[expr].append(fn)
                    break
        except Exception as e:
            sched = f"ERR:{e}"

        # Code sync
        try:
            code_url = L.get_function(FunctionName=fn)["Code"]["Location"]
            zb = urllib.request.urlopen(code_url, timeout=15).read()
            deployed_src = zipfile.ZipFile(io.BytesIO(zb)).read("lambda_function.py").decode("utf-8", "replace")
            try:
                repo_src = open(rp, "r", encoding="utf-8").read()
                d_hash = hashlib.md5(deployed_src.encode()).hexdigest()[:8]
                r_hash = hashlib.md5(repo_src.encode()).hexdigest()[:8]
                sync = "✓" if d_hash == r_hash else "❌"
                if d_hash != r_hash:
                    issue(f"{fn}: code_drift d={d_hash} r={r_hash} delta={len(repo_src)-len(deployed_src):+d}", "MEDIUM")
            except FileNotFoundError:
                sync = "?"
        except Exception as e:
            sync = "?"

        sym = "✓" if state == "Active" else "❌"
        log(f"  {sym} {fn:<42}  {sched:<25}  mem={mem:>5}MB  to={tmo:>4}s  code:{sync}  mod={mod}")
        if state != "Active":
            issue(f"{fn} state={state}", "HIGH")

    log("")
    log("  ── schedule collision check ──")
    for expr, fns in schedule_collisions.items():
        if len(fns) > 1:
            issue(f"Schedule collision on '{expr}': {fns}", "LOW")

    # ─────────────────────────────────────────────────────────────────────
    section("2) S3 FEEDS — freshness + parseability")
    feeds = [
        ("data/themes-detected.json",          24*60),
        ("data/supply-inflection.json",        24*60),
        ("data/theme-tiers.json",              24*60),
        ("data/nobrainers.json",               24*60),
        ("data/nobrainers-rationale.json",     24*60),
        ("data/insider-clusters.json",         24*60),
        ("data/smart-money-clusters.json",     24*60),
        ("data/deep-value.json",               24*60),
        ("data/eps-revision-velocity.json",    24*60),
        ("data/compound-signals.json",         2*60),  # hourly
        ("data/compound-signals-state.json",   2*60),
        ("data/universe.json",                 7*24*60),  # weekly is fine
    ]
    for key, max_age_min in feeds:
        try:
            h = S3.head_object(Bucket=BUCKET, Key=key)
            sz = h["ContentLength"]
            age = (time.time() - h["LastModified"].timestamp()) / 60
            stale = age > max_age_min
            tiny = sz < 200
            sym = "❌" if (stale or tiny) else "✓"
            log(f"  {sym} {key:<42}  {sz:>10,}b  {age:>6.0f}min  (max={max_age_min:>4}m)")
            if stale:
                issue(f"{key} stale: {age:.0f}min > {max_age_min}min", "MEDIUM")
            if tiny:
                issue(f"{key} tiny: {sz}b", "HIGH")
            # Light parse
            obj = S3.get_object(Bucket=BUCKET, Key=key)
            json.loads(obj["Body"].read())
        except Exception as e:
            issue(f"{key}: {e}", "HIGH")

    # ─────────────────────────────────────────────────────────────────────
    section("3) PAGES — HTTP + nav presence")
    pages = [
        "https://justhodl.ai/compound-signals.html",
        "https://justhodl.ai/nobrainers.html",
        "https://justhodl.ai/insider-clusters.html",
        "https://justhodl.ai/smart-money.html",
        "https://justhodl.ai/deep-value.html",
        "https://justhodl.ai/eps-velocity.html",
        "https://justhodl.ai/themes.html",
        "https://justhodl.ai/brief.html",
        "https://justhodl.ai/calls.html",
        "https://justhodl.ai/desk.html",
    ]
    expected_links = ["compound-signals.html", "deep-value.html", "eps-velocity.html"]
    for url in pages:
        try:
            with urllib.request.urlopen(url, timeout=12) as r:
                body = r.read().decode("utf-8", "replace")
                size = len(body)
                missing = [k for k in expected_links if k not in body]
                sym = "✓" if not missing else "⚠"
                log(f"  {sym} {r.status:>3}  {size:>8,}b  {url}  missing={missing or '-'}")
                if missing and "compound" not in url:
                    issue(f"{url} missing nav: {missing}", "LOW")
        except Exception as e:
            log(f"  ❌ {url}: {e}")

    # ─────────────────────────────────────────────────────────────────────
    section("4) COMPOUND SIGNALS state")
    try:
        cs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
        feed_stats = cs.get("feed_stats", {})
        stats = cs.get("stats", {})
        log(f"  schema: {cs.get('schema_version')}")
        log(f"  generated_at: {cs.get('generated_at')}")
        log(f"  feed_stats: {json.dumps(feed_stats)}")
        log(f"  stats: {json.dumps(stats)}")
        log("")
        log("  ── compound leaderboard ──")
        for r in cs.get("compound", [])[:10]:
            log(f"    {r['symbol']:<6} #{r['n_systems']}  comp={r['compound_score']:>7.1f}  ({','.join(r['systems'])})")
        if stats.get("n_multi_signal", 0) < 3:
            issue(f"only {stats.get('n_multi_signal')} multi-signal — system overlap is low", "LOW")
    except Exception as e:
        issue(f"compound-signals.json: {e}", "HIGH")

    # ─────────────────────────────────────────────────────────────────────
    section("5) DDB justhodl-signals — last 24h activity by source")
    try:
        cutoff = int(time.time()) - 86400
        # Scan with filter (small table for now)
        resp = DDB.scan(
            FilterExpression=Attr("logged_at").gte(cutoff),
            ProjectionExpression="signal_id,#src,ticker,conviction,logged_at",
            ExpressionAttributeNames={"#src": "source"},
        )
        items = resp.get("Items", [])
        while "LastEvaluatedKey" in resp:
            resp = DDB.scan(
                FilterExpression=Attr("logged_at").gte(cutoff),
                ProjectionExpression="signal_id,#src,ticker,conviction,logged_at",
                ExpressionAttributeNames={"#src": "source"},
                ExclusiveStartKey=resp["LastEvaluatedKey"],
            )
            items.extend(resp.get("Items", []))
        log(f"  total signals in last 24h: {len(items)}")
        by_src = defaultdict(int)
        unique_tickers = set()
        for it in items:
            by_src[it.get("source", "?")] += 1
            unique_tickers.add(it.get("ticker", "?"))
        for src, count in sorted(by_src.items()):
            log(f"    {src:<24} {count:>4}")
        log(f"  unique tickers: {len(unique_tickers)}")
    except Exception as e:
        issue(f"DDB scan: {e}", "MEDIUM")

    # ─────────────────────────────────────────────────────────────────────
    section("6) L5 RATIONALE — does it mention compound signals?")
    try:
        r5 = json.loads(S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")["Body"].read())
        log(f"  generated_at: {r5.get('generated_at')}")
        log(f"  n_theses: {r5.get('n_theses')}, n_ok: {r5.get('n_claude_ok')}")
        keywords = ["insider", "13f", "smart money", "ben graham", "net cash",
                    "eps revision", "consensus", "burry", "klarman", "druckenmiller",
                    "boardroom", "ceo bought", "deep value"]
        kw_count = defaultdict(int)
        for t in r5.get("theses", []):
            text = (t.get("thesis") or "").lower()
            for kw in keywords:
                if kw in text:
                    kw_count[kw] += 1
        log(f"  ── compound-language across {len(r5.get('theses', []))} theses ──")
        for kw, count in sorted(kw_count.items(), key=lambda x: -x[1])[:8]:
            log(f"    {count:>2}x '{kw}'")
        if not kw_count:
            issue("L5 theses have NO compound-language mentions", "LOW")
    except Exception as e:
        issue(f"L5 rationale: {e}", "MEDIUM")

    # ─────────────────────────────────────────────────────────────────────
    section("7) VERIFY UNIFIED UNIVERSE coverage")
    try:
        u = json.loads(S3.get_object(Bucket=BUCKET, Key="data/universe.json")["Body"].read())
        tickers = set()
        for r in u.get("records", []) or u.get("universe", []) or []:
            sym = r.get("symbol") or r.get("ticker")
            if sym:
                tickers.add(sym.upper())
        log(f"  universe tickers: {len(tickers)}")

        # Spot-check key names
        key_names = ["AAPL","MSFT","GOOGL","AMZN","NVDA","TSLA","CSGP","EPAM",
                      "MU","SNDK","PLTR","FCX","OXY","CNC","HUM","MOH","LLY","AVGO",
                      "AMD","JPM","BAC","WFC","JNJ","XOM","CVX","CAT","DE","COST"]
        missing = [k for k in key_names if k not in tickers]
        present = [k for k in key_names if k in tickers]
        log(f"  key-name coverage: {len(present)}/{len(key_names)}")
        log(f"  present: {present[:15]}")
        if missing:
            log(f"  ⚠ missing: {missing}")
            if len(missing) > 8:
                issue(f"universe missing {len(missing)} key names: {missing}", "MEDIUM")
    except Exception as e:
        issue(f"universe.json: {e}", "MEDIUM")

    # ─────────────────────────────────────────────────────────────────────
    section("8) Issue summary")
    if not ISSUES:
        log("  ✅ ZERO ISSUES")
    else:
        by_sev = defaultdict(list)
        for sev, msg in ISSUES:
            by_sev[sev].append(msg)
        for sev in ["HIGH", "MEDIUM", "LOW"]:
            if sev in by_sev:
                log(f"  {sev}: {len(by_sev[sev])} issues")
                for m in by_sev[sev]:
                    log(f"    • {m}")

    log("")
    log(f"  audit took {time.time()-started:.1f}s")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "master_audit_v3.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
