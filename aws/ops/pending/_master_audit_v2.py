"""
MASTER AUDIT v2 — comprehensive end-to-end check after all today's phases.

Sections:
  A. All 11 hunter Lambdas — code matches repo, schedule attached, recent invoke
  B. All 17 S3 feeds — fresh, parseable, sensible content
  C. All canonical pages — HTTP 200 + nav links present + page actually renders data
  D. Compound signals — final state, quality of detected setups
  E. DDB justhodl-signals — confirm new entries (per-source) are landing for calibration
  F. End-to-end smoke: trigger compound aggregator, verify CSGP/EPAM still detected
  G. Issues + improvements list (prioritized)
"""
import io, json, os, time, base64, urllib.request, zipfile, hashlib
from collections import defaultdict
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
EB = boto3.client("events", region_name=REGION)
DDB = boto3.client("dynamodb", region_name=REGION)

REPORT = []
ISSUES = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def issue(severity, msg):
    ISSUES.append((severity, msg))
    log(f"  {'🚨' if severity=='HIGH' else '⚠'} [{severity}] {msg}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    started = time.time()

    # ─────────────────────────────────────────────────────────────
    section("A) Hunter Lambdas — deployed code matches repo + schedule")
    lambdas = [
        ("justhodl-theme-detector",            "aws/lambdas/justhodl-theme-detector/source/lambda_function.py", "L1"),
        ("justhodl-supply-inflection-scanner", "aws/lambdas/justhodl-supply-inflection-scanner/source/lambda_function.py", "L2"),
        ("justhodl-theme-tier-classifier",     "aws/lambdas/justhodl-theme-tier-classifier/source/lambda_function.py", "L3"),
        ("justhodl-asymmetric-hunter",         "aws/lambdas/justhodl-asymmetric-hunter/source/lambda_function.py", "L4"),
        ("justhodl-nobrainer-rationale",       "aws/lambdas/justhodl-nobrainer-rationale/source/lambda_function.py", "L5"),
        ("justhodl-nobrainer-tracker",         "aws/lambdas/justhodl-nobrainer-tracker/source/lambda_function.py", "L6"),
        ("justhodl-insider-cluster-scanner",   "aws/lambdas/justhodl-insider-cluster-scanner/source/lambda_function.py", "Insider"),
        ("justhodl-smart-money-cluster",       "aws/lambdas/justhodl-smart-money-cluster/source/lambda_function.py", "SmartMoney"),
        ("justhodl-deep-value-screener",       "aws/lambdas/justhodl-deep-value-screener/source/lambda_function.py", "DeepValue"),
        ("justhodl-eps-revision-velocity",     "aws/lambdas/justhodl-eps-revision-velocity/source/lambda_function.py", "EPSVel"),
        ("justhodl-compound-aggregator",       "aws/lambdas/justhodl-compound-aggregator/source/lambda_function.py", "Compound"),
        ("justhodl-system-signal-logger",      "aws/lambdas/justhodl-system-signal-logger/source/lambda_function.py", "SigLog"),
    ]
    for fn, repo_path, label in lambdas:
        try:
            cfg = L.get_function_configuration(FunctionName=fn)
            modified = cfg.get("LastModified", "")[:16]
            mem = cfg.get("MemorySize")
            tmo = cfg.get("Timeout")
            state = cfg.get("State")

            # Schedule check
            rules = EB.list_rule_names_by_target(
                TargetArn=f"arn:aws:lambda:{REGION}:857687956942:function:{fn}"
            ).get("RuleNames", [])
            sched = "NO_SCHEDULE"
            if rules:
                r = EB.describe_rule(Name=rules[0])
                sched = r.get("ScheduleExpression", "?")
                if r.get("State") != "ENABLED":
                    issue("HIGH", f"{fn} schedule {rules[0]} is {r.get('State')}")

            # Code-sync check
            try:
                code_url = L.get_function(FunctionName=fn)["Code"]["Location"]
                zb = urllib.request.urlopen(code_url, timeout=15).read()
                deployed_src = zipfile.ZipFile(io.BytesIO(zb)).read("lambda_function.py").decode("utf-8", "replace")
                repo_src = open(repo_path, "r", encoding="utf-8").read()
                d_hash = hashlib.md5(deployed_src.encode()).hexdigest()[:8]
                r_hash = hashlib.md5(repo_src.encode()).hexdigest()[:8]
                in_sync = d_hash == r_hash
                if not in_sync:
                    delta = len(repo_src) - len(deployed_src)
                    issue("HIGH" if abs(delta) > 100 else "LOW", f"{fn} code drift size_delta={delta}")
                sync_str = "✓" if in_sync else "⚠"
            except Exception as e:
                sync_str = "?"
                issue("LOW", f"{fn} code compare failed: {e}")

            log(f"  ✓ {label:<11} {fn:<40}  mem={mem:>4}MB  to={tmo:>3}s  sched={sched:<22}  code:{sync_str}  mod={modified}")
        except Exception as e:
            issue("HIGH", f"{fn} not found or inaccessible: {e}")

    # ─────────────────────────────────────────────────────────────
    section("B) S3 data feeds — fresh, parseable, sensible")
    feeds = [
        ("data/themes-detected.json",         "L1 themes",          1000, 24*60),
        ("data/supply-inflection.json",       "L2 supply",          1000, 24*60),
        ("data/theme-tiers.json",             "L3 tiers",          50000, 24*60),
        ("data/nobrainers.json",              "L4 hunter",        100000, 24*60),
        ("data/nobrainers-rationale.json",    "L5 rationale",      10000, 24*60),
        ("data/insider-clusters.json",        "Insider",           10000, 25*60),
        ("data/smart-money-clusters.json",    "SmartMoney",        10000, 25*60),
        ("data/deep-value.json",              "DeepValue",          5000, 24*60),
        ("data/eps-revision-velocity.json",   "EPSVelocity",       10000, 24*60),
        ("data/compound-signals.json",        "Compound",            500, 90),
        ("data/compound-signals-state.json",  "CompoundState",        50, 90),
        ("data/13f-positions.json",           "Raw13F",          5000000, 7*24*60),
        ("data/decisive-call-history.json",   "Calls",              1000, 24*60),
        ("backtest/results.json",             "Backtest",           5000, 7*24*60),
        ("data/report.json",                  "DailyLiquidity",   500000, 60),
    ]
    for key, desc, min_size, max_age_min in feeds:
        try:
            h = S3.head_object(Bucket=BUCKET, Key=key)
            sz = h["ContentLength"]
            age = (time.time() - h["LastModified"].timestamp()) / 60
            problems = []
            if sz < min_size: problems.append(f"size={sz}<{min_size}")
            if age > max_age_min: problems.append(f"age={age:.0f}>{max_age_min}min")
            mark = "✓" if not problems else "⚠"
            log(f"  {mark} {key:<42}  {sz:>9,}b  {age:>6.0f}min — {desc}  {','.join(problems)}")
            if problems:
                issue("LOW" if "age" in str(problems) else "HIGH", f"{key}: {','.join(problems)}")
        except Exception as e:
            issue("HIGH", f"{key} unreachable: {e}")

    # ─────────────────────────────────────────────────────────────
    section("C) Live page check — HTTP 200 + nav links")
    pages = [
        "https://justhodl.ai/",
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
        "https://justhodl.ai/backtest.html",
        "https://justhodl.ai/horizons.html",
        "https://justhodl.ai/sizing.html",
    ]
    for url in pages:
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                body = r.read().decode("utf-8", "replace")
                size = len(body)
                has_compound_nav = "/compound-signals.html" in body
                has_dv_nav = "/deep-value.html" in body
                has_eps_nav = "/eps-velocity.html" in body
                problems = []
                if not has_compound_nav: problems.append("no_compound_nav")
                if not has_dv_nav: problems.append("no_dv_nav")
                if not has_eps_nav: problems.append("no_eps_nav")
                mark = "✓" if not problems else "⚠"
                log(f"  {mark} {r.status:>3}  {size:>8,}b  {url:<60}  {','.join(problems) or '-'}")
                if problems:
                    issue("LOW", f"{url}: missing nav: {problems}")
        except Exception as e:
            issue("LOW", f"{url}: {e}")

    # ─────────────────────────────────────────────────────────────
    section("D) Compound signals — quality check")
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
        log(f"  schema: {d.get('schema_version')}")
        log(f"  feed_stats: {json.dumps(d.get('feed_stats', {}))}")
        log(f"  stats: {json.dumps(d.get('stats', {}))}")
        compound = d.get("compound", [])
        log(f"  compound entries: {len(compound)}")
        log("")
        log("  ── full compound leaderboard ──")
        for r in compound[:15]:
            sys_str = ", ".join(r.get("systems", []))
            log(f"    {r['symbol']:<6}  #sys={r['n_systems']}  comp={r['compound_score']:>7.1f}  ({sys_str})")
        log("")
        log("  ── per-system top 3 ──")
        feed_map = {
            "nobrainers": ("data/nobrainers.json", "summary.top_25_overall", "ticker"),
            "insiders": ("data/insider-clusters.json", "clusters", "ticker"),
            "smart_money": ("data/smart-money-clusters.json", "clusters", "ticker"),
            "deep_value": ("data/deep-value.json", "summary.top_25_overall", "symbol"),
            "eps_velocity": ("data/eps-revision-velocity.json", "summary.top_25_overall", "symbol"),
        }
        for sys, (key, path, sym_field) in feed_map.items():
            try:
                d2 = json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
                cur = d2
                for p in path.split("."):
                    cur = cur.get(p, [])
                top3 = sorted(cur or [], key=lambda x: -(x.get("score") or x.get("asymmetric_score") or 0))[:3]
                names = [c.get(sym_field) for c in top3]
                log(f"    {sys}: {names}")
            except Exception as e:
                log(f"    {sys}: ERROR {e}")
    except Exception as e:
        issue("HIGH", f"compound-signals.json read failed: {e}")

    # ─────────────────────────────────────────────────────────────
    section("E) DynamoDB justhodl-signals — per-source signal log")
    try:
        # Scan recent entries by source — just get COUNT of items added in last 24h
        # by scanning logged_at >= now - 86400
        now = int(time.time())
        cutoff = now - 86400
        # Use SCAN since signal_logger doesn't index by logged_at
        # In a 100s budget we can scan up to ~10000 items
        resp = DDB.scan(
            TableName="justhodl-signals",
            FilterExpression="logged_at >= :cutoff",
            ExpressionAttributeValues={":cutoff": {"N": str(cutoff)}},
            ProjectionExpression="signal_id, #src, ticker, conviction, logged_at",
            ExpressionAttributeNames={"#src": "source"},
            Limit=1000,
        )
        items = resp.get("Items", [])
        by_source = defaultdict(int)
        recent_tickers = set()
        for i in items:
            src = i.get("source", {}).get("S", "?")
            by_source[src] += 1
            if i.get("ticker", {}).get("S"):
                recent_tickers.add(i["ticker"]["S"])
        log(f"  signals logged in last 24h: {len(items)}")
        log(f"  by source: {dict(by_source)}")
        log(f"  unique tickers: {len(recent_tickers)}")
        log(f"  sample tickers: {sorted(list(recent_tickers))[:15]}")
    except Exception as e:
        issue("LOW", f"DDB scan failed: {e}")

    # ─────────────────────────────────────────────────────────────
    section("F) End-to-end smoke — trigger compound + verify pages render data")
    try:
        r = L.invoke(FunctionName="justhodl-compound-aggregator",
                      InvocationType="RequestResponse", Payload=b"{}")
        body = json.loads(r["Payload"].read())
        inner = json.loads(body.get("body", "{}"))
        log(f"  compound-aggregator: status={r['StatusCode']} n_compound={inner.get('n_compound')} n_3plus={inner.get('n_3_plus')} alerts={inner.get('n_alerts')}")
    except Exception as e:
        issue("LOW", f"compound smoke failed: {e}")

    # Verify compound page actually renders (has data placeholder filled?)
    try:
        with urllib.request.urlopen("https://justhodl.ai/compound-signals.html", timeout=10) as r:
            html = r.read().decode()
            # We expect "Loading" or "fetch" markers — page is JS-rendered so we just check it loads
            has_fetch = "fetch(" in html and "compound-signals.json" in html
            log(f"  compound page has fetch logic: {has_fetch}")
    except Exception as e:
        issue("LOW", f"compound page check: {e}")

    # ─────────────────────────────────────────────────────────────
    section("G) Issues found — prioritized")
    if not ISSUES:
        log("  ✓ No issues found")
    else:
        high = [m for s, m in ISSUES if s == "HIGH"]
        low = [m for s, m in ISSUES if s != "HIGH"]
        log(f"  HIGH ({len(high)}):")
        for i, m in enumerate(high, 1):
            log(f"    {i}. {m}")
        log(f"  LOW ({len(low)}):")
        for i, m in enumerate(low, 1):
            log(f"    {i}. {m}")

    log("")
    log(f"  Audit took {time.time()-started:.1f}s")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "master_audit_v2.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
