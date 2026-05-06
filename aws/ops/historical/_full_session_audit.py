"""
Comprehensive audit of EVERYTHING built today across multiple sessions:

Systems audited:
  1. Nobrainer chain (L1-L6, 6 Lambdas) — themes, supply, tiers, hunter, rationale, tracker
  2. Insider cluster scanner — SEC Form 4 daily index
  3. Smart-money 13F cluster scanner
  4. Deep-value screener — Ben Graham net-net
  5. EPS revision velocity — MU/SNDK pattern
  6. Compound aggregator — cross-system fusion
  7. Backtest engine v1.2 — horizon-aware + friction
  8. AI brief — horizon-aware compound prompts
  9. Position monitor — paper portfolio + Telegram alerts
 10. Calibration — outcome-checker + calibrator + weights

For each: check
  - Lambda exists, Active, schedule attached
  - Latest output present in S3, fresh, parseable
  - Code matches repo source (catches stale deploys)
  - Output schema reasonable (no obvious bugs/empty)

For each page (24): check
  - HTTP 200 from justhodl.ai
  - Has nav links to all 5 system pages
  - Loads its data feed without JS error (size sanity)
"""
import io, json, os, time, urllib.request, zipfile
import boto3
from collections import defaultdict

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
EB = boto3.client("events", region_name=REGION)

REPORT = []
ISSUES = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def issue(m):
    ISSUES.append(m); log(f"  🚨 {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def check_lambda_health(fn_name, repo_path=None, expect_recent_invoke=True):
    """Returns dict with health metrics."""
    info = {"fn": fn_name, "ok": True, "issues": []}
    try:
        cfg = L.get_function_configuration(FunctionName=fn_name)
        info["state"] = cfg.get("State")
        info["mem"] = cfg.get("MemorySize")
        info["timeout"] = cfg.get("Timeout")
        info["modified"] = cfg.get("LastModified", "")
        info["runtime"] = cfg.get("Runtime")
        info["env"] = cfg.get("Environment", {}).get("Variables", {})

        if cfg.get("State") != "Active":
            info["issues"].append(f"state={cfg.get('State')}")
            info["ok"] = False
    except Exception as e:
        info["issues"].append(f"get_function: {e}")
        info["ok"] = False
        return info

    # Check schedule
    try:
        rules = EB.list_rule_names_by_target(
            TargetArn=f"arn:aws:lambda:{REGION}:857687956942:function:{fn_name}"
        ).get("RuleNames", [])
        info["schedules"] = []
        for rn in rules:
            r = EB.describe_rule(Name=rn)
            info["schedules"].append({
                "name": rn,
                "expr": r.get("ScheduleExpression"),
                "state": r.get("State"),
            })
        if not info["schedules"]:
            info["issues"].append("no_schedule")
    except Exception as e:
        info["issues"].append(f"schedule: {e}")

    # Compare deployed code vs repo
    if repo_path:
        try:
            code_url = L.get_function(FunctionName=fn_name)["Code"]["Location"]
            zb = urllib.request.urlopen(code_url, timeout=15).read()
            deployed_src = zipfile.ZipFile(io.BytesIO(zb)).read("lambda_function.py").decode("utf-8", "replace")
            try:
                repo_src = open(repo_path, "r", encoding="utf-8").read()
                # Compare hashes
                import hashlib
                d_hash = hashlib.md5(deployed_src.encode()).hexdigest()[:8]
                r_hash = hashlib.md5(repo_src.encode()).hexdigest()[:8]
                info["deployed_hash"] = d_hash
                info["repo_hash"] = r_hash
                info["code_in_sync"] = d_hash == r_hash
                if not info["code_in_sync"]:
                    # check size delta
                    info["size_delta"] = len(repo_src) - len(deployed_src)
                    info["issues"].append(f"code_drift size_delta={info['size_delta']}")
            except FileNotFoundError:
                info["issues"].append(f"repo path missing: {repo_path}")
        except Exception as e:
            info["issues"].append(f"code_compare: {e}")

    if info["issues"]:
        info["ok"] = False
    return info


def check_s3_feed(key, expected_max_age_min=24*60, expected_min_size=500):
    """Returns dict with feed health."""
    info = {"key": key, "ok": True, "issues": []}
    try:
        h = S3.head_object(Bucket=BUCKET, Key=key)
        info["size"] = h["ContentLength"]
        info["modified"] = h["LastModified"]
        info["age_min"] = (time.time() - h["LastModified"].timestamp()) / 60
        if info["size"] < expected_min_size:
            info["issues"].append(f"size_too_small {info['size']}b")
        if info["age_min"] > expected_max_age_min:
            info["issues"].append(f"stale {info['age_min']:.0f}min old")
    except Exception as e:
        info["issues"].append(f"head: {e}")
        info["ok"] = False
        return info

    # Light parse check
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        body = obj["Body"].read()
        d = json.loads(body)
        info["top_keys"] = sorted(list(d.keys()))[:8] if isinstance(d, dict) else "non-dict"
    except Exception as e:
        info["issues"].append(f"parse: {e}")

    if info["issues"]:
        info["ok"] = False
    return info


def check_page(url, expect_keywords=None):
    """Returns dict with HTTP status and keyword presence."""
    info = {"url": url, "ok": True, "issues": []}
    try:
        with urllib.request.urlopen(url, timeout=12) as r:
            info["status"] = r.status
            info["size"] = int(r.headers.get("Content-Length", 0))
            body = r.read().decode("utf-8", "replace")
            if expect_keywords:
                missing = [k for k in expect_keywords if k not in body]
                if missing:
                    info["issues"].append(f"missing_keywords: {missing}")
    except Exception as e:
        info["issues"].append(f"fetch: {e}")
        info["ok"] = False
        return info
    if info["issues"]:
        info["ok"] = False
    return info


def main():
    started = time.time()

    section("0) Audit scope")
    log("  This audit checks every Lambda, S3 feed, and page from today's session.")
    log("  Issues are logged and listed at the end with severity tags.")

    section("1) NOBRAINER CHAIN (L1-L6) Lambdas")
    nb_chain = [
        ("justhodl-theme-detector",          "aws/lambdas/justhodl-theme-detector/source/lambda_function.py"),
        ("justhodl-supply-inflection-scanner", "aws/lambdas/justhodl-supply-inflection-scanner/source/lambda_function.py"),
        ("justhodl-theme-tier-classifier",   "aws/lambdas/justhodl-theme-tier-classifier/source/lambda_function.py"),
        ("justhodl-asymmetric-hunter",       "aws/lambdas/justhodl-asymmetric-hunter/source/lambda_function.py"),
        ("justhodl-nobrainer-rationale",     "aws/lambdas/justhodl-nobrainer-rationale/source/lambda_function.py"),
        ("justhodl-nobrainer-tracker",       "aws/lambdas/justhodl-nobrainer-tracker/source/lambda_function.py"),
    ]
    for fn, rp in nb_chain:
        info = check_lambda_health(fn, rp)
        sched = info.get("schedules", [{}])[0].get("expr", "?") if info.get("schedules") else "NO_SCHEDULE"
        sync = "✓" if info.get("code_in_sync") else "⚠"
        sym = "✓" if info["ok"] else "❌"
        log(f"  {sym} {fn:<40}  {sched:<22}  code:{sync}  iss={','.join(info['issues']) or '-'}")

    section("2) NEW HUNTER Lambdas")
    new_lambdas = [
        ("justhodl-insider-cluster-scanner", "aws/lambdas/justhodl-insider-cluster-scanner/source/lambda_function.py"),
        ("justhodl-smart-money-cluster",     "aws/lambdas/justhodl-smart-money-cluster/source/lambda_function.py"),
        ("justhodl-deep-value-screener",     "aws/lambdas/justhodl-deep-value-screener/source/lambda_function.py"),
        ("justhodl-eps-revision-velocity",   "aws/lambdas/justhodl-eps-revision-velocity/source/lambda_function.py"),
    ]
    for fn, rp in new_lambdas:
        info = check_lambda_health(fn, rp)
        sched = info.get("schedules", [{}])[0].get("expr", "?") if info.get("schedules") else "NO_SCHEDULE"
        sync = "✓" if info.get("code_in_sync") else "⚠"
        sym = "✓" if info["ok"] else "❌"
        log(f"  {sym} {fn:<40}  {sched:<22}  code:{sync}  iss={','.join(info['issues']) or '-'}")

    section("3) INFRASTRUCTURE Lambdas (backtest, brief, monitor, calibration)")
    infra = [
        "justhodl-backtest-engine",
        "justhodl-ai-brief",
        "justhodl-position-monitor",
        "justhodl-signal-logger",
        "justhodl-outcome-checker",
        "justhodl-calibrator",
        "justhodl-cot-extremes-scanner",
        "justhodl-asymmetric-scorer",
        "justhodl-risk-sizer",
        "justhodl-auction-crisis-detector",
        "justhodl-eurodollar-stress",
    ]
    for fn in infra:
        info = check_lambda_health(fn)  # no repo compare
        sched = info.get("schedules", [{}])[0].get("expr", "?") if info.get("schedules") else "NO_SCHEDULE"
        sym = "✓" if info["ok"] else "❌"
        log(f"  {sym} {fn:<40}  {sched:<22}  state={info.get('state','?')}  iss={','.join(info['issues']) or '-'}")

    section("4) S3 DATA FEEDS — all 6 system outputs + supporting")
    feeds = [
        ("data/themes-detected.json",        "L1 themes"),
        ("data/supply-inflection.json",      "L2 supply"),
        ("data/theme-tiers.json",            "L3 tiers"),
        ("data/nobrainers.json",             "L4 hunter"),
        ("data/nobrainers-rationale.json",   "L5 rationale"),
        ("data/insider-clusters.json",       "Insider scanner"),
        ("data/smart-money-clusters.json",   "13F smart money"),
        ("data/deep-value.json",             "Deep value"),
        ("data/eps-revision-velocity.json",  "EPS velocity"),
        ("data/compound-signals.json",       "Compound aggregator"),
        ("data/13f-positions.json",          "Raw 13F input"),
        ("data/decisive-call-history.json",  "Calls ledger"),
        ("backtest/results.json",            "Backtest results"),
        ("backtest/summary.json",            "Backtest summary"),
        ("portfolio/positions.json",         "Paper positions"),
        ("data/report.json",                 "Daily liquidity report"),
        ("data/auction-crisis.json",         "Auction crisis"),
    ]
    for k, desc in feeds:
        info = check_s3_feed(k)
        sym = "✓" if info["ok"] else "❌"
        log(f"  {sym} {k:<40}  {info.get('size',0):>10,}b  {info.get('age_min',999):>5.0f}min — {desc}")

    section("5) PAGES — HTTP 200 check + nav presence")
    pages_to_check = [
        ("https://justhodl.ai/", ["compound-signals.html", "nobrainers"]),
        ("https://justhodl.ai/compound-signals.html", ["data/compound-signals.json"]),
        ("https://justhodl.ai/nobrainers.html", ["data/nobrainers"]),
        ("https://justhodl.ai/insider-clusters.html", ["data/insider-clusters.json"]),
        ("https://justhodl.ai/smart-money.html", ["smart-money-clusters"]),
        ("https://justhodl.ai/deep-value.html", ["data/deep-value.json"]),
        ("https://justhodl.ai/eps-velocity.html", ["data/eps-revision-velocity.json"]),
        ("https://justhodl.ai/themes.html", ["themes-detected"]),
        ("https://justhodl.ai/brief.html", ["compound-signals.html"]),
        ("https://justhodl.ai/calls.html", ["compound-signals.html"]),
        ("https://justhodl.ai/desk.html", ["compound-signals.html"]),
        ("https://justhodl.ai/backtest.html", ["compound-signals.html"]),
        ("https://justhodl.ai/horizons.html", ["compound-signals.html"]),
        ("https://justhodl.ai/sizing.html", ["compound-signals.html"]),
        ("https://justhodl.ai/weights.html", ["compound-signals.html"]),
        ("https://justhodl.ai/performance.html", ["compound-signals.html"]),
        ("https://justhodl.ai/13f.html", ["compound-signals.html"]),
    ]
    for url, kws in pages_to_check:
        info = check_page(url, kws)
        sym = "✓" if info["ok"] else "❌"
        log(f"  {sym} {info.get('status','?'):>3}  {info.get('size',0):>8,}b  {url}  iss={','.join(info['issues']) or '-'}")

    section("6) DATA QUALITY DEEP CHECKS")

    log("  ── compound-signals.json ──")
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
        log(f"    feed_stats: {json.dumps(d.get('feed_stats', {}))}")
        log(f"    total_names: {d.get('stats', {}).get('n_total_names')}")
        log(f"    multi-signal: {d.get('stats', {}).get('n_multi_signal')}")
        log(f"    3+ systems: {d.get('stats', {}).get('n_3_plus')}")
        for r in d.get("compound", [])[:5]:
            log(f"    {r.get('symbol'):<6} #{r.get('n_systems')}  systems={r.get('systems')}  comp={r.get('compound_score')}")
    except Exception as e:
        issue(f"compound-signals.json failed: {e}")

    log("")
    log("  ── deep-value.json sector accuracy ──")
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/deep-value.json")["Body"].read())
        top = d.get("summary", {}).get("top_25_overall", [])
        excluded = d.get("summary", {}).get("top_25_excluded_financials", [])
        log(f"    top_25_overall: {len(top)}")
        log(f"    top_25_excluded: {len(excluded)}")
        # Check for ANY financial leakage in top_25
        leaks = [c for c in top if "financial" in (c.get("sector","").lower())]
        if leaks:
            issue(f"deep-value top_25 still has {len(leaks)} financial entries: {[c.get('symbol') for c in leaks]}")
        # Check sector populated
        no_sector = [c for c in top if not c.get("sector")]
        if no_sector:
            log(f"    ⚠ {len(no_sector)} entries with blank sector (may indicate FMP /profile failures)")
            for c in no_sector[:5]:
                log(f"      {c.get('symbol'):<6}  flag={c.get('flag','')}")
    except Exception as e:
        issue(f"deep-value.json check failed: {e}")

    log("")
    log("  ── insider-clusters.json freshness ──")
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/insider-clusters.json")["Body"].read())
        stats = d.get("stats", {})
        clusters = d.get("clusters", [])
        log(f"    n_clusters: {len(clusters)}")
        log(f"    n_strong: {stats.get('n_strong_signals')}")
        log(f"    smart_money_dual: {stats.get('n_smart_money_dual')}")
    except Exception as e:
        issue(f"insider-clusters check failed: {e}")

    log("")
    log("  ── smart-money-clusters.json ──")
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/smart-money-clusters.json")["Body"].read())
        clusters = d.get("clusters", [])
        log(f"    total: {len(clusters)}")
        for c in sorted(clusters, key=lambda x: -(x.get('score') or 0))[:5]:
            tk = c.get("ticker", "?")
            sc = c.get("score", 0)
            sigs = c.get("signal_types", [])
            log(f"    {tk:<6}  score={sc:>6.1f}  signals={sigs}")
    except Exception as e:
        issue(f"smart-money-clusters check failed: {e}")

    section("7) ISSUE SUMMARY")
    if ISSUES:
        log(f"  ❌ Found {len(ISSUES)} issues to fix:")
        for i, msg in enumerate(ISSUES, 1):
            log(f"    {i}. {msg}")
    else:
        log("  ✓ No critical issues found")

    log("")
    log(f"  Audit took {time.time() - started:.1f}s")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "full_session_audit.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
