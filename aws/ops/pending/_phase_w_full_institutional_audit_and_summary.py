"""
PHASE W — Full audit of the institutional-grade system + roadmap of remaining
improvements I'd recommend.

This is the final summary that:
  1. Verifies every Lambda is healthy
  2. Counts the full data pipeline (now 8 systems total)
  3. Reports today's full chain of finds
  4. Lists the 5 remaining high-leverage improvements I'd build next
"""
import json, time, os
import boto3
from collections import defaultdict

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
EB = boto3.client("events", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def main():
    section("1) Full system inventory")
    lambdas = [
        # Hunter chain (L1-L6)
        ("justhodl-theme-detector",          "L1: themes detector"),
        ("justhodl-supply-inflection-scanner", "L2: supply inflection"),
        ("justhodl-theme-tier-classifier",   "L3: theme tier classifier"),
        ("justhodl-asymmetric-hunter",       "L4: asymmetric hunter"),
        ("justhodl-nobrainer-rationale",     "L5: thesis writer (Claude)"),
        ("justhodl-nobrainer-tracker",       "L6: position tracker"),
        # 5 hunter scanners
        ("justhodl-insider-cluster-scanner", "Hunter: SEC Form 4 insider clusters"),
        ("justhodl-smart-money-cluster",     "Hunter: 13F smart money clusters"),
        ("justhodl-deep-value-screener",     "Hunter: Ben Graham deep value"),
        ("justhodl-eps-revision-velocity",   "Hunter: EPS revision velocity"),
        # 2 technical hunters
        ("justhodl-momentum-breakout",       "Hunter: momentum breakout"),
        ("justhodl-pre-pump-detector",       "Hunter: pre-pump (calibrated v2)"),
        # 3 infrastructure
        ("justhodl-compound-aggregator",     "Compound: 7-feed cross-fusion"),
        ("justhodl-universe-builder",        "Universe: shared 563-stock pool"),
        ("justhodl-system-signal-logger",    "Calibration: DDB signal logger"),
        # NEW: theme rotation
        ("justhodl-theme-rotation-engine",   "NEW: institutional money-flow tracker"),
    ]

    active = 0
    for fn, desc in lambdas:
        try:
            cfg = L.get_function_configuration(FunctionName=fn)
            if cfg.get("State") == "Active":
                active += 1
                rules = EB.list_rule_names_by_target(
                    TargetArn="arn:aws:lambda:" + REGION + ":857687956942:function:" + fn
                ).get("RuleNames", [])
                sched = "?"
                for rn in rules:
                    r = EB.describe_rule(Name=rn)
                    if r.get("State") == "ENABLED":
                        sched = r.get("ScheduleExpression")
                        break
                log("  ✓ {:<42} {:<24} {}".format(fn, sched, desc))
            else:
                log("  ❌ " + fn + " state=" + str(cfg.get("State")))
        except Exception as e:
            log("  ❌ " + fn + ": " + str(e))

    log("")
    log("  Total active: " + str(active) + "/" + str(len(lambdas)))

    section("2) S3 data feeds")
    feeds = [
        "data/themes-detected.json",
        "data/supply-inflection.json",
        "data/theme-tiers.json",
        "data/nobrainers.json",
        "data/nobrainers-rationale.json",
        "data/insider-clusters.json",
        "data/smart-money-clusters.json",
        "data/deep-value.json",
        "data/eps-revision-velocity.json",
        "data/momentum-breakout.json",
        "data/pre-pump-signals.json",
        "data/compound-signals.json",
        "data/universe.json",
        "data/theme-rotation.json",
        "data/institutional-convergence.json",
    ]
    fresh = 0
    for k in feeds:
        try:
            h = S3.head_object(Bucket=BUCKET, Key=k)
            sz = h["ContentLength"]
            age = (time.time() - h["LastModified"].timestamp()) / 60
            fresh_flag = "✓" if age < 1440 else "⚠"
            if age < 1440:
                fresh += 1
            log("  {} {:<42}  {:>10,}b  {:>5.0f}min".format(fresh_flag, k, sz, age))
        except Exception as e:
            log("  ❌ " + k + ": " + str(e))
    log("")
    log("  Fresh feeds: " + str(fresh) + "/" + str(len(feeds)))

    section("3) Today's institutional-grade chain of finds")

    # Theme rotation
    try:
        tr = json.loads(S3.get_object(Bucket=BUCKET, Key="data/theme-rotation.json")["Body"].read())
        log("  ── Top 5 rotating IN themes (where institutional money is flowing) ──")
        for t in tr.get("summary", {}).get("top_10_momentum", [])[:5]:
            breadth = t.get("breadth_pct")
            breadth_str = " breadth=" + ("{:.0f}%".format(breadth) if breadth is not None else "N/A")
            log("    {:<6} {:<24}  RS_20d={:+.1f}%  RS_60d={:+.1f}%  {}".format(
                t["ticker"], t["name"][:24], t["rs_20d"], t["rs_60d"], breadth_str))
        log("")
        log("  ── Top 5 rotating OUT themes ──")
        for t in tr.get("summary", {}).get("bottom_10_momentum", [])[-5:]:
            log("    {:<6} {:<24}  RS_20d={:+.1f}%  RS_60d={:+.1f}%".format(
                t["ticker"], t["name"][:24], t["rs_20d"], t["rs_60d"]))
    except Exception as e:
        log("  ❌ theme-rotation read: " + str(e))

    # Compound
    try:
        cs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
        log("")
        log("  ── 7-feed compound (5 TIER-3 names) ──")
        for r in cs.get("compound", [])[:8]:
            sys_str = ",".join(r["systems"])
            log("    {:<6} #{} comp={:>5.0f}  ({})".format(
                r["symbol"], r["n_systems"], r["compound_score"], sys_str))
    except Exception as e:
        log("  ❌ " + str(e))

    # Institutional convergence
    try:
        ic = json.loads(S3.get_object(Bucket=BUCKET, Key="data/institutional-convergence.json")["Body"].read())
        log("")
        log("  ── INSTITUTIONAL CONVERGENCE (theme rotating IN + name on compound) ──")
        for p in ic.get("convergence", []):
            log("    {:<6} theme={:<6}  theme_momentum={}  compound={:.0f}".format(
                p["symbol"], p["theme_etf"], int(p["theme_momentum"]), p["compound_score"]))
    except Exception as e:
        log("  ❌ " + str(e))

    section("4) Today's Telegram delivery chain")
    log("  Through this session we delivered:")
    log("   • msg 692 — initial summary")
    log("   • msg 695 — compound v2 (15 multi-signal, 5 TIER-3)")
    log("   • msg 696 — final breakthrough digest")
    log("   • msg 711 — institutional money flow + convergence")

    section("5) ROADMAP — 5 high-leverage improvements I'd build next")
    log("  Current system has 16 Lambdas covering 8 distinct signal domains.")
    log("  The 5 highest-ROI improvements I'd build next, in priority order:")
    log("")

    log("  PRIORITY 1: SHORT INTEREST + OPTIONS FLOW")
    log("  -----------------------------------------")
    log("  Most institutional desks watch unusual options flow daily — it leads")
    log("  equity moves by 1-3 weeks. Susquehanna & Citadel built their entire")
    log("  edge here. We need:")
    log("    • Short-interest velocity (FINRA daily reg-sho data)")
    log("    • Put/call skew + IV percentile (Polygon options)")
    log("    • Aggressive call-buying detection (sweep volume)")
    log("    • Options dark-pool prints")
    log("  Lambda: justhodl-options-flow-scanner")
    log("  Wires into compound as 9th signal. Expected catch rate: institutional")
    log("  call-buying typically precedes equity breakouts by 5-15 trading days.")
    log("")

    log("  PRIORITY 2: SECTOR EARNINGS DIFFUSION")
    log("  -------------------------------------")
    log("  Beyond individual stock EPS revisions, the BREADTH of upgrades within")
    log("  a sector is a leading institutional signal. When 65% of semis have")
    log("  rising estimates, sell-side desks call 'sector all-in'. We need:")
    log("    • Per-sector % of stocks with rising FY1 estimates last 30d")
    log("    • Per-sector breadth of revenue growth acceleration")
    log("    • Cross-sector earnings diffusion ranking")
    log("  Lambda: justhodl-sector-earnings-diffusion")
    log("  This catches sector-level inflections months before stock screens.")
    log("")

    log("  PRIORITY 3: NARRATIVE / NEWS DENSITY")
    log("  ------------------------------------")
    log("  Bloomberg counts 'AI infrastructure' mentions — when they 3x in a")
    log("  month, the theme is forming. We have NewsAPI key already. Build:")
    log("    • Theme-keyword density tracker (AI, GLP-1, lithium, etc.)")
    log("    • Mention velocity (rate of change)")
    log("    • Cross-reference with our 79 detected themes")
    log("  Lambda: justhodl-narrative-density-tracker")
    log("  Free, low-latency, captures retail-driven moves before institutional")
    log("  signals fire.")
    log("")

    log("  PRIORITY 4: 13D / 5%+ ACTIVIST FILINGS")
    log("  --------------------------------------")
    log("  Activist filings often precede major moves. SEC EDGAR has all 13D/G")
    log("  filings. We need:")
    log("    • Daily SEC EDGAR scrape for 13D, 13G, SC 13D/A filings")
    log("    • Cross-reference filer with known activists (Icahn, Loeb, Ackman)")
    log("    • Trigger alert when activist takes 5%+ stake")
    log("  Lambda: justhodl-activist-filing-scanner")
    log("  Highest single-event signal type. Free SEC data.")
    log("")

    log("  PRIORITY 5: CROSS-ASSET MACRO REGIME DETECTOR")
    log("  ---------------------------------------------")
    log("  When equities, bonds, gold, dollar all move together it's a regime")
    log("  signal. We have FRED + Polygon. Build:")
    log("    • Daily correlation matrix across 8 asset classes")
    log("    • Regime-shift detector (when correlations break >2 sigma)")
    log("    • Risk-on / risk-off rotation flag")
    log("  Lambda: justhodl-cross-asset-regime")
    log("  Tells you WHEN to be long single names vs hedged. Affects every")
    log("  position size in the system.")
    log("")

    log("  These 5 would push the system from 8 → 13 signal domains and bring")
    log("  it to genuine institutional-desk parity.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
        for ln in traceback.format_exc().splitlines():
            log("    " + ln)
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_w_full_audit_roadmap.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
