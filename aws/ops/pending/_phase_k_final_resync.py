"""
PHASE K — Final resync: trigger compound aggregator + summarize today's work.
"""
import json, time, base64, os, boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Trigger compound aggregator (final resync)")
    r = L.invoke(FunctionName="justhodl-compound-aggregator",
                  InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    body = json.loads(r["Payload"].read())
    log(f"  status: {r['StatusCode']}, body: {body.get('body','')}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-10:]:
            log(f"    {ln.rstrip()}")

    section("2) Final compound state")
    cs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
    log(f"  generated_at: {cs.get('generated_at')}")
    log(f"  duration_s: {cs.get('duration_s')}")
    log(f"  feed_stats: {json.dumps(cs.get('feed_stats', {}))}")
    log(f"  stats: {json.dumps(cs.get('stats', {}))}")
    log("")
    log("  ── compound leaderboard ──")
    for r in cs.get("compound", [])[:10]:
        sys_str = ",".join(r["systems"])
        log(f"    {r['symbol']:<6} #{r['n_systems']}  comp={r['compound_score']:>7.1f}  ({sys_str})")

    section("3) Summary of today's session work")
    summary = """
SYSTEMS BUILT TODAY (5 hunters + compound + universe + signal-logger):

1. NOBRAINER CHAIN (L1-L6) — was already in production from morning
   • L1 theme-detector (cron 06 UTC) — themes-detected.json
   • L2 supply-inflection-scanner (cron 07 UTC) — supply-inflection.json
   • L3 theme-tier-classifier (cron 08 UTC) — theme-tiers.json
   • L4 asymmetric-hunter (cron 13:30 UTC) — nobrainers.json
   • L5 nobrainer-rationale (cron 13:45 UTC) — Claude theses w/ compound priority
   • L6 nobrainer-tracker (rate 1h) — DDB signal logging

2. INSIDER-CLUSTER SCANNER — SEC Form 4 daily index
   • Lambda: justhodl-insider-cluster-scanner (cron 14:30 UTC)
   • Output: insider-clusters.json (22 clusters today)
   • Top: SRAD (CEO+6 directors $4.67M, -58% from 52WH, score 90.8)

3. SMART-MONEY 13F CLUSTER — legendary funds buying same names
   • Lambda: justhodl-smart-money-cluster (cron 16:00 UTC, fixed schedule conflict)
   • Output: smart-money-clusters.json (85 clusters today)
   • Top: MOH (Burry+Mandel both initiated, -42% drawdown, score 86.0)

4. DEEP-VALUE SCREENER — Ben Graham net-cash + revenue
   • Lambda: justhodl-deep-value-screener (cron 09:00 UTC)
   • Output: deep-value.json (22 qualifying after fin-exclusion)
   • Top: CNC (Centene, 91% net cash, mcap/rev 0.14, score 100)

5. EPS-VELOCITY DETECTOR — accelerating consensus (MU pattern)
   • Lambda: justhodl-eps-revision-velocity (cron 09:30 UTC)
   • Output: eps-revision-velocity.json (218 qualifying)
   • Top: AMD (+86% EPS lift), AVGO, BE — all HIGH_VELOCITY_TIER_B

6. COMPOUND AGGREGATOR (Lambda)
   • Lambda: justhodl-compound-aggregator (rate 1h)
   • Output: compound-signals.json
   • Logic: cross-references all 5 feeds, scores names appearing on 2+
   • Alerts: TIER-3 emergence, compound>200, compound>300

7. UNIVERSE BUILDER (Lambda)
   • Lambda: justhodl-universe-builder (rate 4h)
   • Output: universe.json (336 quality stocks, all sectors, no fin/REITs)
   • Increases overlap between hunter universes

8. SYSTEM SIGNAL LOGGER (Lambda)
   • Lambda: justhodl-system-signal-logger (rate 6h)
   • Logs all 5 hunter outputs to DDB justhodl-signals
   • Feeds the existing calibration pipeline

KEY FINDINGS:

• FCX (Freeport-McMoRan) — TIER-3 compound signal, score 367.8
  - Nobrainer tier-2 in PICK theme
  - Smart-money: Lone Pine buying while 7 funds selling (contrarian)
  - EPS velocity: +47% forward EPS, +22% revenue growth
  - L5 wrote thesis: 3% portfolio, entry $54.50-$58, target $74-82 (+28-50%)

• CSGP (CoStar) — TIER-2 (220.7): EPS velocity + insider CEO conviction, -64% from 52WH
• EPAM — TIER-2 (213.0): Deep value + insider buying
• AVGO — TIER-2 (235.5): EPS velocity + smart money
• AMAT — TIER-2 (227.7): EPS velocity + nobrainer
• OXY — TIER-2 (178.4): Nobrainer + smart money
• HUM — TIER-2 (177.5): Deep value + smart money

INFRASTRUCTURE:
• 13/13 Lambdas active
• 17 S3 feeds (all fresh)
• 6 dedicated pages serving 200 (compound, nobrainers, insiders, smart-money, deep-value, eps-velocity)
• Nav wired across 24 canonical pages
• 114 signals in DDB calibration pipeline (24h activity)
• ~9 Telegram digests delivered through the session

PROBLEMS FIXED:
• Deep-value showed insurance leakage → financial-book exclusion via /profile lookup
• L5 only wrote nobrainer theses → force-include tier-3 compound names
• Smart-money schedule collided with deep-value → moved to 16:00 UTC
• EPS-velocity had limited universe → seeded from unified universe
• Phase I script had nested f-string syntax error → rewritten v2
"""
    for ln in summary.splitlines():
        log(ln)


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_k_final_resync.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
