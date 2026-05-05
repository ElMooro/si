"""
Wire `Themes` and `Nobrainers` into the canonical sidebar nav across all pages
that already have the tab-style horizons.html link.

Also:
- Patch L6 nobrainer-tracker source: silently skip known-delisted tickers (no n_errors increment).
- Verify Telegram bot token exists for L5 digest.
- Issue: write final report.
"""
import os, json, time, base64, re, glob
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Wire Themes + Nobrainers into sidebar nav")

    # Pages that already have the tab-style nav
    target_pages = [
        "13f.html", "accuracy.html", "allocator.html", "backtest.html",
        "brief.html", "calls.html", "feedback.html", "momentum.html",
        "news.html", "performance.html", "research.html", "sectors.html",
        "sizing.html", "ticker.html", "today.html", "vol.html", "weights.html"
    ]

    new_nav_block = (
        '    <a class="tab" href="/themes.html">Themes</a>\n'
        '    <a class="tab" href="/nobrainers.html">Nobrainers</a>\n'
    )

    # Insert just AFTER /horizons.html line (so they appear in the strategic intelligence cluster)
    insertion_anchor = '    <a class="tab" href="/horizons.html">Horizons</a>\n'

    n_patched = 0
    n_already = 0
    n_missing_anchor = 0
    for page in target_pages:
        if not os.path.exists(page):
            log(f"  ⚠ {page} missing")
            continue
        with open(page, "r", encoding="utf-8") as f:
            content = f.read()

        if 'href="/themes.html"' in content and 'href="/nobrainers.html"' in content:
            n_already += 1
            continue

        if insertion_anchor not in content:
            n_missing_anchor += 1
            log(f"  ⚠ {page}: anchor not found, skipping")
            continue

        # Insert after horizons.html
        new_content = content.replace(insertion_anchor, insertion_anchor + new_nav_block, 1)
        if new_content != content:
            with open(page, "w", encoding="utf-8") as f:
                f.write(new_content)
            n_patched += 1
            log(f"  ✓ {page} patched")

    log("")
    log(f"  patched: {n_patched}  already-wired: {n_already}  missing anchor: {n_missing_anchor}")

    # Also wire into themes.html and nobrainers.html themselves so they're consistent
    section("2) Wire themes.html / nobrainers.html with reciprocal nav")
    for page in ["themes.html", "nobrainers.html"]:
        if not os.path.exists(page): continue
        with open(page, "r", encoding="utf-8") as f:
            content = f.read()
        # Skip if already has full nav structure (they're standalone pages)
        log(f"  {page}: present, length={len(content)} chars")

    section("3) Patch L6 nobrainer-tracker — skip known-delisted tickers silently")
    L6_SRC = "aws/lambdas/justhodl-nobrainer-tracker/source/lambda_function.py"
    if os.path.exists(L6_SRC):
        with open(L6_SRC, "r", encoding="utf-8") as f:
            src = f.read()

        # Build a known-delisted list patch
        # Strategy: at top of file add DELISTED_TICKERS frozenset, then in tracker
        # logic skip them BEFORE attempting baseline price.
        DELISTED = "DELISTED_TICKERS = frozenset({'LTHM', 'ALB-OLD', 'BBBY', 'WBD-OLD'})  # known delisted/merged"
        if "DELISTED_TICKERS" not in src:
            # Insert after imports / constants near top
            anchor = "import boto3"
            if anchor in src:
                # Insert after the imports section — find first blank line after imports
                pos = src.find("\n\n", src.find(anchor))
                if pos > 0:
                    src = src[:pos] + "\n\n" + DELISTED + src[pos:]
                    log(f"  ✓ added DELISTED_TICKERS constant")

        # Now find the "baseline price unavailable, skipping" log line and wrap with delisted check
        # Existing pattern (from log): `[track] LTHM — baseline price unavailable, skipping`
        # We'll find the place where ticker fails baseline lookup and add delisted-aware skip
        baseline_unavail_re = re.compile(
            r"(print\(f?\"?\[track\] \{[a-zA-Z_]+\} — baseline price unavailable, skipping\"?\)?)",
            re.MULTILINE
        )
        m = baseline_unavail_re.search(src)
        if m:
            old_line = m.group(1)
            # Find the broader context — look for the function or block where this print lives
            # We need to handle: if ticker in DELISTED → silent skip (no error increment), else log error
            # The simplest patch is to add a guard EARLIER in the per-ticker iteration
            # Strategy: find `for r in ranked` style loop and add early continue for delisted

            # Look for the loop body
            loop_re = re.compile(r"for [a-z_]+ in ranked", re.MULTILINE)
            log(f"  baseline-unavailable line found: {old_line[:80]}...")

        # Look for existing dedup logic to add the delisted check right BEFORE
        # baseline price fetch
        if "DELISTED_TICKERS" in src:
            # Add early-skip pattern: insert ticker check
            # Most common pattern: ticker = c.get("ticker") or c.get("symbol")
            # then guard: if ticker in DELISTED_TICKERS: continue
            ticker_extract = re.compile(
                r"(\s*)(ticker\s*=\s*[a-zA-Z_]+\.get\([\"']ticker[\"']\)\s*or\s*[a-zA-Z_]+\.get\([\"']symbol[\"']\))",
                re.MULTILINE
            )
            m = ticker_extract.search(src)
            if m:
                indent = m.group(1)
                old = m.group(0)
                guard = (
                    f"\n{indent}if ticker in DELISTED_TICKERS: continue  # silent skip"
                )
                if "if ticker in DELISTED_TICKERS" not in src:
                    src = src.replace(old, old + guard, 1)
                    log(f"  ✓ added early-skip guard for delisted tickers")
                else:
                    log(f"  delisted guard already present")

        with open(L6_SRC, "w", encoding="utf-8") as f:
            f.write(src)
        log(f"  patched {L6_SRC}")

    section("4) Verify Telegram bot token in L5 + SSM")
    try:
        cfg = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
        env = cfg.get("Environment", {}).get("Variables", {})
        log(f"  L5 has TELEGRAM_BOT_TOKEN: {'TELEGRAM_BOT_TOKEN' in env}")
    except Exception as e:
        log(f"  ❌ {e}")
    try:
        SSM.get_parameter(Name="/justhodl/telegram/bot_token", WithDecryption=True)
        log(f"  SSM /justhodl/telegram/bot_token exists ✓")
    except Exception as e:
        log(f"  ⚠ SSM check: {e}")

    section("5) Confirm L5's latest output has real Claude theses")
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
        data = json.loads(obj["Body"].read())
        theses = data.get("theses", [])
        n_dummy = sum(1 for t in theses if "[SKIP_CLAUDE" in str(t.get("thesis") or t.get("rationale") or ""))
        log(f"  generated_at: {data.get('generated_at')}")
        log(f"  n_theses: {len(theses)}  dummy: {n_dummy}  real: {len(theses)-n_dummy}")
        if theses:
            for t in theses[:3]:
                sym = t.get("ticker") or t.get("symbol") or "?"
                theme = t.get("theme_etf") or t.get("theme") or "?"
                score = t.get("asymmetric_score") or "?"
                txt = t.get("thesis") or t.get("rationale") or ""
                log(f"  • {sym} ({theme}) score={score}  thesis chars={len(txt)}  dummy={('[SKIP_CLAUDE' in txt)}")
    except Exception as e:
        log(f"  ❌ {e}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"FATAL: {e}")
    out = "aws/ops/reports/latest/wire_themes_nobrainers_nav.md"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out,"w",encoding="utf-8") as f: f.write("\n".join(REPORT))
    print(f"[written]")
