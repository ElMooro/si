"""
Finalize smart-money cluster integration:
  1. Send Telegram digest of top 8 smart-money clusters
  2. Wire 'Smart Money' link into all canonical pages
  3. Patch L5 nobrainer-rationale to ALSO load smart-money signals (compound)
  4. Verify everything live
"""
import io, json, os, time, urllib.request, urllib.error, zipfile, base64, re
from botocore.config import Config
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)
cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 1})
L = boto3.client("lambda", region_name=REGION, config=cfg)

REPORT = []
def log(m): 
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def md_escape(s):
    out = []
    for c in str(s):
        if c in "_*[]()~`>#+-=|{}.!\\":
            out.append("\\" + c)
        else:
            out.append(c)
    return "".join(out)


def send_telegram(text):
    token = SSM.get_parameter(Name="/justhodl/telegram/bot_token", WithDecryption=True)["Parameter"]["Value"]
    chat = SSM.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({
        "chat_id": chat,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }).encode()
    req = urllib.request.Request(url, data=data,
                                  headers={"Content-Type": "application/json"},
                                  method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            r = json.loads(resp.read())
            return True, r.get("result", {}).get("message_id")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        return False, f"HTTP {e.code}: {body[:300]}"
    except Exception as e:
        return False, str(e)


def main():
    section("1) Build + send smart-money Telegram digest")
    obj = S3.get_object(Bucket=BUCKET, Key="data/smart-money-clusters.json")
    data = json.loads(obj["Body"].read())
    clusters = data.get("clusters", [])
    stats = data.get("stats", {})

    lines = []
    lines.append("🦅 *SMART MONEY DIGEST*")
    lines.append(f"📅 {time.strftime('%Y\\-%m\\-%d %H:%M UTC')} \\| Q {md_escape(data.get('as_of_quarter','?'))}")
    lines.append("")
    lines.append(f"📊 {stats.get('n_total_13f_stocks',0)} stocks tracked \\| {stats.get('n_clusters_scored',0)} clusters \\| *{stats.get('n_strong',0)} strong*")
    lines.append(f"🎯 {stats.get('n_legend_fund_buys',0)} legend buys \\| {stats.get('n_deep_value',0)} deep\\-value \\| {stats.get('n_new_init_clusters',0)} new init clusters")
    lines.append("")
    lines.append("*Top setups* \\(score ≥ 65\\):")
    lines.append("")

    for c in clusters[:8]:
        if c.get("score", 0) < 65:
            continue
        ticker = c.get("ticker", "?")
        name = (c.get("name") or "")[:30]
        score = c.get("score", 0)
        legends = c.get("legend_buyers", [])
        n_buyers = c.get("n_buyers", 0)
        n_sellers = c.get("n_sellers", 0)
        n_new = c.get("n_new", 0)
        ph = c.get("pct_from_52w_high")

        # Choose emoji based on top signal
        sig_types = c.get("signal_types", [])
        emoji = "🟢"
        if "DEEP_VALUE_CONSENSUS" in sig_types:
            emoji = "💎"
        elif "NEW_INITIATION_CLUSTER" in sig_types:
            emoji = "🎯"
        elif "LEGEND_FUND_BUY" in sig_types:
            emoji = "🦅"
        elif "CONSENSUS_BUY" in sig_types:
            emoji = "📊"

        lines.append(f"{emoji} *{md_escape(ticker)}* — score *{md_escape(f'{score:.1f}')}*")
        lines.append(f"  {md_escape(name)}")
        if legends:
            lines.append(f"  🦅 Legends: {md_escape(', '.join(legends))}")
        lines.append(f"  {n_buyers} buyers / {n_sellers} sellers / {n_new} new init")
        if ph is not None:
            lines.append(f"  {md_escape(f'{ph:+.0f}%')} from 52W high")
        rat = (c.get("rationale") or "")[:130]
        lines.append(f"  → _{md_escape(rat)}_")
        lines.append("")

    lines.append("📊 [Smart Money](https://justhodl.ai/smart-money.html)")
    lines.append("👀 [Insider Clusters](https://justhodl.ai/insider-clusters.html)")
    lines.append("🎯 [Nobrainers](https://justhodl.ai/nobrainers.html) \\| 🌊 [Themes](https://justhodl.ai/themes.html)")
    lines.append("")
    lines.append("_13F smart\\-money cluster scanner \\| 17 elite funds tracked \\| daily 09:00 UTC_")

    text = "\n".join(lines)
    log(f"  message length: {len(text)} chars")
    log("  preview (first 700 chars):")
    for ln in text.splitlines()[:18]:
        log(f"    {ln}")

    ok, info = send_telegram(text)
    if ok:
        log(f"  ✅ delivered, message_id={info}")
    else:
        log(f"  ❌ {info}")

    section("2) Wire smart-money.html into canonical nav")
    pages = [
        "index.html", "desk.html", "brief.html", "calls.html", "performance.html",
        "sizing.html", "backtest.html", "weights.html", "horizons.html",
        "themes.html", "nobrainers.html", "insider-clusters.html", "insiders.html",
        "13f.html", "accuracy.html", "allocator.html", "sectors.html",
        "momentum.html", "news.html", "research.html", "vol.html",
        "ticker.html", "today.html", "feedback.html",
    ]
    patched = 0
    skipped = 0
    failed = 0
    for p in pages:
        if not os.path.exists(p):
            log(f"  ⚠ {p}: not found")
            failed += 1
            continue
        with open(p) as f:
            s = f.read()
        if "smart-money.html" in s:
            skipped += 1
            continue
        # Insert after insider-clusters.html, fall back to nobrainers.html
        for anchor_pattern in [
            r'(<a\s+[^>]*href="/insider-clusters\.html"[^>]*>[^<]+</a>)',
            r'(<a\s+[^>]*href="/nobrainers\.html"[^>]*>[^<]+</a>)',
        ]:
            m = re.search(anchor_pattern, s)
            if m:
                full = m.group(1)
                cls_match = re.search(r'class="([^"]+)"', full)
                cls = f' class="{cls_match.group(1)}"' if cls_match else ""
                # detect uppercase context
                if "INSIDER" in full or "NOBRAINER" in full or "CLUSTERS" in full.upper().replace("INSIDER-CLUSTERS",""):
                    label = "SMART MONEY"
                else:
                    label = "Smart Money"
                new_anchor = f'<a{cls} href="/smart-money.html">{label}</a>'
                # find indent from preceding newline
                idx = s.find(full)
                indent = ""
                if idx >= 0:
                    prev_nl = s.rfind("\n", 0, idx)
                    if prev_nl >= 0:
                        indent = s[prev_nl:idx]
                new = s.replace(full, full + indent + new_anchor, 1)
                if new != s:
                    with open(p, "w") as f:
                        f.write(new)
                    patched += 1
                    log(f"  ✓ {p}")
                    break
        else:
            log(f"  ❌ {p}: no anchor found")
            failed += 1
    log("")
    log(f"  patched: {patched}  skipped (already): {skipped}  failed: {failed}")

    section("3) Patch L5 nobrainer-rationale to load smart-money signals")
    rl_path = "aws/lambdas/justhodl-nobrainer-rationale/source/lambda_function.py"
    with open(rl_path) as f:
        src = f.read()
    if "smart-money-clusters.json" in src:
        log("  - already patched")
    else:
        # Add smart-money load right after insider load
        old = '''    # Load insider-cluster signals for compound-score augmentation
    insider_by_ticker = {}
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/insider-clusters.json")
        insider_data = json.loads(obj["Body"].read())
        for cl in insider_data.get("clusters", []):
            tk = cl.get("ticker")
            if tk:
                insider_by_ticker[tk] = cl
        print(f"[rationale] loaded {len(insider_by_ticker)} insider clusters")
    except Exception as e:
        print(f"[rationale] WARN — insider clusters unavailable: {e}")'''

        new = old + '''

    # Load smart-money 13F cluster signals for compound-score augmentation
    smart_money_by_ticker = {}
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/smart-money-clusters.json")
        sm_data = json.loads(obj["Body"].read())
        for cl in sm_data.get("clusters", []):
            tk = cl.get("ticker")
            if tk:
                smart_money_by_ticker[tk] = cl
        print(f"[rationale] loaded {len(smart_money_by_ticker)} smart-money clusters")
    except Exception as e:
        print(f"[rationale] WARN — smart-money clusters unavailable: {e}")'''

        if old in src:
            src = src.replace(old, new)
            log("  ✓ added smart-money load")
        else:
            log("  ⚠ insider load section not found; skipping")

        # Update build_thesis_prompt signature
        src = src.replace(
            "def build_thesis_prompt(candidate, insider_cluster=None):",
            "def build_thesis_prompt(candidate, insider_cluster=None, smart_money_cluster=None):",
            1
        )
        # Add smart-money helper next to _insider_block
        helper = '''
def _smart_money_block(cl):
    """Format smart-money cluster info for prompt injection."""
    if not cl:
        return "  (no recent smart-money 13F cluster activity for this ticker)"
    lines = []
    lines.append(f"  Smart-money score: {cl.get('score', 0):.1f}/100 ({cl.get('flag')})")
    sig_types = cl.get("signal_types", [])
    lines.append(f"  Signal types: {', '.join(sig_types)}")
    lines.append(f"  Rationale: {cl.get('rationale', '')}")
    lines.append(f"  {cl.get('n_buyers', 0)} funds buying / {cl.get('n_sellers', 0)} selling / {cl.get('n_new', 0)} new initiations")
    legends = cl.get("legend_buyers", [])
    if legends:
        lines.append(f"  ⭐ LEGEND FUND BUYERS: {', '.join(legends)}")
    quants = cl.get("quant_buyers", [])
    if quants:
        lines.append(f"  Quant fund buyers: {', '.join(quants)}")
    ph = cl.get("pct_from_52w_high")
    if ph is not None:
        lines.append(f"  Stock {abs(ph):.0f}% off 52W high (drawdown context)")
    fund_actions = cl.get("fund_actions", [])[:5]
    if fund_actions:
        lines.append("  Top fund actions:")
        for fa in fund_actions:
            f_name = (fa.get("fund") or "?")[:14]
            ch = fa.get("change", "?")
            v = (fa.get("value") or 0)/1e6
            pct = fa.get("pct_of_portfolio", 0) or 0
            d = fa.get("delta_pct")
            d_str = f"Δ{d:+.0f}%" if d is not None else (ch if ch=="NEW" else "")
            lines.append(f"    • {f_name:<14} {ch:<5} ${v:>6.1f}M  {pct:>4.1f}% port  {d_str}")
    return "\\n".join(lines)


'''
        # Insert before def build_thesis_prompt
        src = src.replace(
            "def build_thesis_prompt(candidate, insider_cluster=None, smart_money_cluster=None):",
            helper + "def build_thesis_prompt(candidate, insider_cluster=None, smart_money_cluster=None):",
            1
        )
        log("  ✓ added _smart_money_block helper")

        # Add SMART MONEY section in prompt
        old = "INSIDER CLUSTER SIGNAL (if any):\n{_insider_block(insider_cluster)}\n\nYOUR TASK:"
        new = "INSIDER CLUSTER SIGNAL (if any):\n{_insider_block(insider_cluster)}\n\n13F SMART-MONEY CLUSTER SIGNAL (if any):\n{_smart_money_block(smart_money_cluster)}\n\nYOUR TASK:"
        if old in src:
            src = src.replace(old, new)
            log("  ✓ added SMART-MONEY CLUSTER section to prompt")

        # Update call site
        old_call = '''        cl = insider_by_ticker.get(ticker)
        if cl:
            print(f"[rationale] {ticker} ALSO has insider cluster (score={cl.get('score')}, signal={cl.get('signal_type')})")
        prompt = build_thesis_prompt(c, cl)'''
        new_call = '''        cl = insider_by_ticker.get(ticker)
        sm = smart_money_by_ticker.get(ticker)
        if cl:
            print(f"[rationale] {ticker} ALSO has insider cluster (score={cl.get('score')}, signal={cl.get('signal_type')})")
        if sm:
            print(f"[rationale] {ticker} ALSO has smart-money cluster (score={sm.get('score')}, legends={sm.get('legend_buyers')})")
        prompt = build_thesis_prompt(c, cl, sm)'''
        if old_call in src:
            src = src.replace(old_call, new_call)
            log("  ✓ updated call site to pass smart_money_cluster")

        with open(rl_path, "w") as f:
            f.write(src)
        log(f"  ✓ wrote patched L5: {len(src):,} chars")

    section("4) Verify smart-money page is live + summary")
    for url in [
        "https://justhodl.ai/smart-money.html",
        "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/smart-money-clusters.json",
    ]:
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                log(f"  {r.status}  {r.headers.get('Content-Length','?'):>8}b  {url}")
        except Exception as e:
            log(f"  ❌ {url}: {e}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "finalize_smart_money.md"), "w") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
