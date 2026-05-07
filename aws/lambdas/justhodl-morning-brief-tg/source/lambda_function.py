"""
justhodl-morning-brief-tg — Daily Telegram digest for Khalid at 7AM ET (12 UTC).

Pulls:
  - data/morning-intel.json (regime, top signals, blended composite)
  - data/macro-surprise.json (CESI proxy, regime, growth z, inflation z)
  - data/yield-curve.json (regime, 2s10s spread)
  - data/historical-analogs.json (call, 21d hit rate, mean return)
  - data/event-study.json (active themes)
  - data/ab-test-results.json (winner, leaderboard)
  - portfolio/signal-portfolio-state.json (open positions, NAV, win rate)
  - data/correlation-surface.json (macro_regime, regime breaks)
  - data/whats-changed.json (today's biggest deltas — if available)

Formats compact markdown digest, sends to chat_id from SSM.
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")

s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def get_chat_id():
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    except Exception:
        return os.environ.get("CHAT_ID", "")


def fetch_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"fetch_json({key}) fail: {e}")
        return None


def md_escape(s):
    if s is None:
        return "—"
    s = str(s)
    return s.replace("_", "\\_").replace("*", "\\*").replace("[", "\\[").replace("`", "\\`")


def fmt_pct(v, decimals=2, sign=False):
    if v is None:
        return "—"
    try:
        f = float(v)
        if sign:
            return f"{f:+.{decimals}f}%"
        return f"{f:.{decimals}f}%"
    except Exception:
        return "—"


def fmt_num(v, decimals=2, sign=False):
    if v is None:
        return "—"
    try:
        f = float(v)
        if sign:
            return f"{f:+.{decimals}f}"
        return f"{f:.{decimals}f}"
    except Exception:
        return "—"


def build_brief():
    now = datetime.now(timezone.utc)
    morning = fetch_json("data/morning-intel.json") or {}
    macro = fetch_json("data/macro-surprise.json") or {}
    yc = fetch_json("data/yield-curve.json") or {}
    analogs = fetch_json("data/historical-analogs.json") or {}
    events = fetch_json("data/event-study.json") or {}
    abtest = fetch_json("data/ab-test-results.json") or {}
    portfolio = fetch_json("portfolio/signal-portfolio-state.json") or {}
    corr = fetch_json("data/correlation-surface.json") or {}
    changed = fetch_json("data/whats-changed.json") or {}
    # Phase C: divergence v2.5 + regime-conditional interpretation
    divergence_v2 = fetch_json("data/divergence-v2.json") or {}
    divergence_interp = fetch_json("data/divergence-interpreted.json") or {}
    # Phase D: sector tilt + pairs scanner (sprints 5+6)
    sector_tilt = fetch_json("data/sector-tilt.json") or {}
    pairs_scan = fetch_json("data/pairs-scanner.json") or {}

    lines = []
    lines.append(f"*🌅 JustHodl Morning Brief — {now.strftime('%Y-%m-%d')}*")
    lines.append("")

    # Headline regime
    khalid = (morning.get("khalid_index") or {}).get("score") if isinstance(morning.get("khalid_index"), dict) else morning.get("khalid_index")
    regime = morning.get("regime") or "—"
    lines.append(f"*Regime:* `{md_escape(regime)}`  *Khalid Index:* `{fmt_num(khalid, 1)}`")
    lines.append("")

    # Macro layer
    if macro:
        lines.append("*📊 Macro Surprise*")
        lines.append(
            f"  composite z=`{fmt_num(macro.get('composite_z'), 2, sign=True)}` "
            f"regime=`{md_escape(macro.get('regime'))}`"
        )
        lines.append(
            f"  growth\\_z=`{fmt_num(macro.get('growth_z'), 2, sign=True)}` "
            f"infl\\_z=`{fmt_num(macro.get('inflation_z'), 2, sign=True)}`"
        )

    # Yield curve
    if yc:
        spreads = yc.get("spreads", {}) or {}
        lines.append("*📐 Yield Curve*")
        lines.append(
            f"  regime=`{md_escape(yc.get('regime'))}` 2s10s=`{fmt_num(spreads.get('2s10s'), 0, sign=True)}bps` "
            f"10Y=`{fmt_num(yc.get('tenors_nominal', {}).get('10Y'))}%`"
        )

    # Historical analogs
    if analogs:
        fr = analogs.get("forward_returns", {}) or {}
        d21 = fr.get("21d", {}) or {}
        lines.append("*🕰️ Historical Analogs*")
        lines.append(
            f"  call=`{md_escape(analogs.get('call'))}` "
            f"21d hit=`{fmt_pct(d21.get('hit_rate_pct'), 1)}` "
            f"mean=`{fmt_pct(d21.get('mean_pct'), 2, sign=True)}`"
        )

    # Event study active themes
    active_themes = events.get("active_themes") or []
    if active_themes:
        lines.append("*🎯 Active Event Themes*")
        for t in active_themes[:4]:
            lines.append(f"  • `{md_escape(t)}`")

    # Cross-asset correlation
    if corr:
        nb = corr.get("n_regime_breaks") or 0
        nd = corr.get("n_decouplings") or 0
        if nb or nd:
            lines.append("*🔗 Cross\\-Asset*")
            lines.append(
                f"  macro\\_regime=`{md_escape(corr.get('macro_regime'))}` "
                f"breaks=`{nb}` decouplings=`{nd}`"
            )

    # ── Phase C: Divergence Engine v2.5 (cross-asset + crisis-leading) ──
    # Only show this section if there are flagged or extreme divergences.
    div_n_extreme = divergence_v2.get("n_extreme") or 0
    div_n_flagged = divergence_v2.get("n_flagged") or 0
    div_composite = divergence_v2.get("composite_divergence_index") or 0
    if div_n_extreme > 0 or div_n_flagged > 0 or div_composite >= 25:
        lines.append("*🚨 Divergence v2\\.5* \\(70 cross\\-asset pairs\\)")
        # Composite index header
        idx_marker = "🔴" if div_composite >= 50 else ("🟠" if div_composite >= 30 else "🟡")
        lines.append(
            f"  {idx_marker} composite=`{fmt_num(div_composite, 1)}/100`  "
            f"extreme=`{div_n_extreme}`  flagged=`{div_n_flagged}`"
        )

        # Top 3 dislocations sorted by abs(z)
        all_rels = divergence_v2.get("all_relationships") or []
        dislocations = [r for r in all_rels
                        if r.get("status") in ("flagged", "extreme")]
        dislocations.sort(key=lambda x: abs(x.get("divergence_z") or 0), reverse=True)
        if dislocations:
            for r in dislocations[:3]:
                z = r.get("divergence_z")
                cat = r.get("category", "?")
                name = (r.get("name") or "?")[:50]
                icon = "⚡" if r.get("status") == "extreme" else "⚠️"
                lines.append(
                    f"    {icon} `{md_escape(name)}` z=`{fmt_num(z, 2, sign=True)}σ` "
                    f"\\[{md_escape(cat)}\\]"
                )

        # Claude regime-aware interpretation snippet (high-conviction line only)
        if divergence_interp.get("interpretation"):
            interp = divergence_interp["interpretation"]
            # Extract the high-conviction call line — typically right after "HIGH-CONVICTION CALL"
            # Use a robust pattern with fallbacks
            call_line = ""
            for marker in ["HIGH-CONVICTION CALL", "**HIGH-CONVICTION CALL", "HIGH CONVICTION CALL"]:
                idx = interp.find(marker)
                if idx >= 0:
                    # Take up to 220 chars after the marker, stop at first ##/newline-blank
                    snippet = interp[idx + len(marker):idx + len(marker) + 350]
                    # Find first non-whitespace line of substance
                    for raw_line in snippet.split("\n"):
                        cleaned = raw_line.strip().lstrip("*").lstrip(":").strip()
                        if cleaned and len(cleaned) > 10 and not cleaned.startswith("#"):
                            call_line = cleaned[:200]
                            break
                    if call_line:
                        break

            confidence = ""
            for marker in ["CONFIDENCE:", "Confidence:", "**CONFIDENCE"]:
                idx = interp.find(marker)
                if idx >= 0:
                    snippet = interp[idx:idx + 50]
                    import re
                    m = re.search(r"(\d+(?:\.\d+)?)/10", snippet)
                    if m:
                        confidence = m.group(1)
                        break

            if call_line:
                lines.append(f"  💡 Claude: {md_escape(call_line)}")
            if confidence:
                lines.append(f"  Confidence: `{md_escape(confidence)}/10`")

        # Alert reasons (if any triggered last interpretation)
        alert_reasons = divergence_interp.get("alert_reasons") or []
        if alert_reasons:
            lines.append(f"  📍 Alerts: `{len(alert_reasons)}` triggered")

    # ── Phase D / Sprint 5: Sector Tilt (regime → 11 SPDR overweights) ──
    # Only show if there are STRONG OVERWEIGHT or STRONG UNDERWEIGHT calls,
    # OR any MISALIGNED tilts (the high-conviction mean-reversion entries).
    if sector_tilt.get("tilts"):
        st_regime = sector_tilt.get("regime", "—")
        tilts = sector_tilt.get("tilts") or []
        # MISALIGNED tilts where macro regime calls for OW but sector is LAGGING
        # = highest-conviction mean-reversion BUY_OPPORTUNITY
        misaligned_buy = [
            t for t in tilts
            if t.get("alignment") == "MISALIGNED"
            and t.get("implication") == "BUY_OPPORTUNITY"
            and t.get("urgency") == "HIGH"
        ]
        # Strong overweights/underweights regardless of alignment
        strong_ow = [t for t in tilts if (t.get("regime_tilt_score") or 0) >= 2]
        strong_uw = [t for t in tilts if (t.get("regime_tilt_score") or 0) <= -2]

        if misaligned_buy or strong_ow or strong_uw:
            lines.append(f"*🎯 Sector Tilt* \\(regime: `{md_escape(st_regime)}`\\)")
            if misaligned_buy:
                lines.append(f"  📈 *MISALIGNED BUY OPPORTUNITY* \\({len(misaligned_buy)}\\) \\— OW regime call but currently lagging:")
                for t in misaligned_buy[:3]:
                    rs = t.get("rs_20d")
                    rs_str = f"{rs:+.1f}%" if isinstance(rs, (int, float)) else "?"
                    lines.append(
                        f"    • `{md_escape(t.get('ticker',''))}` {md_escape(t.get('name',''))} "
                        f"\\(20d RS=`{md_escape(rs_str)}`\\)"
                    )
            if strong_ow:
                ow_tickers = ", ".join(f"`{md_escape(t.get('ticker',''))}`" for t in strong_ow[:5])
                lines.append(f"  ⬆ Strong OW: {ow_tickers}")
            if strong_uw:
                uw_tickers = ", ".join(f"`{md_escape(t.get('ticker',''))}`" for t in strong_uw[:5])
                lines.append(f"  ⬇ Strong UW: {uw_tickers}")

    # ── Phase D / Sprint 6: Pairs Trading Scanner (only EXTREME / EXTENDED) ──
    if pairs_scan.get("pairs"):
        pairs_list = pairs_scan.get("pairs") or []
        extreme_pairs = [p for p in pairs_list if p.get("state") == "EXTREME"]
        extended_pairs = [p for p in pairs_list if p.get("state") == "EXTENDED"]
        # Only emit section if there's something tradeable to flag
        if extreme_pairs or extended_pairs:
            n_ext = len(extreme_pairs) + len(extended_pairs)
            lines.append(
                f"*📐 Pairs Trading* \\(`{n_ext}` actionable / `{len(pairs_list)}` total\\)"
            )
            # Show top 3 EXTREME first, then top 2 EXTENDED if room
            top_pairs = sorted(
                extreme_pairs + extended_pairs,
                key=lambda p: abs(p.get("spread_z") or 0),
                reverse=True
            )[:3]
            for p in top_pairs:
                z = p.get("spread_z")
                z_str = f"{z:+.2f}σ" if isinstance(z, (int, float)) else "?"
                rr = p.get("rr_estimate")
                rr_str = f"{rr:.1f}:1" if isinstance(rr, (int, float)) else "?"
                hl = p.get("half_life_days")
                hl_str = f"{int(hl)}d" if isinstance(hl, (int, float)) else "?"
                state_emoji = "⚡" if p.get("state") == "EXTREME" else "⚠️"
                lines.append(
                    f"  {state_emoji} `{md_escape(p.get('name',''))}` "
                    f"z=`{md_escape(z_str)}` R:R=`{md_escape(rr_str)}` half\\-life=`{md_escape(hl_str)}`"
                )
                # Trade direction on next line for readability
                trade = p.get("trade") or "—"
                lines.append(f"      📍 _Trade:_ {md_escape(trade)}")

    # Paper portfolio
    if portfolio:
        nav = portfolio.get("nav")
        equity = portfolio.get("total_equity")
        n_open = len(portfolio.get("open_positions") or [])
        stats = portfolio.get("performance") or {}
        win_rate = stats.get("win_rate_pct")
        pf = stats.get("profit_factor")
        lines.append("*💼 Paper Portfolio*")
        lines.append(
            f"  NAV=`${fmt_num(nav, 0)}` equity=`${fmt_num(equity, 0)}` "
            f"open=`{n_open}` win\\_rate=`{fmt_pct(win_rate, 1)}` PF=`{fmt_num(pf, 2)}`"
        )

    # A/B test
    if abtest and abtest.get("leaderboard"):
        lines.append("*🧪 A/B Test*")
        if abtest.get("winner"):
            lines.append(f"  winner=`{md_escape(abtest.get('winner'))}`")
        for row in abtest["leaderboard"][:3]:
            acc = row.get("accuracy_pct")
            if acc is not None and row.get("n_scored"):
                lines.append(
                    f"  • `{md_escape(row['variant']):20s}` "
                    f"acc=`{fmt_pct(acc, 1)}` n=`{row['n_scored']}`"
                )

    # What changed
    if changed and changed.get("changes"):
        lines.append("*🔄 What Changed Today*")
        for c in (changed.get("changes") or [])[:5]:
            lines.append(f"  • {md_escape(c.get('summary') or c)}")

    lines.append("")
    lines.append("_Auto\\-generated · justhodl\\.ai_")

    return "\n".join(lines)


def send_telegram(chat_id, text):
    if not TELEGRAM_TOKEN:
        return False, "missing_token"
    if not chat_id:
        return False, "missing_chat_id"
    api = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text[:4096],
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        api, data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return True, resp.read().decode()
    except urllib.error.HTTPError as e:
        # Markdown parsing can fail — retry as plain text
        body = e.read().decode() if hasattr(e, "read") else ""
        print(f"MarkdownV2 send failed ({e.code}): {body[:300]}")
        try:
            payload2 = {"chat_id": chat_id, "text": text[:4096], "disable_web_page_preview": True}
            req2 = urllib.request.Request(
                api, data=json.dumps(payload2).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req2, timeout=15) as resp:
                return True, resp.read().decode()
        except Exception as e2:
            return False, f"plain_retry_fail: {e2}"
    except Exception as e:
        return False, str(e)


def lambda_handler(event, context):
    chat_id = (event or {}).get("chat_id") or get_chat_id()
    text = build_brief()
    ok, info = send_telegram(chat_id, text)

    # Mirror to S3 for visibility
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key="data/morning-brief-latest.json",
            Body=json.dumps({
                "as_of": datetime.now(timezone.utc).isoformat(),
                "chat_id": chat_id,
                "ok": ok,
                "info": info[:500] if isinstance(info, str) else str(info)[:500],
                "text": text,
            }, indent=2).encode(),
            ContentType="application/json",
        )
    except Exception as e:
        print(f"mirror fail: {e}")

    return {
        "statusCode": 200 if ok else 500,
        "body": json.dumps({"ok": ok, "info": (info[:300] if isinstance(info, str) else str(info)[:300])}),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
