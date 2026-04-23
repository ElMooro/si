#!/usr/bin/env python3
"""
Secretary v2.1 — Tier-2 data pulls.

Adds three enrichments to the briefing without touching the v2 pipeline:
  1. Options flow (from s3://justhodl-dashboard-live/flow-data.json):
     put/call ratio + regime, gamma exposure + max gamma strike,
     top trading signals. Feeds the AI prompt.
  2. Crypto intel (from s3://.../crypto-intel.json):
     BTC dominance, total mcap chg, stablecoin flow signal, fear/greed,
     funding rates, on-chain MVRV, whale activity.
  3. Sector rotation heatmap:
     from fund_flows.sector_rotation — leaders/laggards per 1d/5d.

Approach: patch the Secretary v2 source in-place to add fetch_tier2()
that pulls both files, then extend generate_ai_briefing() to include
them in the prompt, and extend build_email_html() with dedicated cards
for each.

We don't rewrite everything — we surgically add 3 helper functions and
splice them into the scan + render paths.
"""

import io
import os
import re
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
TARGET = REPO_ROOT / "aws/lambdas/justhodl-financial-secretary/source/lambda_function.py"

lam = boto3.client("lambda", region_name=REGION)


# ═════════════════ NEW CODE TO INSERT ═════════════════
FETCH_TIER2_FN = '''
# ═══ v2.1 — TIER 2 PULLS ═══
def fetch_tier2():
    """Pull options-flow + crypto-intel from S3. Returns {'options', 'crypto', 'sector_rotation'}."""
    out = {"options": None, "crypto": None, "sector_rotation": None}
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="flow-data.json")
        fd = json.loads(obj["Body"].read().decode())
        data = fd.get("data", {})
        pc = data.get("put_call") or {}
        gex = data.get("gamma_exposure") or {}
        flows = data.get("fund_flows") or {}
        sentiment = data.get("sentiment") or {}
        signals = data.get("trading_signals") or []
        out["options"] = {
            "put_call_ratio": pc.get("total_put_call_ratio"),
            "pc_signal": pc.get("pc_signal", ""),
            "pc_description": pc.get("pc_description", ""),
            "gamma_regime": gex.get("regime", ""),
            "gamma_description": gex.get("description", ""),
            "max_gamma_strike": gex.get("max_gamma_strike"),
            "spy_price": gex.get("spy_price"),
            "total_gex": gex.get("total_gex"),
            "net_premium": pc.get("net_premium"),
            "trading_signals": [
                {
                    "type": s.get("type", ""),
                    "strength": s.get("strength", ""),
                    "message": s.get("message", "")[:140],
                    "confidence": s.get("confidence", 0),
                }
                for s in signals[:5]
            ],
            "sentiment_composite": (sentiment.get("composite") or {}).get("score"),
            "sentiment_label": (sentiment.get("composite") or {}).get("label", ""),
            "timestamp": fd.get("timestamp"),
        }
        out["sector_rotation"] = flows.get("sector_rotation") or {}
    except Exception as e:
        print(f"fetch options flow: {e}")

    try:
        obj = s3.get_object(Bucket=BUCKET, Key="crypto-intel.json")
        ci = json.loads(obj["Body"].read().decode())
        gm = ci.get("global_market") or {}
        sc = ci.get("stablecoins") or {}
        fg = ci.get("fear_greed") or {}
        funding = ci.get("funding") or {}
        ratios = ci.get("onchain_ratios") or {}
        whale = ci.get("whale_txs") or {}
        top = (ci.get("top_coins") or {}).get("coins") or []
        out["crypto"] = {
            "btc_dominance": gm.get("btc_dominance"),
            "eth_dominance": gm.get("eth_dominance"),
            "total_mcap_fmt": gm.get("total_mcap_fmt"),
            "mcap_change_24h": gm.get("mcap_change_24h"),
            "total_volume_fmt": gm.get("total_volume_fmt"),
            "stablecoin_net_signal": sc.get("net_signal"),
            "stablecoin_minting": sc.get("minting_count"),
            "stablecoin_burning": sc.get("burning_count"),
            "fear_greed_value": (fg.get("current") if isinstance(fg, dict) else None),
            "fear_greed_label": (fg.get("label") if isinstance(fg, dict) else None) or (fg.get("classification") if isinstance(fg, dict) else None),
            "funding_summary": (funding.get("summary") if isinstance(funding, dict) else None) or funding,
            "mvrv_approx": (ratios.get("mvrv_approx") if isinstance(ratios, dict) else None),
            "risk_score": ci.get("risk_score"),
            "whale_count_24h": whale.get("whale_count"),
            "top_movers": [
                {"symbol": c.get("symbol"), "price": c.get("price"), "change_24h": c.get("change_24h")}
                for c in top[:10] if c.get("symbol")
            ],
            "timestamp": ci.get("generated_at"),
        }
    except Exception as e:
        print(f"fetch crypto intel: {e}")

    return out


def format_sector_rotation(sr):
    """Return (leaders_list, laggards_list) strings for display."""
    if not sr or not isinstance(sr, dict):
        return [], []
    # Common shape: {'leaders': [{'ticker','name','chg'}], 'laggards': [...]}
    leaders_raw = sr.get("leaders") or sr.get("top") or []
    laggards_raw = sr.get("laggards") or sr.get("bottom") or []
    def _fmt(entries):
        out = []
        for e in entries[:5]:
            if isinstance(e, dict):
                tkr = e.get("ticker") or e.get("symbol") or e.get("name", "?")
                chg = e.get("chg") or e.get("change_pct") or e.get("change")
                if chg is not None:
                    out.append(f"{tkr} {chg:+.2f}%")
                else:
                    out.append(str(tkr))
        return out
    return _fmt(leaders_raw), _fmt(laggards_raw)
'''


PROMPT_TIER2_BLOCK = '''    # v2.1 — tier 2 enrichment
    tier2_str = ""
    if tier2 and isinstance(tier2, dict):
        opts = tier2.get("options") or {}
        crypto_i = tier2.get("crypto") or {}
        pc = opts.get("put_call_ratio")
        pc_sig = opts.get("pc_signal", "")
        gex_regime = opts.get("gamma_regime", "")
        max_gex = opts.get("max_gamma_strike")
        spy_p = opts.get("spy_price")
        net_prem = opts.get("net_premium")
        tier2_str += f"\\nOPTIONS FLOW: put/call={pc} ({pc_sig}) | gamma={gex_regime} | max-gamma strike=${max_gex} (SPY @ ${spy_p}) | net premium=${net_prem:,.0f}" if pc is not None else ""
        trsigs = opts.get("trading_signals") or []
        if trsigs:
            tier2_str += "\\n  signals: " + " | ".join(f"{s['type']}({s['strength']})" for s in trsigs[:3])
        btc_dom = crypto_i.get("btc_dominance")
        mcap_chg = crypto_i.get("mcap_change_24h")
        fg_v = crypto_i.get("fear_greed_value")
        fg_l = crypto_i.get("fear_greed_label")
        sc_sig = crypto_i.get("stablecoin_net_signal")
        mvrv = crypto_i.get("mvrv_approx")
        crypto_risk = crypto_i.get("risk_score")
        if btc_dom is not None:
            tier2_str += f"\\nCRYPTO: BTC_dom={btc_dom}% total_mcap_chg_24h={mcap_chg}% stablecoins={sc_sig} fear_greed={fg_v}({fg_l}) MVRV={mvrv} crypto_risk={crypto_risk}"
        # Sector rotation
        sr = tier2.get("sector_rotation") or {}
        ldrs, lags = format_sector_rotation(sr)
        if ldrs:
            tier2_str += f"\\nSECTOR LEADERS: {', '.join(ldrs)}"
        if lags:
            tier2_str += f"\\nSECTOR LAGGARDS: {', '.join(lags)}"

'''


EMAIL_TIER2_BLOCK = '''
    # v2.1 — tier 2 cards
    tier2 = scan.get("tier2") or {}
    opts = tier2.get("options") or {}
    crypto_i = tier2.get("crypto") or {}
    sr = tier2.get("sector_rotation") or {}

    tier2_html = ""
    if opts.get("put_call_ratio") is not None:
        pc = opts.get("put_call_ratio")
        pc_sig = opts.get("pc_signal", "") or "—"
        gex_reg = opts.get("gamma_regime", "") or "—"
        max_gex = opts.get("max_gamma_strike")
        spy_p = opts.get("spy_price")
        # Color put/call: < 0.7 = complacency (red for warning), > 1.2 = fear (green for hedged)
        pc_color = "#ff8800" if (pc is not None and pc < 0.5) else "#44cc44" if (pc is not None and pc > 1.0) else "#e0e0e0"
        gex_color = "#44cc44" if "POSITIVE" in str(gex_reg).upper() else "#ff8800" if "NEGATIVE" in str(gex_reg).upper() else "#e0e0e0"
        trsigs = opts.get("trading_signals") or []
        sig_lines = "".join([f"<li style='font-size:12px'><b>{s['type']}</b> ({s['strength']}): {s['message'][:100]}</li>" for s in trsigs[:4]]) or "<li style='font-size:12px;color:#888'>No active signals</li>"
        tier2_html += f"""
<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin:20px 0">
  <h2 style="color:#00ddff">OPTIONS FLOW</h2>
  <table style="width:100%;font-size:13px"><tr>
    <td style="padding:6px"><div style="color:#888;font-size:11px">PUT/CALL RATIO</div><div style="font-size:22px;font-weight:700;color:{pc_color}">{pc:.2f}</div><div style="font-size:11px;color:#888">{pc_sig}</div></td>
    <td style="padding:6px"><div style="color:#888;font-size:11px">GAMMA REGIME</div><div style="font-size:18px;font-weight:700;color:{gex_color}">{gex_reg}</div><div style="font-size:11px;color:#888">Max gamma @ ${max_gex} · SPY ${spy_p}</div></td>
  </tr></table>
  <div style="margin-top:12px;font-size:12px;color:#aaa">Top signals:</div>
  <ul style="line-height:1.5">{sig_lines}</ul>
</div>
"""

    if crypto_i.get("btc_dominance") is not None:
        btc_dom = crypto_i.get("btc_dominance")
        mcap_chg = crypto_i.get("mcap_change_24h")
        fg_v = crypto_i.get("fear_greed_value") or "—"
        fg_l = crypto_i.get("fear_greed_label") or ""
        sc_sig = crypto_i.get("stablecoin_net_signal") or "—"
        sc_color = "#44cc44" if "INFLOW" in str(sc_sig).upper() else "#ff8800" if "OUTFLOW" in str(sc_sig).upper() else "#e0e0e0"
        mcap_color = "#44cc44" if (mcap_chg or 0) > 0 else "#ff4444"
        crypto_risk = crypto_i.get("risk_score") or "—"
        movers = crypto_i.get("top_movers") or []
        mover_rows = "".join([f"<tr><td style='padding:4px;color:#00ddff'>{m['symbol']}</td><td style='padding:4px;font-family:monospace'>${m['price']:,.4f}</td><td style='padding:4px;color:{'#00ff88' if (m.get('change_24h') or 0) > 0 else '#ff4444'}'>{(m.get('change_24h') or 0):+.2f}%</td></tr>" for m in movers[:8]])
        tier2_html += f"""
<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin:20px 0">
  <h2 style="color:#00ddff">CRYPTO INTEL</h2>
  <table style="width:100%;font-size:13px"><tr>
    <td style="padding:6px"><div style="color:#888;font-size:11px">BTC DOMINANCE</div><div style="font-size:22px;font-weight:700">{btc_dom:.1f}%</div></td>
    <td style="padding:6px"><div style="color:#888;font-size:11px">TOTAL MCAP 24h</div><div style="font-size:22px;font-weight:700;color:{mcap_color}">{(mcap_chg or 0):+.2f}%</div></td>
    <td style="padding:6px"><div style="color:#888;font-size:11px">FEAR/GREED</div><div style="font-size:22px;font-weight:700">{fg_v}</div><div style="font-size:11px;color:#888">{fg_l}</div></td>
    <td style="padding:6px"><div style="color:#888;font-size:11px">STABLECOIN FLOW</div><div style="font-size:18px;font-weight:700;color:{sc_color}">{sc_sig}</div></td>
    <td style="padding:6px"><div style="color:#888;font-size:11px">RISK SCORE</div><div style="font-size:22px;font-weight:700">{crypto_risk}</div></td>
  </tr></table>
  <div style="margin-top:12px;font-size:12px;color:#aaa">Top coins:</div>
  <table style="width:100%;font-size:12px;border-collapse:collapse">{mover_rows}</table>
</div>
"""

    ldrs, lags = [], []
    if sr:
        try:
            ldrs = [f"{e.get('ticker') or e.get('symbol')} {(e.get('chg') or e.get('change_pct') or 0):+.2f}%" for e in (sr.get("leaders") or sr.get("top") or [])[:5] if isinstance(e, dict)]
            lags = [f"{e.get('ticker') or e.get('symbol')} {(e.get('chg') or e.get('change_pct') or 0):+.2f}%" for e in (sr.get("laggards") or sr.get("bottom") or [])[:5] if isinstance(e, dict)]
        except Exception:
            ldrs, lags = [], []

    if ldrs or lags:
        ldrs_html = "".join([f"<li style='color:#00ff88'>{l}</li>" for l in ldrs]) or "<li style='color:#888'>—</li>"
        lags_html = "".join([f"<li style='color:#ff4444'>{l}</li>" for l in lags]) or "<li style='color:#888'>—</li>"
        tier2_html += f"""
<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin:20px 0">
  <h2 style="color:#00ddff">SECTOR ROTATION</h2>
  <table style="width:100%"><tr>
    <td style="padding:6px;vertical-align:top;width:50%"><div style="color:#00ff88;font-size:12px;margin-bottom:6px">LEADERS</div><ul style="line-height:1.6;font-size:13px">{ldrs_html}</ul></td>
    <td style="padding:6px;vertical-align:top;width:50%"><div style="color:#ff4444;font-size:12px;margin-bottom:6px">LAGGARDS</div><ul style="line-height:1.6;font-size:13px">{lags_html}</ul></td>
  </tr></table>
</div>
"""
'''


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


with report("ship_secretary_v21") as r:
    r.heading("Secretary v2.1 — add options flow + crypto intel + sector rotation")

    src = TARGET.read_text(encoding="utf-8")

    r.section("Step 1: add fetch_tier2 + format_sector_rotation helpers")
    if "def fetch_tier2(" in src:
        r.log("  Already present — skipping insertion")
    else:
        # Insert right before `# ═══ LIQUIDITY ═══`
        marker = "# ═══ LIQUIDITY ═══"
        if marker not in src:
            r.fail(f"  Can't find marker '{marker}' in source")
            raise SystemExit(1)
        src = src.replace(marker, FETCH_TIER2_FN.strip() + "\n\n\n" + marker, 1)
        r.ok("  Inserted fetch_tier2() + format_sector_rotation()")

    r.section("Step 2: extend run_full_scan to call fetch_tier2")
    # Insert tier2 into the ThreadPoolExecutor submissions
    if "f_tier2 = ex.submit(fetch_tier2)" in src:
        r.log("  Already wired in scan")
    else:
        # Inject after the yesterday fetcher submit line
        old = "        f_yesterday = ex.submit(fetch_yesterday_snapshot)"
        new = (
            "        f_yesterday = ex.submit(fetch_yesterday_snapshot)\n"
            "        f_tier2 = ex.submit(fetch_tier2)"
        )
        if old in src:
            src = src.replace(old, new, 1)
            # Also collect the result
            old2 = "        yesterday = f_yesterday.result()"
            new2 = "        yesterday = f_yesterday.result()\n        tier2 = f_tier2.result()"
            src = src.replace(old2, new2, 1)
            r.ok("  Wired fetch_tier2 into parallel scan")
        else:
            r.fail("  Couldn't find yesterday submit marker")
            raise SystemExit(1)

    r.section("Step 3: pass tier2 into generate_ai_briefing and include in scan payload")
    # Change the generate_ai_briefing signature + call-site
    if "def generate_ai_briefing(liq, risk, recs, fred, crypto, news, cftc, deltas, tier2=None):" in src:
        r.log("  Signature already updated")
    else:
        src = src.replace(
            "def generate_ai_briefing(liq, risk, recs, fred, crypto, news, cftc, deltas):",
            "def generate_ai_briefing(liq, risk, recs, fred, crypto, news, cftc, deltas, tier2=None):",
            1,
        )
        # Update the call site in run_full_scan
        src = src.replace(
            "ai = generate_ai_briefing(liq, risk, recs, fred, crypto, news, cftc, deltas)",
            "ai = generate_ai_briefing(liq, risk, recs, fred, crypto, news, cftc, deltas, tier2)",
            1,
        )
        r.ok("  Extended signature + call site")

    # Inject tier2 prompt block inside generate_ai_briefing
    if "tier2_str" not in src:
        # Insert before the `prompt = f\"\"\"` block
        src = src.replace(
            "    prompt = f\"\"\"You are Khalid's personal financial secretary.",
            PROMPT_TIER2_BLOCK + "    prompt = f\"\"\"You are Khalid's personal financial secretary.",
            1,
        )
        # Now reference tier2_str inside the prompt template. Insert it after the CFTC line.
        src = src.replace(
            "CFTC POSITIONING: {cftc_str}\n",
            "CFTC POSITIONING: {cftc_str}{tier2_str}\n",
            1,
        )
        r.ok("  Added tier2 block to AI prompt")
    else:
        r.log("  tier2_str already in prompt")

    # Add tier2 to the scan payload
    if '"tier2": tier2,' in src:
        r.log("  tier2 already in scan payload")
    else:
        src = src.replace(
            '"cftc": cftc,',
            '"cftc": cftc,\n        "tier2": tier2,',
            1,
        )
        r.ok("  Added tier2 to scan payload")

    r.section("Step 4: add tier2 HTML cards to email")
    # Insert the tier2_html block generation + include it before AI ANALYSIS card
    if "tier2_html = \"\"" in src:
        r.log("  Email tier2 block already present")
    else:
        # Insert block before the `return f\"\"\"<!DOCTYPE html>` line
        src = src.replace(
            '    return f"""<!DOCTYPE html><html><body',
            EMAIL_TIER2_BLOCK + '    return f"""<!DOCTYPE html><html><body',
            1,
        )
        # Splice {tier2_html} into the HTML template — place before AI ANALYSIS card
        src = src.replace(
            '{deltas_html}\n<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin:20px 0">\n<h2 style="color:#00ddff">AI ANALYSIS</h2>',
            '{deltas_html}\n{tier2_html}\n<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin:20px 0">\n<h2 style="color:#00ddff">AI ANALYSIS</h2>',
            1,
        )
        r.ok("  Inserted tier2 HTML cards into email template")

    r.section("Step 5: bump version + verify syntax")
    src = src.replace(
        'JUSTHODL FINANCIAL SECRETARY v2.0',
        'JUSTHODL FINANCIAL SECRETARY v2.1',
        1,
    )
    src = src.replace(
        '"version": "2.0"',
        '"version": "2.1"',
        1,
    )
    src = src.replace(
        'f"Secretary v2:',
        'f"Secretary v2.1:',
        1,
    )
    src = src.replace(
        '"service": "JustHodl Financial Secretary v2.0"',
        '"service": "JustHodl Financial Secretary v2.1"',
        1,
    )

    import ast
    try:
        ast.parse(src)
        r.ok(f"  Syntax valid ({len(src)} bytes)")
    except SyntaxError as e:
        r.fail(f"  SYNTAX ERROR at line {e.lineno}: {e.msg}")
        # Save the broken source for inspection
        broken = REPO_ROOT / "aws/ops/reports/latest/v21_broken.py"
        broken.parent.mkdir(parents=True, exist_ok=True)
        broken.write_text(src, encoding="utf-8")
        r.log(f"  Broken source saved to {broken.relative_to(REPO_ROOT)}")
        raise SystemExit(1)

    TARGET.write_text(src, encoding="utf-8")
    r.ok(f"  Wrote patched source to {TARGET.relative_to(REPO_ROOT)}")

    r.section("Step 6: deploy")
    zbytes = build_zip(TARGET.parent)
    lam.update_function_code(FunctionName="justhodl-financial-secretary", ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-financial-secretary",
        WaiterConfig={"Delay": 3, "MaxAttempts": 30},
    )
    r.ok(f"  Deployed ({len(zbytes)} bytes)")

    r.section("Step 7: trigger async scan — fresh v2.1 email in ~60s")
    import json as _json
    resp = lam.invoke(
        FunctionName="justhodl-financial-secretary",
        InvocationType="Event",
        Payload=_json.dumps({"source": "aws.events"}).encode(),
    )
    r.ok(f"  Scan triggered async (status {resp['StatusCode']})")
    r.log("  Email with 3 new cards (Options Flow, Crypto Intel, Sector Rotation) queued")

    r.log("Done")
