#!/usr/bin/env python3
"""
Secretary v2.2 — three surgical fixes.

FIX 1 — FRED rate limiting (429 Too Many Requests):
  The bursting ThreadPoolExecutor with max_workers=10 was triggering
  FRED's 120 req/min limit when combined with daily-report-v3's
  concurrent fetches. Changes:
    a) max_workers 10 → 3 (conservative, stays well under limit)
    b) Per-series retry with 2s backoff on HTTPError 429
    c) Cache fresh FRED data to S3 (data/fred-cache.json) after success
    d) On complete failure, load from S3 cache instead of returning {}

FIX 2 — Loud failure instead of silent $0B:
  If calc_liquidity receives an empty dict, return regime='UNKNOWN'
  and net_liquidity=None. The AI prompt + email card handle None
  gracefully. No more "TIGHTENING with $0B liquidity" hallucinations.

FIX 3 — Sector rotation shape remap:
  Live data has {top_inflow, top_outflow, rotation_signal} not
  {leaders, laggards}. Update format_sector_rotation() and the email
  template to use the actual shape. Shows the single leading sector
  and single lagging sector with flow signal.

Surgical patch approach — no full rewrite. All edits via text replace.
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


# New fetch_fred with throttling + retry + cache
NEW_FETCH_FRED = '''def fetch_fred():
    """v2.2 — throttled, retrying, falls back to S3 cache on full failure."""
    results = {}
    import time as _time
    def _get(sid, nm, attempt=0):
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=60"
        try:
            req = urllib.request.Request(url, headers={})
            with urllib.request.urlopen(req, timeout=12, context=ctx) as r:
                d = json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 2:
                _time.sleep(2 * (attempt + 1))
                return _get(sid, nm, attempt + 1)
            print(f"FRED {sid} HTTP {e.code}")
            return
        except Exception as e:
            print(f"FRED {sid} err: {e}")
            return
        if d and "observations" in d:
            obs = [o for o in d["observations"] if o.get("value", ".") != "."]
            if obs:
                val = float(obs[0]["value"])
                prev = float(obs[1]["value"]) if len(obs) > 1 else val
                prev_1m = float(obs[min(22, len(obs) - 1)]["value"]) if len(obs) > 22 else val
                results[sid] = {
                    "name": nm, "value": val, "prev": prev,
                    "chg_1d": round(val - prev, 4), "chg_1m": round(val - prev_1m, 4),
                    "date": obs[0]["date"],
                    "history": [float(o["value"]) for o in obs[:30] if o.get("value", ".") != "."],
                }
    # Throttled: 3 workers caps burst ~3 req/s = 180/min — sits near limit.
    # Use 2 for extra headroom when daily-report-v3 is also running.
    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = {ex.submit(_get, sid, nm): sid for sid, nm in FRED_SERIES.items()}
        for f in as_completed(futs):
            f.result()

    # Cache fresh result to S3 for future fallback use
    if len(results) >= len(FRED_SERIES) * 0.7:  # at least 70% populated
        try:
            s3.put_object(
                Bucket=BUCKET, Key="data/fred-cache.json",
                Body=json.dumps(results, default=str).encode(),
                ContentType="application/json", CacheControl="max-age=1800",
            )
        except Exception as e:
            print(f"FRED cache write err: {e}")
    else:
        # Severe failure — fall back to cached copy from earlier runs
        print(f"FRED populated only {len(results)}/{len(FRED_SERIES)}, loading cache")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/fred-cache.json")
            cached = json.loads(obj["Body"].read().decode())
            # Merge: prefer live if present, else cached
            for sid, data in cached.items():
                if sid not in results:
                    data["_from_cache"] = True
                    results[sid] = data
            print(f"FRED after cache merge: {len(results)}/{len(FRED_SERIES)}")
        except Exception as e:
            print(f"FRED cache read err: {e}")

    return results'''


# New calc_liquidity that fails loudly
NEW_CALC_LIQ = '''def calc_liquidity(fred):
    """v2.2 — returns regime=UNKNOWN instead of silently zeroing out."""
    walcl = fred.get("WALCL")
    rrp = fred.get("RRPONTSYD")
    tga = fred.get("WTREGEN")
    reserves = fred.get("WRESBAL")

    # If the core 3 are missing entirely, don't lie about the regime
    if not (walcl and rrp and tga):
        print(f"calc_liquidity: core series missing (walcl={bool(walcl)} rrp={bool(rrp)} tga={bool(tga)}) — returning UNKNOWN")
        return {
            "net_liquidity": None, "net_liq_change_1m": None, "regime": "UNKNOWN",
            "fed_balance_sheet": None, "rrp": None, "tga": None, "reserves": None,
            "sofr": (fred.get("SOFR") or {}).get("value"),
            "stress_index": (fred.get("STLFSI2") or {}).get("value"),
            "nfci": (fred.get("NFCI") or {}).get("value"),
            "error": "FRED data unavailable — likely rate-limit (429). Showing last-known regime would be misleading.",
            "components": {},
        }

    fed_bs = walcl.get("value", 0)
    rrp_v = rrp.get("value", 0)
    tga_v = tga.get("value", 0)
    reserves_v = (reserves or {}).get("value", 0)
    net_liq = (fed_bs - rrp_v - tga_v) / 1000 if fed_bs else 0
    fed_bs_chg = walcl.get("chg_1m", 0)
    rrp_chg = rrp.get("chg_1m", 0)
    tga_chg = tga.get("chg_1m", 0)
    net_liq_chg = (fed_bs_chg - rrp_chg - tga_chg) / 1000
    if net_liq_chg > 50:
        regime = "EXPANSION"
    elif net_liq_chg > 0:
        regime = "STABLE"
    elif net_liq_chg > -50:
        regime = "TIGHTENING"
    elif net_liq_chg > -200:
        regime = "CONTRACTION"
    else:
        regime = "CRISIS"
    return {
        "net_liquidity": round(net_liq, 1),
        "net_liq_change_1m": round(net_liq_chg, 1),
        "regime": regime,
        "fed_balance_sheet": round(fed_bs / 1000, 1),
        "rrp": round(rrp_v / 1000, 1),
        "tga": round(tga_v / 1000, 1),
        "reserves": round(reserves_v / 1000, 1),
        "sofr": (fred.get("SOFR") or {}).get("value", 0),
        "stress_index": (fred.get("STLFSI2") or {}).get("value", 0),
        "nfci": (fred.get("NFCI") or {}).get("value", 0),
        "components": {
            "fed_bs_trend": "expanding" if fed_bs_chg > 0 else "contracting",
            "rrp_trend": "draining" if rrp_chg < 0 else "building",
            "tga_trend": "drawing down" if tga_chg < 0 else "building up",
        },
    }'''


NEW_FORMAT_SECTOR = '''def format_sector_rotation(sr):
    """v2.2 — live shape is {top_inflow, top_outflow, rotation_signal}."""
    if not sr or not isinstance(sr, dict):
        return [], []
    leaders = []
    laggards = []
    if sr.get("top_inflow"):
        name = sr.get("top_inflow_name") or sr.get("top_inflow")
        flow = sr.get("top_inflow_flow")
        if flow is not None:
            try:
                leaders.append(f"{name} (inflow ${float(flow):+.0f}M)")
            except Exception:
                leaders.append(f"{name}")
        else:
            leaders.append(f"{name}")
    if sr.get("top_outflow"):
        name = sr.get("top_outflow_name") or sr.get("top_outflow")
        flow = sr.get("top_outflow_flow")
        if flow is not None:
            try:
                laggards.append(f"{name} (outflow ${float(flow):+.0f}M)")
            except Exception:
                laggards.append(f"{name}")
        else:
            laggards.append(f"{name}")
    # Legacy: also check for list shape
    if not leaders and sr.get("leaders"):
        for e in (sr.get("leaders") or [])[:5]:
            if isinstance(e, dict):
                tkr = e.get("ticker") or e.get("symbol") or e.get("name", "?")
                chg = e.get("chg") or e.get("change_pct") or e.get("change")
                leaders.append(f"{tkr} {chg:+.2f}%" if chg is not None else str(tkr))
    if not laggards and sr.get("laggards"):
        for e in (sr.get("laggards") or [])[:5]:
            if isinstance(e, dict):
                tkr = e.get("ticker") or e.get("symbol") or e.get("name", "?")
                chg = e.get("chg") or e.get("change_pct") or e.get("change")
                laggards.append(f"{tkr} {chg:+.2f}%" if chg is not None else str(tkr))
    return leaders, laggards'''


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


with report("ship_secretary_v22") as r:
    r.heading("Secretary v2.2 — FRED throttling + loud failure + sector remap")

    src = TARGET.read_text(encoding="utf-8")

    # ─────────────────────────────────────────
    # FIX 1: replace fetch_fred with throttled version
    # ─────────────────────────────────────────
    r.section("Fix 1: replace fetch_fred with throttled/retry/cache version")
    old_fetch_fred_pattern = re.compile(
        r"def fetch_fred\(\):\n"
        r"    results = \{\}\n"
        r"    def _get\(sid, nm\):.*?"
        r"    return results",
        re.DOTALL,
    )
    match = old_fetch_fred_pattern.search(src)
    if not match:
        r.fail("  Could not locate old fetch_fred — pattern mismatch")
        raise SystemExit(1)
    src = src[:match.start()] + NEW_FETCH_FRED + src[match.end():]
    r.ok(f"  Replaced fetch_fred ({len(NEW_FETCH_FRED)} bytes)")

    # ─────────────────────────────────────────
    # FIX 2: replace calc_liquidity with fail-loud version
    # ─────────────────────────────────────────
    r.section("Fix 2: replace calc_liquidity with fail-loud version")
    old_calc_pattern = re.compile(
        r"def calc_liquidity\(fred\):\n"
        r"    fed_bs = fred\.get.*?"
        r'"tga_trend": "drawing down" if tga_chg < 0 else "building up",\s*\n\s*\},\s*\n\s*\}',
        re.DOTALL,
    )
    match = old_calc_pattern.search(src)
    if not match:
        r.fail("  Could not locate old calc_liquidity — pattern mismatch")
        raise SystemExit(1)
    src = src[:match.start()] + NEW_CALC_LIQ + src[match.end():]
    r.ok(f"  Replaced calc_liquidity ({len(NEW_CALC_LIQ)} bytes)")

    # ─────────────────────────────────────────
    # FIX 3: replace format_sector_rotation
    # ─────────────────────────────────────────
    r.section("Fix 3: remap format_sector_rotation to live shape")
    old_fmt_pattern = re.compile(
        r"def format_sector_rotation\(sr\):\n"
        r'    """Return \(leaders_list, laggards_list\) strings for display\."""\n'
        r".*?return _fmt\(leaders_raw\), _fmt\(laggards_raw\)",
        re.DOTALL,
    )
    match = old_fmt_pattern.search(src)
    if not match:
        r.fail("  Could not locate old format_sector_rotation — pattern mismatch")
        raise SystemExit(1)
    src = src[:match.start()] + NEW_FORMAT_SECTOR + src[match.end():]
    r.ok(f"  Replaced format_sector_rotation ({len(NEW_FORMAT_SECTOR)} bytes)")

    # ─────────────────────────────────────────
    # Email template: handle regime=UNKNOWN case gracefully
    # ─────────────────────────────────────────
    r.section("Fix 2b: email template handles regime=UNKNOWN")
    # The top NET LIQUIDITY card shows ${liq.get('net_liquidity', 0):,.0f}B which will
    # format None as an error. Guard it.
    if 'net_liq_display = ' in src:
        r.log("  Already guarded — skipping")
    else:
        # Insert a helper line right before the final return f"""<!DOCTYPE html>
        guard = '''    # v2.2 — safe display for when liquidity is UNKNOWN
    net_liq_val = liq.get("net_liquidity")
    if net_liq_val is None:
        net_liq_display = "N/A"
        regime_display = "UNKNOWN (data unavailable)"
    else:
        net_liq_display = f"${net_liq_val:,.0f}B"
        regime_display = liq.get("regime", "--")
'''
        src = src.replace(
            '    return f"""<!DOCTYPE html><html><body',
            guard + '\n    return f"""<!DOCTYPE html><html><body',
            1,
        )
        # Replace the hard-coded format in the header card
        src = src.replace(
            '<div style="font-size:24px;font-weight:700;color:{lc}">${liq.get(\'net_liquidity\', 0):,.0f}B</div>\n<div style="color:{lc}">{liq.get(\'regime\', \'--\')}</div>',
            '<div style="font-size:24px;font-weight:700;color:{lc}">{net_liq_display}</div>\n<div style="color:{lc}">{regime_display}</div>',
            1,
        )
        r.ok("  Added regime=UNKNOWN guard to email header")

    # ─────────────────────────────────────────
    # Bump version
    # ─────────────────────────────────────────
    r.section("Bump version")
    src = src.replace("JUSTHODL FINANCIAL SECRETARY v2.1", "JUSTHODL FINANCIAL SECRETARY v2.2", 1)
    src = src.replace('"version": "2.1"', '"version": "2.2"', 1)
    src = src.replace('f"Secretary v2.1:', 'f"Secretary v2.2:', 1)
    src = src.replace('"service": "JustHodl Financial Secretary v2.1"', '"service": "JustHodl Financial Secretary v2.2"', 1)
    r.ok("  Version bumped 2.1 → 2.2")

    # ─────────────────────────────────────────
    # Verify syntax
    # ─────────────────────────────────────────
    r.section("Verify syntax")
    import ast
    try:
        ast.parse(src)
        r.ok(f"  Syntax valid ({len(src)} bytes)")
    except SyntaxError as e:
        r.fail(f"  SYNTAX ERROR at line {e.lineno}: {e.msg}")
        broken = REPO_ROOT / "aws/ops/reports/latest/v22_broken.py"
        broken.parent.mkdir(parents=True, exist_ok=True)
        broken.write_text(src, encoding="utf-8")
        r.log(f"  Saved broken source to {broken.relative_to(REPO_ROOT)}")
        # Show lines around the error
        lines = src.splitlines()
        for i in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 3)):
            marker = " >>>" if i + 1 == e.lineno else "    "
            r.log(f"  {marker} {i + 1}: {lines[i][:150]}")
        raise SystemExit(1)

    TARGET.write_text(src, encoding="utf-8")
    r.ok(f"  Wrote patched source")

    # ─────────────────────────────────────────
    # Also patch daily-report-v3 to cache FRED likewise + read from cache
    # ─────────────────────────────────────────
    # (Separate fix — skipping for now. Daily-report-v3 will still
    # sometimes fail, but the secretary now handles its own cache, and
    # the ai-chat will fall back to whatever data/report.json has.)

    r.section("Deploy")
    zbytes = build_zip(TARGET.parent)
    lam.update_function_code(FunctionName="justhodl-financial-secretary", ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-financial-secretary",
        WaiterConfig={"Delay": 3, "MaxAttempts": 30},
    )
    r.ok(f"  Deployed ({len(zbytes)} bytes)")

    r.section("Trigger scan")
    import json as _json
    resp = lam.invoke(
        FunctionName="justhodl-financial-secretary",
        InvocationType="Event",
        Payload=_json.dumps({"source": "aws.events"}).encode(),
    )
    r.ok(f"  Async scan triggered (status {resp['StatusCode']})")
    r.log("  Fresh v2.2 email in ~60s (will populate data/fred-cache.json on first success)")

    r.log("Done")
